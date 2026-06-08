//! Demonstrates the Parquet [`PageStore`] API with a **spilling** backend that
//! keeps completed Parquet pages in temp files instead of buffering them on the
//! heap.
//!
//! See [README.md] for more details

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringViewBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use parquet::arrow::arrow_writer::{
    ArrowWriterOptions, PageKey, PageStore, PageStoreArgs, PageStoreFactory,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::WriterProperties;

/// Write a wide, skewed Parquet file and report the writer's peak heap memory,
/// with or without a spilling `PageStore`.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Number of large (~16 KiB) string columns.
    #[arg(long, default_value_t = 10)]
    large_string_columns: usize,

    /// Number of small (~20 byte) string columns.
    #[arg(long, default_value_t = 5)]
    small_string_columns: usize,

    /// Number of `Int64` columns.
    #[arg(long, default_value_t = 3)]
    int_columns: usize,

    /// Total number of rows, all written into a single row group.
    #[arg(long, default_value_t = 8192)]
    rows: usize,

    /// Spill completed pages to temp files instead of buffering them.
    #[arg(long, conflicts_with = "mem_budget_mb")]
    spill: bool,

    /// Keep completed pages in memory up to this many MiB *across all columns*,
    /// spilling only the overflow to temp files.
    ///
    /// A write that fits the budget pays no I/O at all (like the in-memory
    /// default); a larger one keeps peak heap near the budget regardless of how
    /// many columns there are.
    #[arg(long)]
    mem_budget_mb: Option<usize>,
}

/// Length, in bytes, of values in a "large" string column (16 KiB per row).
const LARGE_AVG_LEN: usize = 16 * 1024;
/// Length, in bytes, of values in a "small" string column.
const SMALL_AVG_LEN: usize = 20;
/// Rows per input batch fed to the writer
const BATCH_SIZE: usize = 4096;

/// Running totals of what was spilled, shared across the per-column stores.
#[derive(Debug, Default)]
struct SpillStats {
    pages: AtomicUsize,
    bytes: AtomicU64,
}

/// One temp file shared by every column's store.
///
/// A wide schema that spills would otherwise open (and hold a file descriptor
/// for) one temp file per column; sharing a single file keeps that to one. Pages
/// are appended and addressed by their `(offset, len)` in the file, recorded by
/// each store.
///
/// The file is guarded by a [`Mutex`] so a `seek`+`write`/`read` pair is atomic
/// even when the writer encodes columns on parallel threads. That serializes the
/// spill I/O, but a buffered write to a single file already serializes inside the
/// kernel (one inode lock), so the lock costs little here while keeping the code
/// portable — no platform-specific positioned I/O. The file is unlinked on
/// creation (via [`tempfile::tempfile`]) so the OS reclaims it when the last
/// store is dropped.
#[derive(Debug)]
struct SharedSpill {
    state: Mutex<SpillState>,
    stats: Arc<SpillStats>,
}

#[derive(Debug, Default)]
struct SpillState {
    /// Created on the first spill — a write that fits its budget never opens it.
    file: Option<File>,
    /// Next free byte, where the next page is appended.
    end: u64,
}

impl SharedSpill {
    fn new(stats: Arc<SpillStats>) -> Self {
        Self {
            state: Mutex::new(SpillState::default()),
            stats,
        }
    }

    /// Append `bytes` and return the offset they were written at.
    fn append(&self, bytes: &[u8]) -> Result<u64> {
        let mut st = self.state.lock().expect("spill lock poisoned");
        if st.file.is_none() {
            st.file = Some(tempfile::tempfile()?);
        }
        let offset = st.end;
        {
            let file = st.file.as_mut().expect("just created");
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(bytes)?;
        }
        st.end += bytes.len() as u64;
        self.stats.pages.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(offset)
    }

    /// Read back `len` bytes at `offset` (written by a prior [`append`](Self::append)).
    fn read(&self, offset: u64, len: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; len];
        let mut st = self.state.lock().expect("spill lock poisoned");
        let file = st
            .file
            .as_mut()
            .expect("a spilled page implies the file exists");
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

/// A single memory budget shared by every column's [`SpillingPageStore`].
///
/// `used` is the total page bytes currently resident on the heap across all
/// columns. It's an atomic because the writer can encode columns in parallel,
/// so independent stores may `reserve`/`release` against this budget at once.
#[derive(Debug)]
struct Budget {
    cap: usize,
    used: AtomicUsize,
}

impl Budget {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            used: AtomicUsize::new(0),
        }
    }

    /// Reserve `n` bytes of the budget, returning `true` if they fit. A page too
    /// large for the whole budget never fits and is always spilled.
    fn reserve(&self, n: usize) -> bool {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            if used + n > self.cap {
                return false;
            }
            match self.used.compare_exchange_weak(
                used,
                used + n,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => used = actual,
            }
        }
    }

    fn release(&self, n: usize) {
        self.used.fetch_sub(n, Ordering::Relaxed);
    }
}

/// A completed page, held either on the heap (counted against the [`Budget`]) or
/// spilled to the shared file at `(offset, len)`.
enum Page {
    Mem(Bytes),
    Disk { offset: u64, len: usize },
    /// Already handed out by `take`; kept so `PageKey` indices stay stable.
    Taken,
}

/// A [`PageStore`] that keeps pages on the heap while the shared [`Budget`] has
/// room and spills the overflow to the shared file. With a zero budget it spills
/// everything (the `--spill` mode); with a non-zero budget a write that fits pays
/// no I/O, while a larger one keeps peak heap near the budget no matter how many
/// columns share it.
struct SpillingPageStore {
    budget: Arc<Budget>,
    spill: Arc<SharedSpill>,
    pages: Vec<Page>,
    /// Bytes this store currently holds in memory — reported by `memory_size`
    /// and released back to the budget when taken or dropped.
    resident: usize,
}

impl SpillingPageStore {
    fn new(budget: Arc<Budget>, spill: Arc<SharedSpill>) -> Self {
        Self {
            budget,
            spill,
            pages: Vec::new(),
            resident: 0,
        }
    }
}

impl PageStore for SpillingPageStore {
    fn put(&mut self, value: Bytes) -> Result<PageKey> {
        let key = PageKey::new(self.pages.len() as u64);
        let page = if self.budget.reserve(value.len()) {
            self.resident += value.len();
            Page::Mem(value)
        } else {
            let offset = self.spill.append(&value)?;
            Page::Disk {
                offset,
                len: value.len(),
            }
        };
        self.pages.push(page);
        Ok(key)
    }

    fn take(&mut self, key: PageKey) -> Result<Bytes> {
        // The contract takes a key at most once, so move the page out, freeing
        // its heap (and budget) immediately if it was resident.
        match std::mem::replace(&mut self.pages[key.get() as usize], Page::Taken) {
            Page::Mem(bytes) => {
                self.resident -= bytes.len();
                self.budget.release(bytes.len());
                Ok(bytes)
            }
            Page::Disk { offset, len } => self.spill.read(offset, len),
            Page::Taken => Err(ParquetError::General(format!(
                "page {} already taken",
                key.get()
            ))),
        }
    }

    fn memory_size(&self) -> usize {
        self.resident
    }
}

impl Drop for SpillingPageStore {
    fn drop(&mut self) {
        // Return any still-resident pages (e.g. on an aborted write) to the
        // shared budget so it doesn't leak across row groups.
        if self.resident > 0 {
            self.budget.release(self.resident);
        }
    }
}

/// Creates a [`SpillingPageStore`] per column chunk, all sharing one [`Budget`]
/// and one [`SharedSpill`] file.
#[derive(Debug)]
struct SpillingPageStoreFactory {
    budget: Arc<Budget>,
    spill: Arc<SharedSpill>,
}

impl PageStoreFactory for SpillingPageStoreFactory {
    fn create(&self, _args: &PageStoreArgs<'_>) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(SpillingPageStore::new(
            self.budget.clone(),
            self.spill.clone(),
        )))
    }
}

fn main() -> Result<()> {
    let start = Instant::now();
    let args = Args::parse();
    let schema = build_schema(&args);

    // One uncompressed row group for the whole dataset
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(args.rows * 2))
        .build();

    let stats = Arc::new(SpillStats::default());
    let mut options = ArrowWriterOptions::new().with_properties(props);
    // Both spilling modes share one `SpillingPageStore` backed by one temp file;
    // they differ only in the memory budget (`--spill` keeps nothing resident).
    let buffering: String;
    let spilling = args.spill || args.mem_budget_mb.is_some();
    if spilling {
        let cap = args.mem_budget_mb.map_or(0, |mb| mb * 1024 * 1024);
        buffering = if cap == 0 {
            "SpillingPageStore (all pages to one shared temp file)".to_string()
        } else {
            format!("SpillingPageStore ({} MiB shared budget, then spill)", cap / (1024 * 1024))
        };
        options = options.with_page_store_factory(Arc::new(SpillingPageStoreFactory {
            budget: Arc::new(Budget::new(cap)),
            spill: Arc::new(SharedSpill::new(stats.clone())),
        }));
    } else {
        buffering = "InMemoryPageStore (default, on the heap)".to_string();
    }

    // Total logical payload across the large columns — the part that dominates.
    let large_payload = args.large_string_columns * LARGE_AVG_LEN * args.rows;
    println!(
        "Writing {} rows × {} columns ({} int, {} small-string ~{}B, {} large-string ~{} KiB)",
        args.rows,
        args.int_columns + args.small_string_columns + args.large_string_columns,
        args.int_columns,
        args.small_string_columns,
        SMALL_AVG_LEN,
        args.large_string_columns,
        LARGE_AVG_LEN / 1024,
    );
    println!("{:<31}: {}", "Page buffering", buffering);

    // Throw away all output since we're just measuring memory, not the file
    // size or contents.
    let sink = std::io::sink();
    let mut writer = ArrowWriter::try_new_with_options(sink, schema.clone(), options)?;

    let mut peak_memory = 0usize;
    let mut written = 0usize;
    while written < args.rows {
        let n = BATCH_SIZE.min(args.rows - written);
        let batch = make_batch(&schema, &args, written as u64, n);
        writer.write(&batch)?;
        written += n;
        // `memory_size()` reports the bytes the writer holds on the heap
        peak_memory = peak_memory.max(writer.memory_size());
    }
    peak_memory = peak_memory.max(writer.memory_size());
    writer.close()?;
    let elapsed = start.elapsed();

    println!("{:<31}: {written} rows", "Rows written");
    println!(
        "{:<31}: {:.1} MiB   <- bytes the writer held on the heap",
        "Peak ArrowWriter::memory_size()",
        mib(peak_memory),
    );
    println!(
        "{:<31}: {:.3} s",
        "Total elapsed time",
        elapsed.as_secs_f64(),
    );
    if spilling {
        println!(
            "{:<31}: {} pages ({:.1} MiB)",
            "Spilled to temp file",
            stats.pages.load(Ordering::Relaxed),
            mib(stats.bytes.load(Ordering::Relaxed) as usize),
        );
    } else {
        println!();
        println!(
            "Re-run with --spill to keep those pages off the heap and watch peak \n\
             writer memory drop well below the {:.1} MiB row group payload.",
            mib(large_payload),
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Schema + deterministic data generation.
// ---------------------------------------------------------------------------

/// Build the wide, skewed schema: a few integer columns, then small string
/// columns, then the fat large string columns.
fn build_schema(args: &Args) -> SchemaRef {
    let mut fields = Vec::new();
    for i in 0..args.int_columns {
        fields.push(Field::new(format!("int_{i}"), DataType::Int64, false));
    }
    for i in 0..args.small_string_columns {
        fields.push(Field::new(
            format!("small_str_{i}"),
            DataType::Utf8View,
            false,
        ));
    }
    for i in 0..args.large_string_columns {
        fields.push(Field::new(
            format!("large_str_{i}"),
            DataType::Utf8View,
            false,
        ));
    }
    Arc::new(Schema::new(fields))
}

/// Fill `buf` with a deterministic value of exactly `len` bytes derived from the
/// counter `n`. The 20-digit zero-padded counter makes every value distinct, so
/// the fat columns stay plain-encoded (high cardinality) rather than
/// dictionary-encoding away; the remainder is padded with a fixed `a`–`z` cycle.
fn fill_value(buf: &mut String, n: u64, len: usize) {
    use std::fmt::Write;
    buf.clear();
    let _ = write!(buf, "{n:020}");
    while buf.len() < len {
        buf.push((b'a' + (buf.len() % 26) as u8) as char);
    }
    buf.truncate(len); // all bytes are ASCII, so this is a clean char boundary
}

/// Build a string column of `rows` values, each exactly `len` bytes, keyed by
/// the global row index and a per-column `salt` so values are distinct.
fn make_string_array(rows: usize, row_offset: u64, salt: u64, len: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::with_capacity(rows);
    let mut value = String::new();
    for r in 0..rows {
        let n = (row_offset + r as u64).wrapping_mul(101).wrapping_add(salt);
        fill_value(&mut value, n, len);
        builder.append_value(&value);
    }
    Arc::new(builder.finish())
}

/// Build the record batch covering rows `[row_offset, row_offset + rows)`.
fn make_batch(schema: &SchemaRef, args: &Args, row_offset: u64, rows: usize) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut salt = 0u64; // distinguishes columns so they don't all hold equal values
    for _ in 0..args.int_columns {
        let s = salt;
        salt += 1;
        let vals: Vec<i64> = (0..rows)
            .map(|r| (row_offset + r as u64 + s) as i64)
            .collect();
        columns.push(Arc::new(Int64Array::from(vals)));
    }
    for _ in 0..args.small_string_columns {
        columns.push(make_string_array(rows, row_offset, salt, SMALL_AVG_LEN));
        salt += 1;
    }
    for _ in 0..args.large_string_columns {
        columns.push(make_string_array(rows, row_offset, salt, LARGE_AVG_LEN));
        salt += 1;
    }
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(seed: u8, len: usize) -> Bytes {
        Bytes::from(vec![seed; len])
    }

    /// The [`PageStore`] contract makes no ordering promise, so `put`/`take` must
    /// round-trip every blob regardless of how the caller interleaves them or in
    /// what order keys are taken. Uses a zero budget so every page goes through
    /// the shared file and is read back at its recorded offset.
    #[test]
    fn roundtrips_under_interleaved_out_of_order_access() {
        let spill = Arc::new(SharedSpill::new(Arc::new(SpillStats::default())));
        let mut store = SpillingPageStore::new(Arc::new(Budget::new(0)), spill);

        let (a, b, c, d) = (
            blob(0xAA, 100),
            blob(0xBB, 4096),
            blob(0xCC, 1),
            blob(0xDD, 7777),
        );

        let ka = store.put(a.clone()).unwrap();
        let kb = store.put(b.clone()).unwrap();
        assert_eq!(store.take(kb).unwrap(), b); // read, then put again below
        let kc = store.put(c.clone()).unwrap();
        assert_eq!(store.take(ka).unwrap(), a); // earlier key, out of order
        let kd = store.put(d.clone()).unwrap();
        assert_eq!(store.take(kd).unwrap(), d);
        assert_eq!(store.take(kc).unwrap(), c);
    }

    /// A write that fits the shared budget never opens the temp file and spills
    /// nothing, and the budget is fully released once the pages are taken.
    #[test]
    fn under_budget_never_touches_disk() {
        let stats = Arc::new(SpillStats::default());
        let spill = Arc::new(SharedSpill::new(stats.clone()));
        let budget = Arc::new(Budget::new(1024));
        let mut store = SpillingPageStore::new(budget.clone(), spill.clone());

        let (a, b) = (blob(0xAA, 400), blob(0xBB, 400));
        let ka = store.put(a.clone()).unwrap();
        let kb = store.put(b.clone()).unwrap();

        assert!(
            spill.state.lock().unwrap().file.is_none(),
            "nothing should have spilled"
        );
        assert_eq!(stats.pages.load(Ordering::Relaxed), 0);
        assert_eq!(budget.used.load(Ordering::Relaxed), 800);

        assert_eq!(store.take(ka).unwrap(), a);
        assert_eq!(store.take(kb).unwrap(), b);
        assert_eq!(budget.used.load(Ordering::Relaxed), 0, "budget released");
    }

    /// Once the shared budget is full, the overflow spills while the resident
    /// pages stay in memory; every page still round-trips.
    #[test]
    fn spills_only_the_overflow() {
        let stats = Arc::new(SpillStats::default());
        let spill = Arc::new(SharedSpill::new(stats.clone()));
        let budget = Arc::new(Budget::new(500));
        let mut store = SpillingPageStore::new(budget, spill.clone());

        let a = blob(0xAA, 400); // fits: 400 <= 500
        let b = blob(0xBB, 400); // overflow: 800 > 500 -> spill
        let ka = store.put(a.clone()).unwrap();
        let kb = store.put(b.clone()).unwrap();

        assert!(
            spill.state.lock().unwrap().file.is_some(),
            "overflow should have spilled"
        );
        assert_eq!(stats.pages.load(Ordering::Relaxed), 1);
        assert_eq!(store.memory_size(), 400);

        assert_eq!(store.take(kb).unwrap(), b); // from disk
        assert_eq!(store.take(ka).unwrap(), a); // from memory
    }

    /// The budget and the file are shared across columns: one store filling the
    /// budget forces another to spill, both columns' pages land in the one file
    /// (interleaved by offset), and a page bigger than the whole budget spills.
    #[test]
    fn budget_and_file_are_shared_across_columns() {
        let stats = Arc::new(SpillStats::default());
        let spill = Arc::new(SharedSpill::new(stats.clone()));
        let budget = Arc::new(Budget::new(500));
        let mut col0 = SpillingPageStore::new(budget.clone(), spill.clone());
        let mut col1 = SpillingPageStore::new(budget.clone(), spill.clone());

        let big = blob(1, 500); // fills the budget on col0 (stays resident)
        let s1 = blob(2, 300); // no room left -> col1 spills
        let s2 = blob(3, 700); // bigger than the whole budget -> spills too
        let _k0 = col0.put(big.clone()).unwrap();
        let k1 = col1.put(s1.clone()).unwrap();
        let k2 = col1.put(s2.clone()).unwrap();

        assert!(spill.state.lock().unwrap().file.is_some());
        assert_eq!(stats.pages.load(Ordering::Relaxed), 2, "two pages spilled");

        // Both spilled pages live in the single shared file and read back intact.
        assert_eq!(col1.take(k2).unwrap(), s2);
        assert_eq!(col1.take(k1).unwrap(), s1);
    }
}
