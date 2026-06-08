# wide-parquet

Write Parquet with many heterogeneous columns efficiently, in Rust.

This example shows how to use the Rust [`parquet crate`] to write wide tables
(1000s of columns) with large string values (16 KiB per row) using limited
memory.

The example reports the peak memory buffered by the underlying `ArrowWriter`. You
can also run it with a `--spill` argument, which writes buffered pages to
temporary files instead.

For the default configuration (8192 rows, 18 columns) the memory savings are
almost 100x:

| Configuration (8192 rows) | Heap Memory |
|---------------------------|-------------|
| Default                   | 1.25 GiB    |
| Spilling                  | 13.4 MiB    |

Using a more common row group size of 100,000 rows, the memory savings are even
more dramatic — more than 500x:

| Configuration (100,000 rows)  | Heap Memory |
|-------------------------------|-------------|
| Default                       | 15.65 GiB   |
| Spilling                      | 30.5 MiB    |

## Example output

You can see this by running `cargo run --release` and `cargo run --release -- --spill`:

```shell
$ cargo run --release
Writing 8192 rows × 18 columns (3 int, 5 small-string ~20B, 10 large-string ~16 KiB)
Page buffering                 : InMemoryPageStore (default, on the heap)
Rows written                   : 8192 rows
Peak ArrowWriter::memory_size(): 1283.7 MiB   <- bytes the writer held on the heap
Total elapsed time             : 1.172 s
```

```shell
$ cargo run --release -- --spill
Writing 8192 rows × 18 columns (3 int, 5 small-string ~20B, 10 large-string ~16 KiB)
Page buffering                 : SpillingPageStore (all pages to one shared temp file)
Rows written                   : 8192 rows
Peak ArrowWriter::memory_size(): 13.4 MiB   <- bytes the writer held on the heap
Total elapsed time             : 1.336 s
Spilled to temp file           : 2576 pages (1270.4 MiB)
```

## Background

The nature of Parquet is that data pages for a particular column chunk (the rows
for a column within a row group) must be contiguous in the output file, meaning
that the row group encoding must complete before the final bytes can be written.

By default, the [arrow-rs] Parquet writer, like many other Parquet writer
implementations, buffers the entire (compressed) row group in RAM before writing
it out to storage. While efficient, this can buffer very large amounts of data
for wide schemas or columns with large (e.g. string / image) values.

## PageStore

To avoid buffering an entire row group in memory, the Parquet writer can be
configured to use a [`PageStore`] for buffering the encoded pages. The store
factory is invoked once per column chunk, but the factory itself is shared, so
the per-column stores can share state: this example appends every spilled page to
a *single* temp file (one file descriptor for the whole writer, not one per
column) and addresses pages by their `(offset, len)`. A `PageStore` could equally
buffer pages in memory, write them to a remote object store, and more.

`--mem-budget-mb` shows the dynamic case: keep pages in memory up to a budget
shared across all columns and spill only the overflow, so a write that fits the
budget pays no I/O at all.

Nothing comes for free, of course: using a `PageStore` writes the bytes one extra
time — both to and from the store. Those bytes are efficiently encoded Parquet
data pages, though, not the original input data.

## Write overhead

Spilling sends every page through the store an extra time, so it is slower than
the default in-memory path. Measured here (median of interleaved runs, 30,000
rows × 18 columns, ≈4.6 GiB spilled):

| Spilling store    | Elapsed | vs. no-spill |
|-------------------|---------|--------------|
| none (in-memory)  | 2.52 s  | —            |
| temp-file spill   | 2.90 s  | +15%         |

That ~15% is mostly the unavoidable cost of writing the encoded pages out and
reading them back. Coalescing the writes/reads through a buffer doesn't help:
bounded data pages are already ~0.5 MiB, and one buffer per column would cost
real memory on wide schemas.

## Running

```sh
# Baseline: default in-memory page buffering. Peak writer memory grows with the
# row group.
cargo run --release

# Spill completed pages to temp files: peak writer memory stays bounded.
cargo run --release -- --spill

# Keep pages in memory up to a shared budget (across all columns), spilling only
# the overflow: writes that fit the budget pay no I/O, larger ones stay bounded.
cargo run --release -- --mem-budget-mb 128

# Make the schema wider / the skew worse:
cargo run --release -- --spill --large-string-columns 40
```

## Dependency pinning

The `PageStore` API is not yet in a published `parquet` release, so `Cargo.toml`
pins `parquet` (and the matching `arrow` crates) to the
[apache/arrow-rs](https://github.com/apache/arrow-rs) commit that merged it
([#10020](https://github.com/apache/arrow-rs/pull/10020)). Once it ships in a
crates.io release, switch the git dependencies to a version requirement.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

[`parquet crate`]: https://docs.rs/parquet
[arrow-rs]: https://github.com/apache/arrow-rs
[`PageStore`]: https://github.com/apache/arrow-rs/blob/main/parquet/src/column/page_store.rs
