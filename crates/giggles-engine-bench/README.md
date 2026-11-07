# GigglesDB engine benchmark: GD-0054 F0

This standalone Rust 2024 crate qualifies the frozen F0 MVCC access pattern on
redb, fjall, and optional RocksDB. It measures engine behavior, durability grouping, retained
versions, and space reclamation before a production engine choice. It does not
implement the GigglesDB server, SQL layer, replication, or production HLC.
Use Rust 1.99.1 and the checked-in `Cargo.lock` (`--locked`). Direct dependency
versions resolved for this qualification are redb **3.1.3**, fjall **3.1.13**,
clap 4.6.7, fastrand 2.5.0, hdrhistogram 7.6.0, serde 1.0.229,
serde_json 1.0.154, and memory-stats 1.2.0. The optional official `rocksdb`
crate is **0.25.0**, backed by `librocksdb-sys` **0.19.0+11.8.1**
(native RocksDB **11.8.1**). The `rocksdb` feature is disabled by default;
requesting that engine without the feature prints the required build flag.

## Frozen byte format

Every physical key is 25 bytes, sorted lexicographically:

| Bytes | Encoding |
| --- | --- |
| 0..8 | Table ID, unsigned big endian; benchmark uses table 0 |
| 8 | Signed integer primary-key type tag `0x02` |
| 9..17 | Signed i64 primary key, sign bit flipped, big endian |
| 17..25 | `u64::MAX - commit_timestamp`, big endian |

Values begin with one flag byte: `0x00` followed by the live payload, or exactly
`0x01` for a tombstone. Newest versions sort first within a logical key.
`get_at` seeks directly to the logical key plus inverted snapshot timestamp and
rejects a neighboring logical key. `scan_at` streams versions, resolves each
logical key once, skips tombstones, and returns up to the requested live rows.
The harness uses monotonic counter timestamps. Load increments the counter for
each loaded row; the run writer assigns one per submitted four-row transaction.
These counters deliberately substitute for the production HLC.

## Reproduce

Run from `K:/Projects/gigglesdb` in PowerShell. Choose a fresh unique task folder
for every experiment. Build outputs, temporary files, databases, and JSONL stay
under `J:/gigglesdb-work`; existing evidence must remain in place.

```powershell
$task = 'J:/gigglesdb-work/f0-example-20261107-a'
New-Item -ItemType Directory -Force "$task/tmp" | Out-Null
$env:CARGO_TARGET_DIR = "$task/target"
$env:TEMP = "$task/tmp"
$env:TMP = "$task/tmp"
cargo +1.99.1 build --release --locked --manifest-path crates/giggles-engine-bench/Cargo.toml
$bench = "$task/target/release/giggles-engine-bench.exe"
foreach ($engine in @('redb', 'fjall')) {
  $db = "$task/data-$engine"
  $out = "$task/$engine.jsonl"
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out load --dataset-gb 1
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out reopen
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out run --mix read --threads 16 --warmup-secs 30 --duration-secs 300 --group-max 64 --group-wait-us 500
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out run --mix write --threads 16 --warmup-secs 30 --duration-secs 300 --group-max 64 --group-wait-us 500 --pin-snapshot-secs 60
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out gc
}
```

For skewed traffic append `--zipf 0.99` to `run`; selection is uniform by default.
For a quick functional smoke use `load --dataset-gb 0.0001`, then
`run --threads 2 --warmup-secs 1 --duration-secs 2`. Use fresh databases to compare
independent mixes; the sequence above intentionally accumulates write history.
Common required arguments (`--engine`, `--dir`, `--out`) precede the phase.
The directory guard resolves existing ancestors and canonical paths before
accepting a database under J:/. An intentional other-drive experiment requires
`--allow-any-dir`. This opt-out does not change the project's output policy.

```powershell
cargo +1.99.1 test --locked --manifest-path crates/giggles-engine-bench/Cargo.toml
cargo +1.99.1 clippy --locked --manifest-path crates/giggles-engine-bench/Cargo.toml --all-targets -- -D warnings
```

The required F0 verification and smoke use the qualification task below. These
commands do not delete existing evidence. The two smoke database directories
must be fresh before the first load; choose a new unique task if they already
contain a completed run. Run the two Cargo checks before invoking the binary.

```powershell
$task = 'J:/gigglesdb-work/trackF-f0-20261107'
New-Item -ItemType Directory -Force "$task/tmp", "$task/data", "$task/out" | Out-Null
$env:CARGO_TARGET_DIR = "$task/target"
$env:TEMP = "$task/tmp"
$env:TMP = "$task/tmp"
cargo +1.99.1 build --release --locked --manifest-path crates/giggles-engine-bench/Cargo.toml
cargo +1.99.1 test --release --locked --manifest-path crates/giggles-engine-bench/Cargo.toml
cargo +1.99.1 clippy --release --locked --manifest-path crates/giggles-engine-bench/Cargo.toml --all-targets -- -D warnings
$bench = "$task/target/release/giggles-engine-bench.exe"
$out = "$task/out/smoke.jsonl"
foreach ($engine in @('redb', 'fjall')) {
  $db = "$task/data/smoke-$engine"
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out load --dataset-gb 0.05
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out reopen
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out run --mix read --threads 16 --warmup-secs 5 --duration-secs 20 --group-max 64 --group-wait-us 500
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out run --mix write --threads 16 --warmup-secs 5 --duration-secs 20 --group-max 64 --group-wait-us 500
  & $bench --engine $engine --dir $db --cache-mb 1024 --value-bytes 300 --out $out gc
}
```

## RocksDB F0b reproduction and settings

The frozen RocksDB build uses Rust 1.99.1, MSVC 2022 Community, and LLVM 23.1.5.
No global environment or toolchain changes are required. Run this in PowerShell
from the repository root; use a new unique task directory for a new experiment.

```powershell
$task = 'J:/gigglesdb-work/trackF-f0-20261107'
New-Item -ItemType Directory -Force "$task/tmp", "$task/data", "$task/out" | Out-Null
$env:CARGO_TARGET_DIR = "$task/target"
$env:TEMP = "$task/tmp"
$env:TMP = "$task/tmp"
$env:LIBCLANG_PATH = 'J:/tools/LLVM/bin'
# If CMake is not already visible, prefix this command-local shell PATH:
$env:PATH = 'C:/Program Files/Microsoft Visual Studio/2022/Community/Common7/IDE/CommonExtensions/Microsoft/CMake/CMake/bin;' + $env:PATH
cargo +1.99.1 build --release --locked --features rocksdb --manifest-path crates/giggles-engine-bench/Cargo.toml
cargo +1.99.1 test --release --locked --features rocksdb --manifest-path crates/giggles-engine-bench/Cargo.toml
cargo +1.99.1 clippy --release --locked --features rocksdb --manifest-path crates/giggles-engine-bench/Cargo.toml --all-targets -- -D warnings
$bench = "$task/target/release/giggles-engine-bench.exe"
$db = "$task/data/smoke-rocksdb"
$out = "$task/out/smoke.jsonl"
& $bench --engine rocksdb --dir $db --cache-mb 1024 --value-bytes 300 --out $out load --dataset-gb 0.05
& $bench --engine rocksdb --dir $db --cache-mb 1024 --value-bytes 300 --out $out reopen
& $bench --engine rocksdb --dir $db --cache-mb 1024 --value-bytes 300 --out $out run --mix read --threads 16 --warmup-secs 5 --duration-secs 20 --group-max 64 --group-wait-us 500
& $bench --engine rocksdb --dir $db --cache-mb 1024 --value-bytes 300 --out $out run --mix write --threads 16 --warmup-secs 5 --duration-secs 20 --group-max 64 --group-wait-us 500
& $bench --engine rocksdb --dir $db --cache-mb 1024 --value-bytes 300 --out $out gc
# Run after the RocksDB smoke, since this replaces the executable:
cargo +1.99.1 build --release --locked --manifest-path crates/giggles-engine-bench/Cargo.toml
```

First feature-enabled release build wall clock: **157.083 seconds (2m 37.083s)**,
successful on the first attempt with the toolchain and LZ4 settings above.
Native compilation can take several minutes. Features are limited to `lz4` and
`bindgen-runtime`; LZ4 compression is explicitly selected. There is one default
column family, a `cache_bytes` LRU block cache, a 10-bit/key full Bloom filter,
cached index/filter blocks, default level compaction settings, and
`max_background_jobs = min(8, available CPUs)`. Other RocksDB options retain
upstream defaults. The engine uses WAL-enabled atomic `WriteBatch` operations;
durable batches set sync true and load/GC batches set sync false. Final flush
syncs the WAL and flushes memtables. GC calls `compact_range(None, None)`.

On Windows the adapter converts canonical verbatim disk/UNC paths to equivalent
ordinary absolute paths before native open, because RocksDB appends slash-separated
filenames that Windows verbatim path syntax rejects.

Iterators stream native RocksDB records lazily and retain their native consistent
view. A pin holds an actual native RocksDB snapshot on a dedicated holder thread:
the thread owns an `Arc<DB>` and borrows it locally, acknowledges snapshot creation,
and waits for release. The returned Send+Sync guard signals release and joins the
thread on drop. Snapshot destruction precedes the thread's DB reference release,
so a pin may safely outlive its backend without unsafe lifetime extension or leaks.
The DB lock remains held until the last pin is dropped. Snapshot retention can
delay reclamation; compaction and immediate directory-size samples do not promise
a settled minimum.

## Phases and measurements

`load` requires an empty database and creates
`floor(dataset_gb * 1e9 / (25 + 1 + value_bytes))` rows, with dense primary keys
starting at zero. A fixed-seed shuffle controls insertion order, and fixed-seed
random payloads avoid giving compression an artificial constant-byte workload.
It writes batches of 10,000 rows with per-batch sync disabled, then performs one
final sync. It reports rows/s, logical decimal MB/s, elapsed time, and disk bytes.
The shuffle holds one u64 per loaded row in memory.

`reopen` closes the initial handle, measures reopening, and measures the first
`get_at` separately, before metadata discovery. Other phases first stream
physical MVCC records to recover the maximum committed timestamp and primary-key allocation counter.
No external counter or metadata file is needed. Handle reopen is not a cold OS
page-cache measurement. Setup discovery for run and GC can warm that cache.

`run` defaults to 16 workers, 30 seconds warmup and 300 measured seconds. The
read mix is 90% point lookups, 5% scans of up to 100 live rows, and 5% four-row
update transactions. The write mix is 20% point lookups and 80% four-row write
transactions; each write row independently chooses 90% existing-key update and
10% new unique primary key. Reads and updates select the existing dense key
range discovered at phase start; inserts extend beyond that range and become
eligible after reopening. Use harness-generated databases; arbitrary sparse
primary keys or manual tombstones are outside this workload selection contract.
Each changed row receives fresh deterministic random payload bytes.

A single group writer assigns run timestamps, collects up to 64 transactions
or 500 microseconds by default, atomically writes their physical versions in a
durable batch, and only then publishes the visible timestamp and acknowledgments.
Warmup workers finish before measured counters and grouping statistics reset.
Reported latency histograms exclude warmup; point/scan timings include their
selection and engine call, while writes measure submit through durable ack.
Reports include p50/p90/p99/p99.9/max microseconds, per-operation throughput,
committed transactions and rows, a group-size frequency distribution, measured
start/end disk bytes, and RSS sampled approximately once per second plus its
sampled peak. Completed in-flight operations count and can slightly extend the
requested duration. Worker scheduling can change the interleaving despite fixed
random seeds.

`--pin-snapshot-secs X` holds an actual engine snapshot across X seconds from
the measured phase start. Its separate result records disk/RSS around retention
and snapshot release. If X exceeds the measured duration, the process waits for
release after taking the measured end-disk sample. Release does not promise an
immediate reduction in disk bytes or RSS.

`gc` uses the recovered latest timestamp as watermark W. One streaming full pass
keeps all versions newer than W and the newest live version at or below W. A
tombstone at that boundary causes all versions at or below W to be deleted.
Physical deletions are applied in 10,000-key batches without per-batch sync,
followed by final sync. It then invokes native engine compaction and reports
whether that operation is exposed, along with before/after-GC/after-compaction
disk bytes. GC runs separately from foreground traffic and pinned snapshots.

For fjall 3.1.13, `SyncAll` persists the WAL without forcing the active memtable
into SST files. Its native `major_compact` compacts on-disk tables without first
flushing that memtable. Immediate directory-size samples can therefore grow
during GC/compaction and do not represent a settled minimum or guarantee space
reclamation. Closing and reopening can also change the files on disk.

Every successful phase appends one compact JSON object and newline to `--out`
and prints the same object to stdout. The envelope contains `engine`, exact
engine `version`, `phase`, `parameters`, `results`, and `host` metadata (CPU count,
OS, architecture, hostname, and total RAM where available). Errors print a
context message to stderr and exit nonzero. Keep raw JSONL and commands with
qualification evidence; do not substitute a tiny smoke for a capacity run.

The configured 1 GiB engine cache does not cap the OS page cache. On the intended
approximately 95 GB RAM host, small datasets can be served largely from RAM.
Report physical RAM, dataset size, warm/cold-cache procedure, free space, and
other machine activity alongside comparisons. F0 deliberately does not claim
production HLC, concurrency-control, crash-injection, or end-to-end SQL results.
