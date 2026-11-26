# End-to-end benchmarks on boat.dev (GD-0082)

`scripts/bench-remote.sh` compares PostgreSQL 18, GigglesDB SQL (pgwire) and
GigglesDB native (the HTTPS `text` arm) with `scripts/app-loadtest load`, on
one large boat.dev box started from the `gigglesdb-bench-base` snapshot.

## Running it

```
scripts/bench-remote.sh --tip <commit> --who <name> \
  --targets pg,giggles-sql,giggles-native --opset common \
  --clients 8,32 --mixes read,write,tx --warmup 30 --duration 300 --repeats 3
```

`--tip` is any commit in the shared repository. To bench uncommitted work,
make a throwaway commit object (`git commit-tree`, never a move of
`refs/heads/main`). The script sends it as a bundle and deletes its temporary
ref afterwards. Run `scripts/bench-remote.sh` with no arguments for every
option.

- Targets: `pg`, `giggles-sql` and `giggles-native`. Both GigglesDB targets
  use the same config-mode server: v2 engine, synced commits, 128 pgwire
  connections, 128 API sessions, mTLS on HTTPS. The request rate limiter
  (`service_limits`) is set to its maximum, burst and per_second 1,000,000.
  The default of 4096 per second rejects a 32-client run with 53300 or
  HTTP 429. giggles-native still uses the
  pgwire listener for schema setup and seeding.
- Every measured config (target, clients, mix, protocol, repeat) gets a fresh
  server and an empty data directory. Warmup and the measured run share that
  server.
- Mixes: `read`, `write`, `tx` and `error` put 100% of operations on that
  kind. `mixed` uses app-loadtest's default weights, read=60, write=25,
  tx=10, error=5. You can also give raw weights, such as `read=50+write=50`.
  Every mix draws only from the op set.
- Protocols apply only to the pgwire targets (`all` means
  simple+extended+prepared, chosen at random per operation). giggles-native
  runs once per (clients, mix) and is labelled `protocol=native`.
- Cores: servers are pinned to all cores but the last two, and the client to
  the last two (`--server-cpus` / `--client-cpus` override this).

## Op sets (`--opset`)

| Op set | Targets | Operations |
|---|---|---|
| `common` (default) | all three | pk, email, keyset, range_in, insert_returning, multi_insert, update, optimistic, bulk_delete, rollback, savepoint, unique, fk, check |
| `sql-full` | pg, giggles-sql only | common plus pagination, search, join_sum, large_in, upsert, roundtrip |

`sql-full` is the default app-loadtest load set without count_exists, cte
and window, which GigglesDB SQL rejects with 0A000, and without transfer
(FOR UPDATE), place_order and cancel_order. The script refuses `sql-full`
together with giggles-native.

Runs use `--max-error-rate 0`. A run with any error, a broken invariant or a
server that fails to start is marked FAILED in `summary.md`, and the script
exits 1. Failed runs are never left out of the table.

## Matrix size and splitting

Each run is estimated at warmup + duration + 30 s. The total must fit in the
box TTL (at most 7200 s) minus 20 minutes for the build and setup. Otherwise
the script refuses the matrix (exit 2). Split it into several invocations,
for example one `--clients` value or one `--mixes` value each, or lower
`--repeats` or `--duration`. Each invocation is one box start.

## Running several benches at once

Launch each `bench-remote.sh` as its own independent background process
(for example separate `nohup ... &` commands or separate tool background
tasks), staggered by about 30 s. Do not fan out through one parent such as
`xargs -P`: killing that parent can kill the children without running their
EXIT trap. HUP, INT and TERM are trapped and clean up, but a hard kill
(`kill -9`, `taskkill /F`) cannot be trapped.

The next gate or bench that takes a slot reclaims a dead driver's slot
automatically (`scripts/remote-slots.sh`: box deleted, END logged as
`rc=reclaimed`, ref removed, slot released; see
`J:/gigglesdb-work/boat/reclaim.log`), but it does not copy results back. To
keep a dead driver's results, first remove the `lease` file from its slot
directory (a slot without a lease is never reclaimed), then finish its box by
hand with these coordinator steps. Workers never run `boat`.

1. Find the box id and slot in
   `J:/gigglesdb-work/boat/slots/slot-*/owner` and `usage.log`; identify the
   matching run directory and START line.
2. Wait until `/home/user/bench/done` exists on the box:
   `boat exec <id> -- cat /home/user/bench/done`. Read the file's `rc=` value as
   `<rc>` and compute elapsed minutes, rounded up, from the START time.
3. Collect the logs:
   `boat exec <id> -- bash -lc 'cd /home/user && tar czf bench-logs.tar.gz bench bench.out'`.
4. Run `boat scp <id>:/home/user/bench-logs.tar.gz <run-dir>/logs.tar.gz`
   and extract `summary.md`, `results.jsonl` and `meta.json` from the archive.
5. Delete the box: `boat delete <id> --yes`.
6. Append an END line to `J:/gigglesdb-work/boat/usage.log` in the script's
   format: `<utc> | <who> | <box> | large | <tip12> | END bench rc=<rc> <mins>min`.
7. Release that run's slot with
   `rm -rf J:/gigglesdb-work/boat/slots/slot-<n>`.
8. Find the run's ref with `git for-each-ref refs/bench-remote`, then remove
   it with `git update-ref -d refs/bench-remote/<stamp>-<pid>`.
9. Remove `<run-dir>/stack.bundle`.

## Results

Results land in `J:/gigglesdb-work/boat/bench/<utc>-<tip12>-<who>-<pid>/`:

- `summary.md`: medians per config, failures and the data-dir filesystem;
- `results.jsonl`: one line per run;
- `meta.json`: hardware, PostgreSQL settings, the GigglesDB server config,
  the op set, and the data dir's filesystem type and device;
- `logs.tar.gz`: per-run app-loadtest reports and logs, server logs, the
  build log and `logs/data-probe.log` (findmnt, df -T, lsblk).

## Data directory

`/home/user` on the box is FUSE (`ascii-lazyfs`), which distorts fsync-bound
numbers. The runner probes `/mnt`, `/var/tmp`, `/tmp`, `/data`, `/srv`,
`/opt` and `/var/lib`. It takes the first writable ext4, xfs, btrfs or f2fs
mount whose source is a `/dev/*` block device and that has at least 8 GB
free. `--data-root <path>` overrides the probe. If nothing qualifies, the
data stays on `/home/user`, and both summary.md and meta.json say so.

## boat rules it follows (AGENTS.md "Remote gates on boat.dev")

- Only coordinators run it. It enforces the daily start cap (200, counted in
  `J:/gigglesdb-work/boat/usage.log`) and takes one of the shared
  `max_boxes` slots (`J:/gigglesdb-work/boat/slots`).
- Every box is large, uses `--no-env` and a TTL of at most 7200 s. It never
  uses `--no-auto-stop`, `--count` or xlarge.
- It writes a START and an END line (rc, minutes) to usage.log. On every exit
  path it deletes the box and releases the slot.
- Code goes up only as a git bundle.
- `--build-snapshot` rebuilds `gigglesdb-bench-base` from
  `gigglesdb-gate-base` and never writes `gigglesdb-gate-base`.

## Caveats

- Cloud noise. Boxes differ in CPU and neighbours, so absolute numbers vary
  between boxes. Compare ratios between targets measured on the same box,
  and use repeats. Targets alternate inside each repeat so drift hits all of
  them alike.
- Native `created_at`. The native text arm has no `now()`, so the
  app-loadtest native frontend sends `created_at` from the client. The pgwire
  targets use the server default.
- Not every operation runs natively (see `not_applicable` in results.jsonl),
  so only `common` compares all three targets.
- Client and servers share one box. The client is pinned to two cores, which
  can limit very high client counts.

## Disk flush latency (`--slow-disk`)

Before the build, the runner measures raw `fdatasync` on the data directory
(400 appends of 1 KB, each followed by `fdatasync`) and writes the mean, p50
and p99 in microseconds to `logs/fsync-probe.txt`. Commit latency, and on a
slow device also CPU per operation, depends on this value, so record it with
every result.

- `--slow-disk off` (default): probe only. A failed probe is logged and the
  run continues.
- `--slow-disk require`: when the mean is under `--min-fsync-us` (default
  2000), stop before any run. `summary.md` says so and the script exits 6, so
  a fast-disk box costs one start and no matrix time.
- `--slow-disk auto`: when the mean is under the threshold, build a 10 GB
  ext4 device from dm-delay (write 1 ms, flush 3 ms; `SLOW_WRITE_MS` /
  `SLOW_FLUSH_MS` on the box) over a direct-I/O loop file, probe it again and
  run there. Setup fails (rc 2) if the emulated device still measures under
  the threshold. Emulated WAL syncs measured about 7.5-9 ms (GD-0084 E1086).

With `--profile`, each profiled window also records exact per-thread CPU and
context switches from `/proc` (`profiles/threads{0,1,2}-<key>.txt`, with the
load's `completed=` count; use threads0 to threads2 for per-op figures, since
the count is logged every 2 s), and a `comm,dso` perf report. The profile
phase skips targets that cannot finish 15 minutes before the box TTL.

## Optional profiling (`--profile`)

Add `--profile` for one diagnostic run per selected GigglesDB target/client
count after all measured repeats, using the first mix and protocol group
(native uses native). Diagnostic results stay out of `results.jsonl` and
summary medians. Normal runs use the release binary without profiling.
A separate build in `$CARGO_TARGET_DIR-profile` enables frame pointers and
`line-tables-only` debug information (1500 s timeout plus 10 s to kill).
Build failure is nonfatal: on-CPU capture falls back to the normal binary
with DWARF; off-CPU user stacks will then be poor.

The benchmark snapshot installs `bpfcc-tools`, `bpftrace`, `sysstat`, running
kernel headers, `linux-tools` (perf), and pinned inferno 0.12.8 (`--locked`).
Run `scripts/bench-snapshot-setup.sh --probe-only` on the snapshot to check
these without package installation or building. Each required probe is
bounded at 45 s and reports OK or FAILED; all probes run before failure:

| Probe | Meaning |
|---|---|
| btf | Running kernel exposes readable `/sys/kernel/btf/vmlinux` |
| kprobe / uprobe | bpftrace can attach to kernel sleep / libc getpid |
| futex | Syscall tracepoint tracing works |
| offcputime / wakeuptime | bcc scheduler tracing and folded output work |
| biolatency / syscount | Block latency and syscall collectors attach |
| perf-trace / perf-sched | perf syscall and scheduler events are accessible (not an off-CPU fallback) |
| pidstat / iostat | Thread and device observers run |
| inferno | Folded stacks render as SVG |
| perf_lock_bpf | Optional `perf lock contention -b` support; absence is nonfatal |

The diagnosis block (`~/bench-perf-diagnosis.txt`) shows virtualization,
container/cgroup context, `perf_event_paranoid` (perf access), `kptr_restrict`
(kernel symbol visibility), unprivileged BPF policy, active lockdown, BTF,
and available kernel BPF/kprobe configuration. Probe logs are retained at the
printed path; failures print the diagnosis too. bcc needs headers for the
**running** kernel, even when the snapshot has headers for another kernel.
Ubuntu perf commonly lacks BPF lock support; the runner skips it explicitly.

Each target starts a fresh server and a 360 s diagnostic load. After waiting
up to 60 s for its first `completed=` line (schema setup/seeding precedes it),
the runner starts background observers and sequential stack captures:

| Step | Capture |
|---|---|
| Background, about 300 s | pidstat, iostat, biolatency, fsync histogram, syscount |
| On-CPU, 30 s | 499 Hz frame pointers; fallback 99 Hz `dwarf,8192` |
| Off-CPU, 30 s | `offcputime-bpfcc -f -d` |
| Futex, 30 s | bpftrace user stacks, summed wait Âµs and counts |
| Optional lock, 30 s | `perf lock contention -b` when supported |
| Wakeup, 20 s | `wakeuptime-bpfcc -f` |
| After captures/load | Copy RocksDB LOGs, stop server, CPU report/folding, SVGs, wait report |

Observers are bounded at 310 s. Every timeout uses `--kill-after=10`;
root collectors run timeout inside `sudo -n`. Missing tools and permissions
are nonfatal and recorded in the manifest. Capture, load and processing
budgets are clamped to the remaining target deadline minus 10 s; less than
15 s available means skip. The load timeout kills its process group,
including tsx children. CPU perf data above 1 GB skips script/folding but
still gets a bounded report. Raw files are deleted after processing.

The phase budget is 1800 s, reserving 900 s before each target. Each target
allows up to 160 s server startup, clamps later steps to start + 840 s,
and leaves room for final stop/kill. The matrix estimate adds
`1510 + min(1800, 270 + profile_runs * 900)` seconds; `profile_runs` is
non-pg targets times client counts. Split matrices exceeding the TTL allowance.
Progress and skipped steps appear in `progress.log` and the copied box
`progress`. At TTL minus 600 s, the waiter attempts partial copy-back before
deleting the box; failures after launch also attempt copy-back.

`profiles/` travels in `logs.tar.gz` and is extracted to **`<run>/profiles/`**.
Names below use `<key>` such as `giggles-sql-c32`:

| File | Contents |
|---|---|
| `folded-<key>.txt`, `top-<key>.txt`, `flame-cpu-<key>.svg` | CPU folded stacks, text report, flamegraph |
| `offcpu-<key>.folded`, `flame-offcpu-<key>.svg` | Blocked time in Âµs and flamegraph |
| `futex-<key>.txt` | User stack wait-time and count maps |
| `wakeup-<key>.folded`, `flame-wakeup-<key>.svg` | Waker stacks/time and flamegraph |
| `lock-<key>.txt` | Optional perf BPF lock contention |
| `pidstat-<key>.txt`, `iostat-<key>.txt` | Thread CPU/context switches and extended device samples |
| `biolatency-<key>.txt`, `fsync-<key>.txt`, `syscount-<key>.txt` | Block latency, fsync/fdatasync histograms, syscall totals |
| `manifest-<key>.txt` | Capture status, actual duration, start, PID and thread names |
| `rocksdb-<key>/LOG-n` | Up to five live LOG files, capped at 20 MiB each |
| `wait-report-<key>.md` | Aggregated waits, idle split, locks and storage/thread summaries |
| `ran-<key>`, `ran-offcpu-<key>` | Usable CPU capture / nonempty off-CPU capture markers |

Raw perf data and `raw-*` directories are excluded from download. `meta.json`
(at the run root) records `profile_requested`, `profiling_ran` (CPU),
`offcpu_ran`, `perf_version`, `profile_callgraph`, and `profile_build_seconds`.
Check manifest statuses too: a nonempty off-CPU marker alone is not proof of
valid stacks. Failures are in `logs/build-profile.log`,
`logs/profile-*.capture.txt` and `logs/perf-*`.

Start with `wait-report-<key>.md`. Its main table ranks working waits; idle
Tokio parking is expected and summarized separately. Thread names have
trailing worker numbers removed. `% of wall` divides summed wait by capture
time and the matching thread count; absent thread counts show n/a. The
flame-offcpu SVG gives the full picture: width is blocked time, with thread
name at the root. Unknown symbols limit classification; inspect full stacks
rather than treating every futex as useful work.

The futex table shows user-space waits in parking_lot, std Mutex/Condvar,
and channels. Fsync histograms plus biolatency show durability cost; pidstat
shows which threads burn CPU versus context switch; iostat `w_await` and
`%util` show device pressure/saturation. Off-CPU explains where threads wait
while on-CPU shows where active time goes: together they can explain the
observed roughly 60% idle cores. Captures are sequential samples, so their
percentages are not an exact accounting of a single shared time interval.
BPF can add overhead on very hot futex paths. RocksDB has no live stats
endpoint here, so only LOG files are copied.

Rerun the helper locally (Python 3.10+, stdlib only):

```sh
python -I scripts/bench-wait-report.py <run>/profiles
```

Use `--key KEY`, `--top N` (default 15), `--out FILE`, or `--full` to retain
full stacks in the wait tables. Missing/empty captures stay visible as
not captured; consult their manifest status.

Each measured JSONL record also includes `ops` and `protocols` aggregates:
count, p50_ms, p95_ms, p99_ms, max_ms, errors. Counts include failed operations;
errors count terminal failures after retries, preserving existing semantics.
Warmup is a separate invocation and is excluded. The summary includes a
per-operation table for each measured run.

## Agent-load suite (GD-0061 R3-404)

Run `scripts/bench-remote.sh --suite agent-load --tip <commit> --who <name>`
to build the release server and run the full application latency bound harness
on a large benchmark box. This mode uses a fixed 45-minute estimate, ignores
matrix options, and refuses `--profile` and `--build-snapshot`. The normal cap,
slot, TTL, bundle and box cleanup rules apply. `--data-root` overrides the box
data parent; `--results-root <local dir>` overrides the local results parent
(default `J:/gigglesdb-work/boat/bench`) for either suite.

Results include `agent-load.json`, `summary.md`, `meta.json` and `logs.tar.gz`.
The archive includes harness artifacts except database `data` directories.
Harness exits are 0 (pass), 2 (application errors), 3 (bound failure), and 1
(harness failure); setup failures use rc=2 with an `AGENT-LOAD SETUP FAILED`
summary marker. The remote wrapper succeeds only for rc=0. No smoke mode is
used. The box needs host OpenSSL and Python 3 with the standard library.
