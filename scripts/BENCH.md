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

If a driver dies, finish its box by hand with these coordinator steps.
Workers never run `boat`.

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

## Optional profiling (`--profile`)

Add `--profile` for one diagnostic run per selected GigglesDB target/client
count after all measured repeats, using the first mix and protocol group
(native uses native). Diagnostic results stay out of `results.jsonl` and
summary medians. Normal runs use the unchanged release binary, without perf.
A separate build in `$CARGO_TARGET_DIR-profile` enables frame pointers and
`line-tables-only` debug information. It has a 1500 s timeout plus 10 s to
kill; failure is nonfatal and falls back to the normal binary with DWARF.

The phase starts its budget before checking/installing perf. Installation
has a 180 s timeout plus 10 s to kill (package-lock wait is at most 60 s).
Each target gets a fresh server and a 150 s diagnostic load, including schema
setup/seeding, bounded by a 240 s timeout. After up to 60 s waiting for the
first `completed=` line, captures run sequentially while the load is active:

| Step | Capture | Timeout |
|---|---|---|
| CPU, frame pointers | 499 Hz, `--call-graph fp`, 30 s | 45 s |
| CPU, DWARF fallback | 99 Hz, `dwarf,8192`, 20 s | 35 s |
| Lock | BPF contention, 30 s | 45 s |
| Lock fallback | `perf lock record`, 30 s | 45 s |
| Off-CPU | `offcputime-bpfcc`, 30 s | 45 s |
| Off-CPU fallback | `perf sched record`, 20 s | 35 s |
| Lock report / sched timehist | after load ends and server stops | 120 s each |
| CPU report | stdio, no children, 0.5% minimum | 180 s |
| CPU script and AWK fold | both pipeline members bounded independently | 300 s each, concurrently |
| Flamegraph render | inferno or flamegraph.pl, when installed | 60 s |

Every timeout uses `--kill-after=10`; root captures/processors run timeout
inside `sudo -n` so root perf can be killed. The perf version/sysctl checks
and data-size check are bounded at 15 s too. Every per-target capture,
processing and load timeout is clamped to the remaining deadline minus 10 s;
steps with less than 15 s available are skipped and logged. The diagnostic
load's timeout kills its process group, including tsx children. Captures
finish before waiting for the load, stopping the server, and post-processing.
Raw files survive until processing ends, then are deleted. A nonempty CPU
capture with exit 0 or timeout exit 124 counts as usable. Above 1 GB
(1,073,741,824 bytes), `perf script` and folding are skipped; the bounded
CPU report is still attempted. Missing tools/permissions never fail the bench.

Normal time is about 5–6 min per target: server start about 20 s, seeding,
a 150 s load covering roughly 95 s of captures, and 1–3 min for report/script.
Worst case is 15 min per target; the whole phase has a 30 min per-box budget.
Before starting a target, the runner reserves 900 s; if it would exceed
1800 s since phase start, that target and all later targets are skipped.

| Per-target worst-case component | Bound |
|---|---|
| `start_server` | <=160 s: two 60 s readiness waits plus up to 40 s stop |
| All later capture/load/processing steps | clamped to start + 840 s, with 10 s kill-after reserved |
| Final `stop_server` | <=40 s |
| Total (including up to 10 s kill-after allowance) | <=890 s, within the 900 s reservation |

The matrix estimate adds `1510 + min(1800, 270 + profile_runs * 900)` seconds
when profiling has non-pg targets; `profile_runs` is non-pg targets times
client counts, and 270 s covers perf installation (190 s) plus the bounded
version and sysctl checks. This covers the separate build and bounded phase. The measured
estimate is still runs times (warmup + duration + 30 s), with 20 min separately
reserved for normal build/setup. The launch log shows measured and profile
estimates separately; split matrices that exceed the TTL allowance.

Progress includes each capture and post-processing start and budget skips.
The local console log mirrors changed box progress lines, also saved in the
run folder's `progress.log`. At TTL minus 600 s the waiter gives up and
attempts partial copy-back before deleting the box; other failures after
runner launch also trigger best-effort copy-back. Archive creation, boat
execution and download are bounded. Incomplete/missing files are tolerated.

`profiles/` is included in `logs.tar.gz` and extracted beside `summary.md`:
`top-<target>-c<clients>.txt`, `lock-...txt`, `offcpu-...txt`, and
`folded-...txt`; `flame-...svg` is produced when `inferno-flamegraph` or
`flamegraph.pl` is already on PATH. Raw perf files and `raw-*` directories
are excluded from download as a backstop. The run folder also contains
`results.jsonl`, `meta.json`, and the copied box `progress` file.
`meta.json` records `profile_requested`, `profiling_ran` (usable CPU capture),
`perf_version`, `profile_callgraph`, and `profile_build_seconds`. Failures
are in `logs/build-profile.log`, `logs/profile-*.capture.txt` and `logs/perf-*`.

Each measured JSONL record also includes `ops` and `protocols` aggregates:
count, p50_ms, p95_ms, p99_ms, max_ms, errors. Counts include failed operations;
errors count terminal failures after retries, preserving existing semantics.
Warmup is a separate invocation and is excluded. The summary includes a
per-operation table for each measured run.
