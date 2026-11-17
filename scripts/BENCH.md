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

## Results

Results land in `J:/gigglesdb-work/boat/bench/<utc>-<tip12>/`:

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
