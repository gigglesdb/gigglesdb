# S4 Docker spike (GD-0049 / RW0)

Requires Windows, Python 3, Docker Desktop Linux engine + Compose v2; `run.sh`
requires Git Bash. No Cargo project, host data binds, host ports, or production credentials.
All services, named volumes and the isolated network carry `skedaddle.test=<run-id>`.
Postgres 16/17 use the data mount; 18 uses its new parent-directory mount (no anonymous volume).

```powershell
.\run.ps1 -PgMajor 18 -Mode smoke
.\run.ps1 -PgMajor 18 -Workload { Invoke-Engine 'smoke'; Invoke-Engine 'privileges' }
.\cleanup.ps1 -Check
.\cleanup.ps1 -WhatIf
.\cleanup.ps1 -Images -WhatIf
```

```bash
RUN_ID=my-unique-run PG_MAJOR=18 MODE=smoke bash run.sh
RUN_ID=my-failure WORKLOAD='exit 23' bash run.sh
```

Use fresh lowercase run IDs. PowerShell accepts a scriptblock callback; Bash accepts
`WORKLOAD` as shell code, or `MODE=smoke|privileges|large|hold|fail`.
The run's environment identifies its PG major and run ID for `harness.py` operations.
`large` initializes pgbench scale 60, checks database size under 1,000,000,000 bytes,
then updates a bounded hot set six times. It is verification code, not a general size limiter.
Logs stay on stderr; Docker log rotation bounds each container to about 4MB.

The disk guard runs first, before sweeper/up/directories. It reads C: free bytes and
runs the pinned tiny BusyBox image with a run label and `--rm`, executing `df -Pk /`.
That overlay is backed by Docker VM's data filesystem; no host filesystem mount is used.
If BusyBox is missing, C: is checked before its small pull; VM free is checked immediately afterward.
All other missing-image pulls happen only after both guards pass.
Both C: and VM need **4 GiB warm**, plus **3 x image-inspect Size** for missing images;
all eight missing require **5,682,386,497 bytes (about 5.29 GiB)**. See `budget.json`:
measured peak volume growth 1.252GB + 50MiB evidence + 2GiB margin, rounded up to 4GiB;
measured cold-image growth 1.188GB is covered by the 1.387GB image allowance.
Approximately 2GB free is refused. `-BudgetGiB` / `BUDGET_GIB` overrides the total budget
for testing. The harness never prunes; the author may choose their own reclamation.

Startup/end sweepers remove known dead runs, preserve live/unknown owners, and recover
orphan logs before removal. `cleanup.ps1` explicitly removes *all* test-labeled
containers/volumes/networks; `-Check` lists leftovers and exits nonzero. It never
touches unlabeled resources. `-Images` targets only exact manifest digest references;
**only `-Images -WhatIf` was verified; real image removal was not executed.**

PowerShell uses try/finally; Bash traps EXIT/INT/TERM and waits for its callback in the
background so signals invoke teardown. Cleanup steps are independent of export failure.
A hard process kill cannot execute finally; next startup sweeps that dead run.
Evidence includes raw PG stderr/server logs, other service logs, inspect, Compose ps,
and docker system df, compressed as gzip. The export drains streams while retaining at
most the remaining raw allowance; raw + gzip overhead is bounded below the remaining
50MiB per-run budget. A footer reports clipping. Existing exports are included in the budget.

**No host file/directory deletion, truncation, or overwriting existing author files.**
All host writes are under `crates/skedaddle/artifacts/`. Every per-run `temp/` is retained
and its full path printed. There is no enabled host-temp cleanup path. An earlier
`images.lock` was preserved by renaming it to `images.prior-20261106.lock` before creating
the new manifest; `bootstrap.py` and the old verification directory remain untouched.

GA as of 2026-11-06: PG18. PG19 is RC1, planned GA 2026-11-19:
https://www.postgresql.org/about/news/postgresql-19-rc-1-released-3409/
Docker Hub corroboration: https://hub.docker.com/_/postgres (19rc1-alpine).
PgBouncer: maintained minimal Alpine `edoburu/pgbouncer`, release activity at
https://github.com/edoburu/docker-pgbouncer/releases .
MinIO official Hub/Quay repos were unavailable. The manifest pins Thanos's mirror of
MinIO RELEASE.2025-09-07T16-13-09Z, **not** an upstream-maintained image:
https://quay.io/repository/thanos/minio?tab=tags . All runtime images are digest-pinned.

`resolve.py` records pull/inspect evidence; `verify-main.py` runs PG16/17/18 and the
large run; `verify-paths.py` exercises failures/signals/hard-kill/refusal/noninterference.
Verification drivers use fixed evidence names and must be given fresh names before rerunning.
