# Live policy agent

From `crates/nunya`, run:

```sh
NUNYA_PYTHON=J:/gigglesdb-work/nunya/gd88/live-3e/venv/Scripts/python.exe \
NUNYA_AGENT_JARS=J:/gigglesdb-work/nunya/gd88/live-3e/jars \
bash harness/run.sh policy-agent \
  --work-root J:/gigglesdb-work/nunya/gd88/tasks/close-review/run \
  --nunya-exe J:/gigglesdb-work/nunya/gd88/target-w1/debug/nunya.exe
```

Requires Docker with PostgreSQL 18.6 and Spanner emulator 1.5.41 images already
available, Go 1.24.5, Java 21, psycopg and psycopg_pool. Set `NUNYA_AGENT_JARS`
(required) to a directory with the pinned pgJDBC/Hikari jars (see
`harness/drivers/fetch-jdbc.sh`).
On Windows use Git Bash rather than the Windows WSL launcher named bash.

Tokenize and Show run sequentially. Each pass acquires one PostgreSQL and one
native Spanner access session. A psycopg connection and a long-lived Hikari
child exercise PostgreSQL; a persistent Go client exercises native Spanner.
They reconnect within the original access session after stale-schema errors,
then explicitly check reconnect stability. Sessions are revoked at teardown.

The scenario counts migration, JSON whole-document/path, tightening fence,
idempotency, stale revision, loosening and lifecycle checks. Missing checks
fail, including the second mode when an earlier product blocker stops the run.
Stale management revisions use HTTP 409 `stale_revision`, not an NY SQLSTATE.
Private driver protocol carries rows only through parent/child pipes; stdout
prints fixed check names and codes. Each stream fixture has 64 rows and is 8 MB;
the two databases together stay below 50 MB.

Only `gd88-close-tokenize-{pg,emulator}` and `gd88-close-show-{pg,emulator}`
containers are created. Cleanup removes owned names with `docker rm -fv`.
No Kubernetes commands, pruning, volumes or networks are created explicitly.

Close-3 live qualification: 94 passed; 0 failed across Tokenize and Show.
The new generated-domain allocation preserves both original access sessions.
PostgreSQL row-release fencing closes its socket with NY008; clients reconnect
using the same access session before testing the new policy. The loosening
check compiles a fresh query instead of reusing a tightened prepared plan.
Native fencing uses the generated gRPC stream API with static 64 KiB windows:
the high-level Go iterator buffers until resume tokens and can consume the
entire small emulator fixture before returning its first row.
Evidence: J:/gigglesdb-work/nunya/gd88/tasks/close-3/run/final.log.
