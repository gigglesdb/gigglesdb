# Long-running agent harness

Run `bash harness/run.sh agent --work-root DIR --nunya-exe PATH` from
`crates/nunya`. Use `NUNYA_AGENT_MINUTES=6` for the short acceptance run;
the default is 15 minutes. The run requires Docker, Git Bash on Windows,
Go 1.24.5, Java 21, and Python with psycopg 3.3.9. `NUNYA_PYTHON` overrides the default `python` command.

`NUNYA_AGENT_PREFIX` selects container names (default `gd81-d5`); acceptance uses
`gd81-v-agent`. `NUNYA_AGENT_JARS` selects the jar cache, otherwise jars are
fetched into the run directory. The pinned pgJDBC 42.7.14, HikariCP 7.1.0 and
slf4j-api 2.0.17 jars are SHA-256 checked by `drivers/fetch-jdbc.sh`.

The config supplies standing principals `agent` and `agent-standing2`. Each route acquires one session, then reconnects fresh clients after gaps longer
than the 60-second idle timeout: sleep 40 seconds, renew mid-gap, sleep 35 seconds.
Every measured gap must exceed 60 seconds; absolute expiry and public session IDs
must remain unchanged. An unrenewed third session is checked after 65 seconds.
The same state directory is used for a restart in the first mid-gap renewal at or after the
halfway point, renewing after restart within the idle window. A blind token saved before restart must still match afterwards.
Finally each route must admit exactly 20 explicit tokenizations, refuse the
21st with NY007, stop an active result stream with NY011 after revocation,
and refuse fresh clients using the original credentials.

Every missing check counts as a failure. Tool diagnostics and data are not
printed. Secrets, tokens, helper storage, and gateway state are removed at
exit; named containers are removed by Python cleanup and an EXIT trap.
Go caches and binaries stay under the selected work root for inspection
and can be deleted when no run is active. No Cargo build is required.

Never run any `docker ... prune` command (volume, system, image, container, network, builder). Name or label every container, volume and network you create, and remove only those, by name or label (`docker rm -fv <name>`, `docker volume rm <name>`, `docker network rm <name>`). If you pull an image, remove only that image by its exact tag, and only if nothing else uses it. (On 2026-11-16 a prune deleted ~777 volumes that belonged to others.)

One Java child and one HikariDataSource persist through the whole run, with
maximumPoolSize=2, minimumIdle=1, keepaliveTime=0, maxLifetime=0, idleTimeout=0.
No background queries run. Each cycle borrows twice for token read/bind, comparing
only SHA-256 prefixes with Python. Java reports first/last statement timestamps,
statement counters and retries; every between-cycle gap must exceed 60 seconds.
The first post-restart cycle must recover through Hikari's stale-connection eviction.

In the first gap, max_connections changes 256 -> 200 and a PG policy comment
changes its revision. Reload must report applied; session IDs and absolute expiry
stay fixed. The next Python, Go and Java cycles preserve reference tokens, and a
Python connection held since before reload still works. Audit records must show
both policy revision hashes (only eight-character prefixes are printed).
In the next gap, a separate short stage acquires agent-rm for agent-standing2,
removes that config principal and reloads. Its held connection must fail with
SQLSTATE NY011 from psycopg when the next query runs immediately after reload,
on both Windows and Linux. The check reports `held=NY011`. A fresh connection must fail
NY003 and renewal must fail; the original routes keep working.
This is separate from the halfway restart and final revocation stages.

Before the final PG stream revoke, Java leases a connection and holds it through
the revoke. It must then observe SQLSTATE NY011 at the JDBC driver level on that
lease on both Windows and Linux, and NY003 on a fresh physical connection, and exit
zero after quit. The counted additions are jdbc-pool-start,
jdbc-reconnect-token, jdbc-renewal, jdbc-restart, jdbc-revoked, jdbc-exit,
reload-continuity and reload-standing-removed. All original checks remain required.
