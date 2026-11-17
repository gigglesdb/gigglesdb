# Long-running agent harness

Run `bash harness/run.sh agent --work-root DIR --nunya-exe PATH` from
`crates/nunya`. Use `NUNYA_AGENT_MINUTES=3` for the short acceptance run;
the default is 15 minutes. The run requires Docker, Git Bash on Windows,
Go 1.24.5, and Python with psycopg 3.3.9. `NUNYA_PYTHON` overrides the default `python` command.

Each route acquires one session, then reconnects fresh clients after gaps longer
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
