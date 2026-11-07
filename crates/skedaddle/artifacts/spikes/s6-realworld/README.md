# S6 / GD-0049 real-world harness selection

All host files are retained. No commits, pushes, Archdocs, or Linear writes.
The adapter imports S4's PG18 service, guard, export, down, and scoped label
teardown. It never calls sweep, cleanup, prune, or a file deletion operation.
Every service/volume/network uses `skedaddle.test=<run-id>`; Compose project
is `skedaddle-<run-id>`. Published ports are ephemeral and loopback-only.

Run from `K:\Projects\gigglesdb\crates\skedaddle`, using a fresh run-id:

```powershell
python -B artifacts\spikes\s6-realworld\prepare.py <fresh-run-id>
python -B artifacts\spikes\s6-realworld\measure_images.py <fresh-run-id>
python -B artifacts\spikes\s6-realworld\research_finish.py <fresh-run-id>
python -B artifacts\spikes\s6-realworld\run.py <fresh-run-id> 180
```

`prepare.py` only creates evidence under `artifacts/s6-<run-id>/`, resolves
linux/amd64 manifests, retains official source snapshots, and pulls four
digest references. The chosen images total well below the 6 GB allowance.
It does not fetch dataset data. S4's runtime disk guard executes before up.
The data footprint is small; the runner checks each populated database and
the upgraded database against 1,000,000,000 bytes. This is a bounded spike,
not a general concurrent database quota mechanism.

`run.py` starts Miniflux 2.2.13, Gitea 1.24.6 and Keycloak 26.7.5 on S4 PG18
using separate databases. Gitea's Git directory is an explicitly labeled
named volume; no image-declared anonymous volumes are left. A BusyBox HTTP
fixture serves three synthetic RSS entries, so external feed content and
downloads do not introduce nondeterminism. App API workloads create/update
categories, feeds, entries, users, a repository, issues, comments and a realm;
user/feed/category deletes exercise database cascades. No Git repository
deletion is requested. Miniflux runs for at least 180 seconds; the other
two apps receive bounded API smoke workloads. All three initialize their
real schemas/migrations and background services. Only Miniflux's populated
version upgrade is actually tested.

Every workload response is appended and fsynced to `history.jsonl` before
the driver validates success. Sequence, UTC timestamp, app/action, HTTP
method/path/status, entity IDs, payload, response and Location are retained;
passwords and tokens are redacted. Bootstrap/readiness requests are outside
this workload history and separately evidenced. The source now additionally
copies Location's final ID into `ids.location_id`; run b predates that small
metadata convenience, but already retains each full Location.

`oracle.py` independently replays the acknowledged history into expected
row projections. It checks every tracked entity, including feed-entry
cascades and deleted user/category absence, using read-only SQL, before
and after upgrade. Feed existence/category association is checked, while
its editable title is excluded because asynchronous refresh may change it.
Entry read status/bookmark, category title, username, issue title/body,
comment body, repository name and Keycloak username/first name are checked.
This is an application-projection oracle, not exhaustive database equality
or a replication correctness proof. App-owned incidental rows are excluded.

Upgrade uses separate old/new labeled services: stop old, start new with
`RUN_MIGRATIONS=1`, wait for HTTP health, check `schema_version` changed,
then replay the same oracle. No implicit Compose replacement removes the
old app container. The final S4 down handles Compose resources, then its
exact-run label fallback handles disabled-profile services. Both attempts
retain pre-down resource IDs and post-down empty inventory evidence.

Image sizes: registry layer sums are compressed download bytes.
`measure_images.py` streams `docker image save` through tar/gzip in memory,
counts uncompressed layer tar bytes, and deduplicates shared layer digests.
Docker's containerd `image inspect Size` is compressed on this machine;
the initial `images.lock.json` field named `uncompressed_bytes` is therefore
misnamed. Use `images-measured.json` for true uncompressed measurements.
No archive is written, and no image is removed. Physical VHDX deltas cannot
be attributed to this spike while other spikes run concurrently.

Dataset choices are metadata-only: Chinook PostgreSQL SQL, Pagila schema
plus COPY data, and Monaco OSM PBF. Source licence texts and exact Git tree
byte sizes are in the evidence. Estimated database budgets are 50 MB,
100 MB and 100 MB respectively; imported sizes were not measured. A future
importer must enforce the 1 GB hard cap. Pagila's current schema includes
UUIDv7, composite keys, arrays, full-text types and optional temporal/JSONB
extensions. Monaco's `(element_type, osm_id)` composite identity and text
tag keys complement these. No paid or noncommercial-only dataset is chosen.

Fresh-run caveats: failed run a intentionally remains as evidence of the
HTTP 400 invalid-theme rejection; the driver now uses a username update.
The corrected successful run b has no dependency on run a's Docker resources.
All source snapshots are retained; an initial Pagila LICENSE URL 404 is
recorded, and the correct pinned `LICENSE.txt` was fetched afterward.
