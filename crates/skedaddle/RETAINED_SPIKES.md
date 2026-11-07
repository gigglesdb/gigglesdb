# Retained GD-0049 spikes

These are historical, reviewed P0 research inputs, not the shipped migration
implementation. Their original reports and qualification caveats remain beside
the source. Production P2 uses the crate-level `harness/`.

Retained source covers the PG replication decoder and TLS/SCRAM probe (S1),
fencing and pool tests (S2), the DDL decoder/start-marker experiment (S3/S3b),
run-scoped Docker hygiene and image pins (S4), object-store qualification notes
(S5), real-app workload/oracle code (S6), and network fault tooling (S7).
S2 derives its source from S1; S3b derives from S3. Those source dependencies and
standalone Cargo manifests/locks are retained together.

Legacy drivers contain historical K: output paths and assumptions. Do not run
them unchanged under the current J:-only output policy. Set CARGO_TARGET_DIR,
CARGO_HOME, TEMP, TMP and fixture paths to a unique J:/gigglesdb-work task root
for source qualification. Port old driver output/executable paths to that root
before rerunning historical service campaigns. Current Docker checks run via
`harness/run.ps1 p2`, with its disk guard and labeled-resource cleanup.

This retention increment compiles/tests all four retained Rust probe crates and
syntax-checks the Python sources in a git-archive overlay on J:. It does not
claim a new execution of every historical Docker campaign.

Excluded deliberately: target directories, Cargo homes and executables, copied
vendor crates, compile surveys and intentionally throwaway mutation probes,
raw run logs/databases, fixed-run report generators, old image locks, and bulk
Miniflux migration snapshots. The DDL probe's required small upgrade SQL fixture
is retained with its Apache-2.0 license and provenance under `licenses/`.
The S6 source uses the separately licensed upstream application image.
The obsolete S5 patched-vendor Cargo project is excluded; only its small
qualification record, image references and caveated notes are kept. P2 uses
upstream object_store rather than that patched experiment.

The broad artifacts ignore rule intentionally keeps new run output untracked.
The curated source files are tracked explicitly. No files were deleted during
curation; excluded historical material remains in the working tree for manual
review/removal.
