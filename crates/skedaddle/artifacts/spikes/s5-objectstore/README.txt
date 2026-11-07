GD-0049 / S5 object-store conditional-write qualification

This is an isolated Rust probe, not product implementation. No commits or pushes.
S4's images.lock and harness source are read-only. No Archdocs/Linear writes.
All GigglesDB architecture documents, if ever written, must use and verify
spaceId 5714972c-8f62-4f17-8ebc-b62fbf4420b9; no documents were created here.

Run from K:/Projects/gigglesdb/crates/skedaddle. Set:
CARGO_BUILD_JOBS=2
CARGO_TARGET_DIR=K:/Projects/gigglesdb/crates/skedaddle/target/s5
CARGO_HOME=K:/Projects/gigglesdb/crates/skedaddle/artifacts/spikes/s1-replication/cargo-home
TEMP and TMP to the evidence directory's temp subdirectory.
cargo build --manifest-path artifacts/spikes/s5-objectstore/Cargo.toml --locked
python -B artifacts/spikes/s5-objectstore/resolve.py <fresh-resolution-run-id>
python -B artifacts/spikes/s5-objectstore/run.py <fresh-run-id>
python -B artifacts/spikes/s5-objectstore/seaweed-followup.py <same-run-id>
python -B artifacts/spikes/s5-objectstore/seaweed-followup.py <same-run-id> seaweed-parallel
python -B artifacts/spikes/s5-objectstore/gcs-followup.py <fresh-gcs-run-id>
The followup requires the first driver's Docker teardown to have finished.

resolve.py creates its evidence directory; run.py expects an existing evidence
directory. All runtime images use digests in compose.yaml. resolve.py records
upstream metadata and the pull errors for official community MinIO repositories.
It also resolves AIStor and the upstream-documented SeaweedFS image. AIStor is
available but denies bucket creation without a license. No license was obtained.

Probe CLI: target/s5/debug/s5-objectstore.exe BACKEND ENDPOINT EVIDENCE_DIR
BACKEND: seaweed, thanos, gcs, local. Each race uses a fresh key and a Tokio
barrier, 200 trials per N=2 and N=16, per supported create/update mode. Each
writer uses unique bytes; each successful PUT immediately checks GET bytes and
LIST of the parent directory containing the exact key. Seed writes and duplicate
rejection are checked too. S3 uses ETagMatch; GCS uses the returned generation
in UpdateVersion.version. Duplicate errors retain full Debug output in JSON.
S5_BOUNDED=1 uses zero retries and a 2-second HTTP timeout for S3. The bounded
binary contains the same source, with this setting controlled by the environment.
S5_PARALLEL_TRIALS=4 starts at most four independent-key trials concurrently;
each key still has exactly N=2 or N=16 writers starting at its own barrier.
The final Seaweed run uses these settings after an HTTP/lock-ring readiness wait.
Earlier sequential runs were interrupted for slow N16 progress and retained.

fake-gcs-server 1.56.1 rejects object_store's plain XML PUT with HTTP 400
invalid uploadType. gcs-followup.py seeds an existing object through the supported
JSON API solely as a fixture, then repeats the UNMODIFIED object_store XML PUT
path: fresh keys for create races; the unchanged existing generation for update
races. Every attempt fails (zero successes), so the generation stays current.
This is a protocol-compatibility failure, not evidence of a lost-update race.
Read-after-success is blocked because there are no successful object_store PUTs.

NO HOST FILE DELETION: vendor/object_store is copied from crates.io 0.14.2;
the ONLY source changes are omission of TWO staging-file removal statements in
src/local.rs (put_opts error cleanup and finish_staged_hard_link success cleanup).
The hard-link operation, error mapping, payload write and list/get code are stock.
Create and rejected Update are the only local write modes exercised. Every
staging file/hard link is intentionally retained. Local results therefore qualify
the stock atomic-create mechanism with cleanup disabled, not an unmodified crate.
No delete, overwrite, multipart or rename API is exercised by the local probe.

The drivers call S4 export/down/check(RUN), never sweep or unscoped cleanup.
Only containers, volumes and networks with this run's exact label are removed.
No image removal, prune, host file removal or Docker/WSL maintenance is performed.

The first pilot evidence is retained under artifacts/s5-s5-20261106-obj-1452.
Its full-key LIST prefix was incorrect; its GCS builder also attempted ambient
ADC/token initialization. The final probe uses parent LIST prefixes and explicit
dummy disable_oauth service credentials, preventing ambient credential reads.
The first Seaweed startup flag was corrected to master.volumeSizeLimitMB.
Final qualification excludes pilot observations.
