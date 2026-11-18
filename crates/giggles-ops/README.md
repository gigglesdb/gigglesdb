# giggles-ops

A full snapshot backup catalog and fresh-identity restore library and CLI for local operators.

**Status:** Uses verified core exports; incremental backup, PITR and automatic retention deletion are unsupported.

Part of [GigglesDB](../../README.md).

Local full snapshot backup catalog and restore into a fresh database identity.
This standalone crate uses the existing `giggles-core` verified consistent export
API. It never copies an open `data.redb` file. The library accepts an existing
`Admin` for online exports. The CLI opens an existing local database exclusively;
close other processes holding that database before using CLI backup.

From this directory in PowerShell:

```powershell
$env:CARGO_TARGET_DIR = "$PWD/target-ops"
$env:CARGO_BUILD_JOBS = '2'
cargo build --locked -j2
New-Item -ItemType Directory ./backup-catalog
cargo run --locked -- backup --root ./backup-catalog --source ./data/source-db --id incident-001
cargo run --locked -- list --root ./backup-catalog
cargo run --locked -- inspect --root ./backup-catalog --id incident-001
cargo run --locked -- verify --root ./backup-catalog --id incident-001
cargo run --locked -- restore --root ./backup-catalog --id incident-001 --target ./data/recovered-db
```

`./data` must already exist; `recovered-db` must not exist, even as an empty
directory. The source database must already exist. All commands emit one JSON
value on stdout on success, exit 0; errors emit only `{"error":"code"}` on stderr
and exit 1. No paths, SQL, credentials or customer row values appear in errors.
Unknown/duplicate/missing flags reject. IDs are 1..64 ASCII alphanumeric/hyphen
characters, begin with alphanumeric, and exclude Windows device names.

Expected observations:

| Command | Successful JSON observation |
| --- | --- |
| backup | `format_version:1`, `state:"complete"`, immutable `backup_id`, `source_domain`, exact `content_length` and `sha256` |
| list | Array of `state:"complete"` manifests and `state:"incomplete"` retained staging directories |
| inspect | Parsed validated manifest metadata; contents have not been verified by this command |
| verify | `manifest` plus protected `rows`, `tasks`, `approvals`, `operations` counts; both outer integrity and core envelope validated |
| restore | `format_version:1`, original/new domain IDs, counts, `authorities_revoked:true`, `reopened:true` |

Counts refer to core protected task data and decisions. Relational tables are
included in the snapshot but their customer values and per-table counts are not
included in operator reports. Restored schema and typed values can be queried via
the core SQL API. `ops-restore-report.json` in the new database stores the same safe
restore summary with a durable file flush.

Backup creates a random `staging-<32hex>` directory with `incomplete.json`, then
exports and flushes `snapshot.gdb`, verifies its whole-file SHA256 and length and
the core canonical envelope, writes/flushed `manifest.json`, and finally publishes
`<id>.json` using atomic no-clobber hard-link creation. Snapshot names in manifests
must be exactly `staging-<32hex>/snapshot.gdb`; absolute paths, ADS, separators and
traversal outside that grammar reject. Whole-file and embedded checksums are
integrity checks, not signatures or authentication. Backups contain sensitive
data: secure the storage independently.

Every stage remains on success and failure. Its incomplete marker is never
changed, and listing always labels staging itself incomplete; a separate published
manifest is the sole completion record for a backup. Core also retains its export
staging file. Preserve the whole catalog, including stage directories, when moving
it. A standalone published JSON file is insufficient. Inspect incomplete stages
for diagnosis, and retry with a fresh ID when appropriate. This crate performs no
retention deletion, automatic cleanup or rollback of failed installs.

The retained `.ops-admission` regular file provides a nonblocking cross-process
writer lock. A concurrent writer may receive `busy`; retry after the current call
returns. A crash releases its OS lock while retaining the file. Backup reserves
room for stage and manifest before export. Defaults cap the entire root scan at
1024 entries, JSON at 16 KiB, exports at 16 MiB + 52 bytes. The library can lower
these caps with `OpsLimits`; it cannot raise them. Per-call IO is bounded and
synchronous. A full catalog returns `limit`; allocate another catalog root rather
than hiding incomplete records. `list` and `inspect` are metadata operations;
always `verify` before relying on the artifact. Restore verifies automatically.

Duplicate backup IDs and every existing restore target reject without changing
them. An error after publication can leave a complete, verifiable backup. Restore
crashes before the core install commit leave an existing, unusable target; crashes
after commit can leave a valid fresh database without an ops report. Reopen and
inspect independently; absence of a report means no ops acknowledgement. Retry
into a different nonexistent target. Never overwrite a live database to repair it.

Restore preserves operation decisions/receipts and relational schema/data while
core creates a fresh domain, records origin, and revokes tasks and approvals.
An exact decided-operation replay may return the original old-domain receipt
without new effects. New protected operations reject revoked authority. Preserved
operation history is not reissued authority. Reissue grants deliberately and
reconcile external effects independently before promoting recovered data. This
crate does not provide a dedicated ledger engine or financial reconciliation.

Filesystem assumptions: an operator controls source/root/target parents; existing
ancestors and final managed files reject symbolic links and Windows reparse
points/junctions. Windows managed reads deny sharing and open reparse points
explicitly; Unix final reads use `O_NOFOLLOW`. This is not a security boundary
against a privileged host replacing ancestors concurrently. Catalog contents
must be immutable: do not modify published snapshots during calls.
Full checksums are rechecked after verification and before restore acknowledgment
to detect persistent source replacement, including SQL-only changes. These checks
do not defend against a privileged writer deliberately swapping bytes back and
forth between reads. Hard links must work
on the local filesystem. File contents are synced; parent-directory entry
power-loss durability remains unqualified, as in the core export contract.

Full snapshots only. No continuous archive, PITR, incremental backup, encryption,
signatures, KMS, cloud deployment, migration, RPO/RTO, throughput, financial,
security or production qualification is claimed. `ops-fault-driver` is an explicit
argument-driven recovery test utility; the production CLI has no fault flags or
environment-activated faults.

Run `./verify.ps1` for fmt, strict all-target Clippy, debug/release tests, doc tests
and warning-free docs. Logs and dependency/source hashes are retained under unique
ignored `artifacts` paths. Test fixtures use unique OS temporary directories and
are deliberately retained, including failed/crashed cases.

## Usage from sh

From the repository root:

```sh
cargo run --manifest-path crates/giggles-ops/Cargo.toml -- list --root ./backup-catalog
```
