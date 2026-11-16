# Real MySQL binlog decoder fixtures

These are fresh captures from the repository's `harness/mysql-capture.py`, run
through `harness/run.ps1`. No database behaviour is mocked. Each variant has 12
event types and 13 individual captures: QUERY has both BEGIN and DDL examples.

| Directory | Server | GTID mode | Official image digest |
| --- | --- | --- | --- |
| `mysql80-gtid-on` | MySQL 8.0.46 | ON | `mysql:8.0@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b` |
| `mysql80-gtid-off` | MySQL 8.0.46 | OFF | Same 8.0 digest |
| `mysql84-gtid-on` | MySQL 8.4.13 | ON | `mysql:8.4@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902` |
| `mysql84-gtid-off` | MySQL 8.4.13 | OFF | Same 8.4 digest |

`metadata.txt` records the observed server version, image reference and local
image ID, UUID, GTID state, binlog status, ROW/FULL/FULL settings, and the source
row's NULL count and 20,480-byte wide value. Image references are pinned in
`harness/images.lock`; capture timestamps, UUIDs, GTIDs and TLS cipher details
can differ on regeneration.

Each directory contains only test data:

- `FORMAT_DESCRIPTION_EVENT.bin`, `PREVIOUS_GTIDS_EVENT.bin`, and either
  `GTID_EVENT.bin` or `ANONYMOUS_GTID_EVENT.bin`: stream and transaction metadata.
- `QUERY_EVENT.bin`: BEGIN; `QUERY_DDL_EVENT.bin`: CREATE TABLE. Both manifest
  entries have type `QUERY_EVENT`; the optional `capture` field names the file.
- `TABLE_MAP_EVENT.bin`, `WRITE_ROWS_EVENT.bin`, `UPDATE_ROWS_EVENT.bin`, and
  `DELETE_ROWS_EVENT.bin`: the first table map and row events belong to the
  30-column `type_zoo.zoo` from `harness/mysql-init.sql`. They retain NULLs,
  unsigned limits, precise decimals, fractional times, UTF-8, binary values,
  ENUM/SET, JSON and the 20,480-byte value. Rows and column metadata are FULL.
- `XID_EVENT.bin`, `ROTATE_EVENT.bin`, `TRANSACTION_PAYLOAD_EVENT.bin`: commit,
  real file rotation, and a compressed transaction containing zoo writes/deletes.
- `HEARTBEAT_EVENT.bin`: a real replication-wire event captured after rotation
  over CA- and hostname-verified TLS; heartbeats are absent from disk binlogs.
- `mysql-bin.*.binlog`: the complete isolated workload binlog, including magic,
  every event header and CRC32. Initialization traffic is in earlier files.
- `manifest.json`: event type, capture filename (defaults to `<type>.bin`),
  source binlog basename, byte offset, size and SHA-256. The heartbeat uses
  `file: "replication-wire"` and `offset: null`.
- `heartbeat-tls.json`: negotiated TLS version/cipher, verified hostname and
  dump starting position; contains no certificate or key.
- `workload.sql`: exact DDL, uncompressed multi-statement transaction and
  compressed transaction run after the type-zoo initialization.

Regenerate from the repository root in PowerShell with Docker running. These
commands start one labelled, bounded, ephemeral MySQL service at a time, capture
into a unique J: task directory, and copy only verified test data into the repo.
The harness tears down its own Compose resources even if capture fails.

```powershell
$task = 'J:/gigglesdb-work/m2-fixtures-' + (Get-Date -Format yyyyMMdd-HHmmss)
New-Item -ItemType Directory -Path "$task/tmp" -Force | Out-Null
$env:TEMP = "$task/tmp"; $env:TMP = $env:TEMP
$env:CARGO_TARGET_DIR = "$task/target"
$env:SKEDADDLE_TEST_ROOT = "$task/scratch"
$producer = (Resolve-Path crates/skedaddle/harness/mysql-capture.cmd).Path
Get-PSDrive C,J | Select-Object Name,Free
foreach ($profile in @('mysql80','mysql84')) {
    foreach ($mode in @('ON','OFF')) {
        $variant = "$profile-gtid-$($mode.ToLower())"
        powershell -NoProfile -File crates/skedaddle/harness/run.ps1 -Phase $profile -MysqlGtid $mode -TaskRoot "$task/$variant" -MysqlSpike $producer
        if ($LASTEXITCODE -ne 0) { throw "Capture failed: $variant" }
        $destination = "crates/skedaddle/tests/fixtures/mysql/$variant"
        New-Item -ItemType Directory -Path $destination -Force | Out-Null
        Copy-Item -Path "$task/$variant/disk-fixtures/*" -Destination $destination
    }
}
python -I -X "pycache_prefix=$task/pycache" crates/skedaddle/harness/mysql-fixtures-verify.py crates/skedaddle/tests/fixtures/mysql
if ($LASTEXITCODE -ne 0) { throw 'Fixture integrity failed' }
powershell -NoProfile -File crates/skedaddle/harness/cleanup.ps1 -Check -TaskRoot "$task/cleanup-final"
if ($LASTEXITCODE -ne 0) { throw 'Labelled resources remain; preserve other tasks resources' }
```

The executable wrapper accepts `--output <directory>` when invoked directly
with the harness's `M1_*` environment. Output must be a fresh directory under
`J:/gigglesdb-work/`. Normal harness invocation chooses
`<TaskRoot>/disk-fixtures`. Python uses only the standard library; no Cargo
commands are needed.

To verify existing fixtures offline, set TEMP/TMP to a J: task directory, then
run the Python verification command above. It checks all four variants, every
manifest entry's size/SHA-256/CRC32 and exact source slice at an event boundary,
every workload event's framing/CRC32, the required type sets, BEGIN/DDL coverage
and TLS metadata. Expected result: **4 variants, 52 captures, no failures**.
