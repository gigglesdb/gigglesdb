## Quick start (PostgreSQL clients)

From the repository root, start a local development server:

```powershell
./scripts/dev.ps1 -Task dev-server -Port 5432
```

The script stores the database and build outputs under `J:/gigglesdb-work/ide`;
use `-Work J:/gigglesdb-work/MY-SESSION` for a separate instance and `-Port` to
choose its port. Connect your PostgreSQL client to `127.0.0.1:5432`, user
`postgres`, database `postgres`, with TLS disabled. Development mode binds to
`127.0.0.1` and defaults to SCRAM-SHA-256 password authentication.

On first start, the server generates a random 24-character owner password and
prints it once in a connection URL. Save that password: restarting with the same
data directory keeps it unchanged and does not print it again. Set
`GIGGLES_OWNER_PASSWORD` to a nonempty value before first start to choose the
password instead. Only the SCRAM verifier is stored, not the plaintext password.

For Docker's PostgreSQL client, replace `PASSWORD` with the owner password:

```powershell
docker run --rm postgres:18.6 psql "postgres://postgres:PASSWORD@host.docker.internal:5432/postgres"
```

The binary also accepts `giggles-server --dev --data DIR --port 5432` (default
data directory `giggles-data`). `DIR` is the containing data directory; the
database is at `DIR/db`. Development mode disables TLS. The agent mTLS listener
is available only through the `giggles-server CONFIG.json` configuration
workflow below, which also supports TLS and scoped identities.

Build prerequisites: Rust/Cargo and libclang/LLVM. On Windows, set
`LIBCLANG_PATH` to the directory containing `libclang.dll` before running
Cargo or `scripts/dev.ps1` / `scripts/dev.sh`. On macOS, install Xcode Command
Line Tools or `brew install llvm`; set `LIBCLANG_PATH` to its `lib` directory
if needed. The scripts inherit this environment without machine-specific paths.

`cargo build --bin giggles-server` enables RocksDB by default, and
`giggles-server --dev --data DIR` creates v2 databases. Use `--engine legacy`
to create legacy databases. For a legacy-only build without LLVM, use
`cargo build --bin giggles-server --no-default-features`; its creation default
is legacy and `--engine v2` returns an unsupported-engine error.
`--engine` applies only to creation: opening an existing directory always
detects its format. Upgrade is explicit with `giggles upgrade <path>` (the
core CLI); neither binary upgrades on open. V2 supports SQL and catalog roles;
native task operations, receipts, and export still return an unsupported-feature
error.

To reset the owner password offline, stop the server first, then run:

```powershell
giggles-server --dev --data DIR --reset-owner-password
```

Use the same `DIR` as the stopped server, and add `--owner NAME` if its owner is
not `postgres`. Reset requires an existing database and owner. It uses a nonempty
`GIGGLES_OWNER_PASSWORD` if set, otherwise generates a new random 24-character
password, and prints the new password in a connection URL. Restart the server
afterward.

`--trust-local` explicitly opts into connections without password authentication.

WARNING: --trust-local lets any local process connect as any role without a password, including Docker containers (they arrive via 127.0.0.1) and port-forwarded peers. Use only on a single-user machine.

# Single-host service preview (P2)

Rust server and native client over the existing giggles-core/redb database. This
is local implementation evidence, not production, cloud, ORM or general
PostgreSQL compatibility qualification. Designs: [GD-0007](https://app.archdocs.io/design-docs/bcc8c9b3-54d8-4f78-aeba-e64771af999c)
and [GD-0008](https://app.archdocs.io/design-docs/ea3a9a40-18d9-48a1-aa6b-67e2f19f2847).
See [Design documentation](https://app.archdocs.io/design-docs/5850ae99-0d51-403f-9201-d9735687a000), [COMPATIBILITY.md](COMPATIBILITY.md) and
[SECURITY.md](SECURITY.md).

## Run using generated local fixtures

From this crate directory:

```powershell
cargo test --test network executable_server_starts_and_native_client_connects -- --nocapture
```

The output names a unique retained `artifacts/network-fixture-*` directory. It
contains `db.redb`, `server.TEST-ONLY.json`, generated CA/client/server PEMs and
private keys named `*.key.TEST-ONLY`. These are disposable local demonstration
identities, never deployment credentials. Keep the artifact directory; no cleanup
is required. Tests start real TLS sockets and terminate their own subprocesses.
Paths in the fixture configuration are relative to this crate directory.

With the test finished, use its printed directory in these commands:

```powershell
cargo run --bin giggles-server -- artifacts/NETWORK-FIXTURE/server.TEST-ONLY.json
```

The fixture configuration binds native to `127.0.0.1:7432` and pgwire to
`127.0.0.1:7433`. Startup prints actual addresses. If those ports are occupied,
edit a copy of the configuration to unused loopback ports. The database must
already exist. The server never creates or silently replaces a missing database.
Ctrl-C stops listeners and drops staged transactions. A synchronous core decision
already started may finish before shutdown.

Create a local request file containing `{"kind":"Ping"}` and run:

```powershell
cargo run --bin giggles-client -- 127.0.0.1:7432 localhost artifacts/NETWORK-FIXTURE/app.pem artifacts/NETWORK-FIXTURE/app.key.TEST-ONLY artifacts/NETWORK-FIXTURE/ca.pem REQUEST.json
```

Native protocol: one big-endian u32 payload length followed by UTF8 JSON. Responses
are framed the same way. Request examples:

```json
{"kind":"Sql","sql":"INSERT INTO app (id,body) VALUES ($1,$2)","params":[{"Int":9},{"Text":"hello"}]}
```

```json
{"kind":"Status","operation":700}
```

```json
{"kind":"RepairSql","operation":701,"sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 7","approval":null}
```

Finite repair SQL requires whitespace around operators; it is the core's existing
repair parser, not generic application SQL. Preview and Execute use the existing
typed repair Request. Approval IDs come from a separate trusted local approver.
Remote Preview returns only count/digest; an approver independently obtains the
exact full preview through trusted local APIs. Network clients cannot approve.

An optional `{"kind":"Deadline","budget_ms":0,"request":{...}}` supplies a
cooperative decision budget (maximum10000ms) from complete-frame receipt. Frame
reading retains its separate10s timeout. One envelope only. Expiry before the
atomic business decision returns57014 without that invocation publishing business
rows, approvals, budgets or receipts. Checks include task/core lock waits and the
final redb commit fence. Native decode still awaits blocking admission; the budget
includes that wait and is checked once decoded, without a queue latency promise.
After decision wins, expiry cannot replace success or ambiguous storage errors.
No unsafe interruption or automatic retry is used. Task OperationId identifies the
original durable operation, not the current invocation:57014 on a retry never
proves an earlier attempt rolled back; reconcile its status.

PostgreSQL BackendKeyData is generated with OS crypto randomness. Use the pinned
driver's `cancel_token().cancel_query(...)` with the same mTLS client certificate
and required TLS configuration. Cancel connections must negotiate SSLRequest then
mTLS, like normal sessions. Wrong/stale keys and different certificates are silent
no-ops; idle cancellation is not carried into the next request. PostgreSQL's wire
packet has no per-query generation: a delayed packet can target a later active
query on the same live backend. Keys are invalidated when the socket task ends;
backend IDs are never reused within one Service lifetime. The historical ID set
is capped at65536 registered sessions; exhaustion requires a Service restart.
Socket disconnect/shutdown alone does not cancel admitted work.

To use pgwire, use `tokio-postgres = "=0.7.15"` with
`tokio-postgres-rustls = "0.13"`, require TLS, load the generated CA and app client
certificate/private key, use TLS hostname `localhost`, startup user/database
`giggles`, and port7433. These startup names never select authority; the verified
leaf fingerprint does. An executable integration example is in `tests/network.rs`
(`Fixture::pg`). No password is needed or accepted. Server version 18.0 is a wire
parameter for driver negotiation, not a PostgreSQL implementation claim.

For DataGrip, select PostgreSQL, host `localhost`, port `7433`, user `giggles`,
database `giggles`, and no password. Check the effective URL as well:
`jdbc:postgresql://localhost:7433/giggles`. Use full SSL verification with the
fixture `ca.pem`, `app.pem`, and `app.key.TEST-ONLY`. Identity functions and bounded
PostgreSQL catalog queries now expose real, authorized schemas/tables/columns,
primary keys and types. Full DataGrip GUI introspection remains unqualified.
Actual pgJDBC 42.7.3 probes exercise default initialization, prepared queries,
`Connection.isValid()`, rollback, boolean bindings, statement reuse across commits,
and queries after eleven seconds idle. This is a bounded query surface, not full
DataGrip support. See [the compatibility matrix](COMPATIBILITY.md) before choosing
a client, including JDBC cancellation and statement-cache limits.

The startup profile admits bounded `DateStyle=ISO`, timezone labels (128 bytes),
and `extra_float_digits` from -15 through 3. These presentation hints do not affect
the trusted clock or authorization, and the current SQL type set has no temporal
or floating-point types. `application_name` is capped at 128 UTF-8 bytes.
`options`, `replication`, and `search_path` remain unsupported.

JDBC initialization supports exactly `SET application_name` and
`SET extra_float_digits`, using `=` or `TO`; simple-query batches may contain up
to 16 of these statements. Prepared SET accepts one statement and no parameters.
Quoted application names use doubled single quotes. Settings follow transaction
commit/rollback, reject in aborted transactions, and are unavailable to task
roles. This is not a generic SET implementation. Empty extended queries support
driver validation. Application parameters accept varchar wire values for TEXT
and integer wire widths with engine destination-range checks; task parameter OIDs
remain exact.

Ordinary extended application SQL is staged until Sync; an error in the cycle
rolls it back. Flush does not commit. Explicit transactions remain open across
Sync, and COMMIT of an aborted transaction performs a rollback. Named statements
survive transaction boundaries, while portals close at transaction end. Task
operations retain their own durable decision/status contract.

Startup rejection messages distinguish an incorrect/missing user or database
(`28000`), unsupported startup fields or encoding (`0A000`), and malformed startup
packets (`08P01`). The console writes fixed rejection reasons to stderr, plus
transport stages for SSL negotiation, TLS handshake, certificate binding, and
wire I/O errors. PostgreSQL query rejections include SQLSTATE, a static phase,
and a SHA256 fingerprint of the exact SQL bytes. Prepared execution retains the
original query fingerprint; each new message resets its diagnostic context. Diagnostics
never include submitted startup values, SQL, certificate bytes, or key material.
After updating the binary, restart the server to get these diagnostics.

Task wire uses simple-query `GIGGLES ` followed by exactly one native JSON request
for preview/execute/repair/status (optionally Deadline). It returns one TEXT JSON
column named `decision`. The same structured code/count/digest/operation response
is returned natively. Task SQL never obtains a generic SqlSession.

Prepared task requests use the finite typed grammar documented in
[PREPARED_TASKS.md](PREPARED_TASKS.md). Parse, Describe and Bind return static
metadata or validate typed values; only Execute enters task authority. Repeated
Execute on the same portal checks current authority and returns a fresh redacted
decision. Durable operation IDs deduplicate mutations across native, simple and
extended requests. See [GD-0011](https://app.archdocs.io/design-docs/4a6a02da-d8a0-4f41-8141-7c26a5e6c313).

## Verification and retained evidence

`./verify.ps1 -ArtifactDirectory artifacts/UNIQUE-NAME` runs fmt, warning-free
Clippy, all debug/release targets and doctests, retaining commands, stdout/stderr
and SHA256 input manifests. Use a fresh artifact directory each time. Tests retain
their databases, generated test-only certificates, seeded histories and malformed
wire traces. `--nocapture` on a targeted run prints fixture paths. Existing core
and FFI verification scripts and actual C clients must also pass after core edits.

Official sources consulted: [PostgreSQL protocol flow](https://www.postgresql.org/docs/17/protocol-flow.html),
[message formats](https://www.postgresql.org/docs/17/protocol-message-formats.html),
[rustls client verification](https://docs.rs/rustls/0.23.46/rustls/server/struct.WebPkiClientVerifier.html),
and the pinned tokio-postgres source for START TRANSACTION and RowDescription.

## Live credential and shared admission control

See [CREDENTIALS_ADMISSION.md](CREDENTIALS_ADMISSION.md) for the bounded trusted
stdin `reload`/`shutdown` workflow, binding epochs, identical-authority certificate
overlap, reloadable certificates and restart-required CA changes, aggregate session/rate defaults,
cancellation saturation behavior and capacity worksheet.

Optional HTTP/1.1, HTTP/2 and typed gRPC interfaces expose native reads, guarded
writes, claims and atomic blocks as structured requests. Native and SQL calls
share persistent transactions; typed controls let native clients begin, commit
and roll back without SQL strings. Existing scoped task operations remain
available. See [API.md](API.md) for configuration, schemas, limits and runnable
clients. Both listeners are disabled unless configured and reuse the existing
mTLS bindings and shared service admission.
See [Design documentation](https://app.archdocs.io/design-docs/5850ae99-0d51-403f-9201-d9735687a000) for the actual parent P2 row.

### Rotating credentials

Replace the PEM certificate and matching private key at the configured paths,
then send `reload` on the server's trusted local stdin (the binding reload
command). This also reloads the optional `postgresql` listener's certificate
from its own configured paths. New TLS handshakes use the replacement;
established connections continue. Invalid PEM or a mismatched key logs an
error and retains that listener's prior certificate. CA changes require restart.
Certificate reload works even without `bindings_file`; bindings then remain unchanged.

For SCRAM, connect as the role or an authorized administrator and run
`ALTER ROLE role_name PASSWORD 'new-password'`. New connections must use the
new password; already authenticated sessions continue. Role privilege or login
changes still invalidate sessions.
