# giggles-server

The GigglesDB server exposes SQL over the PostgreSQL wire protocol and optional framed native wire, HTTP and gRPC interfaces. Use it to connect database clients to a configured service.

**Status:** Supports local development and configured services; PostgreSQL compatibility is bounded and listener capabilities differ.

Part of [GigglesDB](../../README.md).

## Quick start (PostgreSQL clients)

From the repository root, start a local development server:

```powershell
./scripts/dev.ps1 -Task dev-server -Port 5432
```

The script stores the database and build outputs in its work directory (set with
`-Work`). Use `-Work <work-dir>` for a separate instance and `-Port` to
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

V2 supports transactional `COMMENT ON TABLE [schema.]t IS 'text'` and
`COMMENT ON COLUMN [schema.]t.c IS 'text'`, with table-owner authorization.
`NULL` and empty strings remove comments. Text is limited to 65,536 UTF-8 bytes;
larger comments return `54000` (`program_limit_exceeded`). PostgreSQL 18.6
accepts larger comments. Legacy databases and other object kinds return `0A000`.
Stored comments are not yet exposed through `pg_description`, `obj_description`
or `col_description`.

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

## Configured mTLS service

Run `giggles-server CONFIG.json`. It opens an existing
embedded database and exposes a bounded PostgreSQL wire interface and an optional
framed native wire listener. These configured mTLS listeners require server and client
certificates; they do not offer password fallback or plaintext connections.

Optional HTTPS/JSON and typed gRPC listeners expose structured native reads,
guarded writes, claims and atomic blocks, alongside parameterized SQL and scoped
task operations. Native and SQL calls share persistent transactions, mTLS
bindings and service limits. See the
[HTTP/gRPC guide](API.md) for configuration and runnable clients.

For this mTLS workflow, use the existing fixture generator, which creates
the database, application table, certificates and matching authorization bindings.
The following commands use **PowerShell**, starting at the repository root.

### 1. Generate a local database and credentials

```powershell
cd crates/giggles-server
cargo test --locked --test network executable_server_starts_and_native_client_connects -- --exact --nocapture
```

This runs a startup smoke test and prints a line like:

```text
retained_fixture=artifacts/network-fixture-12345-...-0
```

Copy that path. The test stops its server before returning and keeps the files.
The fixture contains:

| File | Purpose |
| --- | --- |
| `db.redb` | Persistent database, including an empty application table named `app` |
| `server.TEST-ONLY.json` | Server configuration with fixed local ports |
| `server.pem`, `server.key.TEST-ONLY` | Server certificate and private key |
| `app.pem`, `app.key.TEST-ONLY` | Client identity authorized to query the `app` table |
| `ca.pem` | CA trusted by the client and server |
| `bindings.json` | Generated certificate-to-role mappings |

These generated identities are for local demonstrations. The startup test also
creates an `ephemeral-server.TEST-ONLY.json`; use **`server.TEST-ONLY.json`** for
the fixed-port commands below.

### 2. Run the server

Remain in `crates/giggles-server`. Replace the example path with the printed path:

```powershell
$fixture = 'artifacts/network-fixture-12345-...-0'
cargo run --locked --bin giggles-server -- "$fixture/server.TEST-ONLY.json"
```

Successful startup prints:

```text
wire=127.0.0.1:7432 postgres=127.0.0.1:7433 preview=single-host
```

Leave this terminal running. Type `shutdown` and press Enter, or press Ctrl-C,
to stop. Restart with the same configuration to reopen the same database.

### 3. Connect and run queries

Open a second PowerShell terminal at the repository root, then use the **same**
fixture path:

```powershell
cd crates/giggles-server
$fixture = 'artifacts/network-fixture-12345-...-0'

function Invoke-Giggles {
    param([string]$Json)
    $Json | Set-Content -Encoding ascii "$fixture/request.json"
    cargo run --locked --bin giggles-client -- 127.0.0.1:7432 localhost "$fixture/app.pem" "$fixture/app.key.TEST-ONLY" "$fixture/ca.pem" "$fixture/request.json"
}

Invoke-Giggles '{"kind":"Ping"}'
Invoke-Giggles '{"kind":"Sql","sql":"INSERT INTO app (id,body) VALUES ($1,$2)","params":[{"Int":1},{"Text":"hello GigglesDB"}]}'
Invoke-Giggles '{"kind":"Sql","sql":"SELECT id,body FROM app WHERE id=$1","params":[{"Int":1}]}'
```

Responses are JSON. Ping should return `"code":"ok"`; the SELECT should return
the inserted ID and text in its SQL result. Repeating the INSERT with the same
primary key returns an error; choose another ID or issue an UPDATE.

This `giggles-client` binary belongs to **`giggles-server`**. Each invocation
of the helper above opens a new connection, so use a persistent client connection
for multi-statement transactions.

The client sends SQL over framed native wire, including when it reads a local
JSON request file. Use `--ping`, `--sql TEXT`, or `--native-text TEXT` in place
of the file path. See the [native language guide](../giggles-language/README.md)
for native language syntax.

### PostgreSQL clients and configuration for the mTLS listener

In the mTLS fixture above, port **7433** implements a bounded PostgreSQL wire subset. Tested clients include
`tokio-postgres 0.7.15` with `tokio-postgres-rustls 0.13` and bounded pgJDBC 42.7.3
query/transaction cases. TLS is required, hostname
`localhost`, user/database `giggles`, and the same application certificate/key/CA.
The certificate binding determines authority. General `psql`, JDBC and ORM
compatibility has not been qualified; use the bundled wire client above for the
quickstart. See the [network compatibility matrix](COMPATIBILITY.md)
and the protocol reference below for supported messages and driver examples.

**Bounded metadata support is implemented:** identity, schemas, tables, columns,
primary keys and type information use the real authorized catalog. Full DataGrip
GUI introspection is not yet qualified. To check capabilities before configuring a client, run
**PostgreSQL client compatibility** in IntelliJ or
`powershell -NoProfile -File scripts/check-pg-clients.ps1`. It uses isolated test
databases and reports each passing, failing and untested case. Failed or untested
cases deliberately produce a nonzero exit code.

The JSON configuration specifies `database`, `postgres`, `certificate`,
`private_key`, `client_ca`, `bindings`, `connections`, `skew_seconds` and
`max_pause_seconds`. Set `wire` to enable the framed native wire listener.
`native` is a deprecated alias for `wire` and keeps the `native=<addr>` startup
label. Setting both keys fails at startup. Paths resolve against the **process working
directory**, not the configuration file's directory. Keep using
`crates/giggles-server`, or convert those paths to absolute paths when moving the
configuration.

For your own schema, create the database and tables through the embedded CLI
before starting the service, then provision certificates and matching bindings.
The server opens an existing database; it does not create one. Network schema
creation is unavailable, and the demo application certificate is limited to
`app`. See [credential configuration and reload](CREDENTIALS_ADMISSION.md)
for adding bindings and certificate rotation.

### Troubleshooting

- **Address already in use:** change `wire` and `postgres` in your configuration
  to unused loopback ports, and update the client address accordingly.
- **File not found:** check your working directory and fixture path. A missing
  database is an error, not a request to create one.
- **Database already open:** stop the other server or embedded shell using that
  file. A database file has one process owner.
- **TLS or authorization failure:** use the certificate, key and CA from the
  same fixture, with TLS hostname `localhost`. The application identity is
  `app.pem`, not the server identity or an unbound certificate.
- **SQL refused:** check the [SQL reference](../giggles-core/SQL.md) and the
  table permissions in the binding. PostgreSQL wire support does not imply all
  PostgreSQL SQL is implemented.

## Details: single-host service

Rust server and native client over the existing giggles-core/redb database. This
is local implementation evidence, not production, cloud, ORM or general
PostgreSQL compatibility qualification.
See [COMPATIBILITY.md](COMPATIBILITY.md) and
[SECURITY.md](SECURITY.md).

## Fixture and native protocol reference

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

The fixture configuration binds `wire` to `127.0.0.1:7432` and pgwire to
`127.0.0.1:7433`. Startup prints actual addresses. If those ports are occupied,
edit a copy of the configuration to unused loopback ports. The database must
already exist. The server never creates or silently replaces a missing database.
Ctrl-C stops listeners and drops staged transactions. A synchronous core decision
already started may finish before shutdown.

Create a local request file containing `{"kind":"Ping"}` and run:

```powershell
cargo run --bin giggles-client -- 127.0.0.1:7432 localhost artifacts/NETWORK-FIXTURE/app.pem artifacts/NETWORK-FIXTURE/app.key.TEST-ONLY artifacts/NETWORK-FIXTURE/ca.pem REQUEST.json
```

The framed native wire protocol uses protobuf messages. The old length-prefixed
JSON transport has been removed. Local JSON request files still support parameterized
SQL and a single `Deadline` wrapper with a cooperative `budget_ms` up to 10000.

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
extended requests.

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

For CONFIG.json mode without stdin control (for example in containers), run
`giggles-server --no-stdin-control CONFIG.json`. This disables stdin reads;
SIGINT and, on Unix, SIGTERM use the graceful shutdown path. The image uses
this flag by default, so `docker stop` drains the server. Reload requires
stdin control; omit the flag to use the trusted local `reload` command.

Optional HTTP/1.1, HTTP/2 and typed gRPC interfaces expose native reads, guarded
writes, claims and atomic blocks as structured requests. Native and SQL calls
share persistent transactions; typed controls let native clients begin, commit
and roll back without SQL strings. Existing scoped task operations remain
available. See [API.md](API.md) for configuration, schemas, limits and runnable
clients. Both listeners are disabled unless configured and reuse the existing
mTLS bindings and shared service admission.

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

HTTP/gRPC also accept native language text through the `text` request arm, for
example `{"text":{"text":"app where id=7 select id","languageVersion":"0.16"}}`.
Optional named `bindings` use the structured operation value format. The omitted
version defaults to CLI profile `0.1`; supported versions are `0.1` through `0.16`.
Lowered data statements and blocks share application table authorization,
results, and retained transactions with structured operations. Catalog and
inspection text are refused. See [API.md](API.md) for errors and wire details.

## Usage from sh

From the repository root:

```sh
cargo run --manifest-path crates/giggles-server/Cargo.toml -- --dev --data DIR --port 5432
```
