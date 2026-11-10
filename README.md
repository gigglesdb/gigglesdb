# GigglesDB

GigglesDB is a Rust database with an embedded SQL engine, distributed consensus,
and scoped task operations with explicit limits, approvals and receipts.

It is under active development. The embedded SQL shell is usable today;
distributed features have different implementation and verification limits.
This is not a production-qualified release or a PostgreSQL-compatible replacement.

## Build prerequisites and engines

Install Rust/Cargo and libclang/LLVM (required by the default RocksDB build).
On Windows, set `LIBCLANG_PATH` to the directory containing `libclang.dll`
before building or running `scripts/dev.ps1` or `scripts/dev.sh`. On macOS,
install Xcode Command Line Tools or `brew install llvm` (set `LIBCLANG_PATH`
to its `lib` directory if needed).

New databases created by `giggles create <path>` and `giggles-server --dev`
default to v2. Use `giggles create --engine legacy <path>` or the server's
`--engine legacy` to choose legacy. Existing databases retain their engine;
`giggles upgrade <path>` explicitly upgrades legacy databases to v2.
`Admin::create` keeps its legacy default for embedders.

For a legacy-only build without LLVM, run `cargo build --no-default-features`
from `crates/giggles-core` or `crates/giggles-server`. Those binaries default
to legacy and reject requests to create v2 databases.

## Run from IntelliJ

Shared run configurations are included. Run **01 Setup local demo**, then
**02 Server**, then **03 Client Ping**. The setup creates and retains a local
database and development TLS credentials. The run selector also includes SQL and
native queries, backup/restore, multi-node failover suites, and per-crate tests.
See [IntelliJ setup and run guide](scripts/README.md) for requirements and details.

## Start a server

For a local server you can connect to and query, use **`giggles-server`**. It opens
one existing embedded database and exposes a JSON protocol and a bounded
PostgreSQL wire interface. Both require mutual TLS: the server and client each
present a certificate. There is no password-only or plaintext mode.

Optional HTTPS/JSON and typed gRPC listeners expose structured native reads,
guarded writes, claims and atomic blocks, alongside parameterized SQL and scoped
task operations. Native and SQL calls share persistent transactions, mTLS
bindings and service limits. See the
[HTTP/gRPC guide](crates/giggles-server/API.md) for configuration and runnable clients.

The quickest working setup uses the existing fixture generator, which creates
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
native=127.0.0.1:7432 postgres=127.0.0.1:7433 preview=single-host
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

This `giggles-client` binary belongs to **`giggles-server`**. The separately named
`crates/giggles-client` SDK targets the replicated task protocol. Each invocation
of the helper above opens a new connection, so use a persistent client connection
for multi-statement transactions.

Here, “native” means the framed JSON transport; `Sql` requests still contain SQL.
See the [native language guide](crates/giggles-language/README.md) for the separate
embedded language frontend.

### PostgreSQL clients and configuration

Port **7433** implements a bounded PostgreSQL wire subset. Tested clients include
`tokio-postgres 0.7.15` with `tokio-postgres-rustls 0.13` and bounded pgJDBC 42.7.3
query/transaction cases. TLS is required, hostname
`localhost`, user/database `giggles`, and the same application certificate/key/CA.
The certificate binding determines authority. General `psql`, JDBC and ORM
compatibility has not been qualified; use the bundled JSON client above for the
quickstart. See the [network compatibility matrix](crates/giggles-server/COMPATIBILITY.md)
and [server guide](crates/giggles-server/README.md) for supported messages and
driver examples.

**Bounded metadata support is implemented:** identity, schemas, tables, columns,
primary keys and type information use the real authorized catalog. Full DataGrip
GUI introspection is not yet qualified. To check capabilities before configuring a client, run
**PostgreSQL client compatibility** in IntelliJ or
`powershell -NoProfile -File scripts/check-pg-clients.ps1`. It uses isolated test
databases and reports each passing, failing and untested case. Failed or untested
cases deliberately produce a nonzero exit code.

The JSON configuration specifies `database`, `native`, `postgres`, `certificate`,
`private_key`, `client_ca`, `bindings`, `connections`, `skew_seconds` and
`max_pause_seconds`. Paths inside it resolve against the **process working
directory**, not the configuration file's directory. Keep using
`crates/giggles-server`, or convert those paths to absolute paths when moving the
configuration.

For your own schema, create the database and tables through the embedded CLI
before starting the service, then provision certificates and matching bindings.
The server opens an existing database; it does not create one. Network schema
creation is unavailable, and the demo application certificate is limited to
`app`. See [credential configuration and reload](crates/giggles-server/CREDENTIALS_ADMISSION.md)
for adding bindings and certificate rotation.

### Troubleshooting

- **Address already in use:** change `native` and `postgres` in your configuration
  to unused loopback ports, and update the client address accordingly.
- **File not found:** check your working directory and fixture path. A missing
  database is an error, not a request to create one.
- **Database already open:** stop the other server or embedded shell using that
  file. A database file has one process owner.
- **TLS or authorization failure:** use the certificate, key and CA from the
  same fixture, with TLS hostname `localhost`. The application identity is
  `app.pem`, not the server identity or an unbound certificate.
- **SQL refused:** check the [SQL reference](crates/giggles-core/SQL.md) and the
  table permissions in the binding. PostgreSQL wire support does not imply all
  PostgreSQL SQL is implemented.

## Embedded SQL, without a server

Install Rust and Cargo, then run these commands from the repository root.
Use a fresh path for `demo.db`:

```sh
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --create demo.db --sql "CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT)"
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --open demo.db --sql "INSERT INTO notes VALUES (1, 'hello GigglesDB')"
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --open demo.db --sql "SELECT * FROM notes LIMIT 10"
```

Start an interactive session:

```sh
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --open demo.db --repl
```

The local shell has trusted access to the database files. See the
[CLI guide](crates/giggles-cli/README.md) for transactions, parameters and JSON output.

## What's here

- **Embedded storage and SQL:** typed tables, composite primary keys, transactions,
  bounded scans, indexed joins, global aggregates and `EXPLAIN`.
- **Distributed runtime:** durable Raft replication, multi-range transactions,
  snapshots and learner replacement.
- **Task operations:** scoped repair policies, exact approvals, deduplication,
  receipts and cumulative disclosure budgets on supported interfaces.
- **Tools:** a Rust client, native language frontend, backup/restore, logical
  export/import, ledger, transactional outbox and fault-testing utilities.

Start with the [SQL compatibility reference](crates/giggles-core/SQL.md),
[distributed runtime guide](crates/giggles-distributed/README.md),
[Rust client](crates/giggles-client/README.md), or
[native language guide](crates/giggles-language/README.md).
Each crate documents its supported behavior and current limits.

## Run multiple nodes

The single-host server above does **not** become replicated by starting multiple
copies. The distributed binaries use their own configurations and storage files:

| Binary | Use |
| --- | --- |
| `giggles-server` | Single-host application SQL and scoped task service |
| `giggles-node` (`giggles-consensus`) | Three-voter single-range consensus and replicated task interfaces |
| `giggles-ranges` (`giggles-distributed`) | Multi-range runtime, transactions and membership operations through trusted owner controls |

To exercise a real three-process cluster with generated mTLS credentials, run
from the repository root:

```powershell
cd crates/giggles-consensus
cargo test --locked --test process real_three_process_mtls_failover_snapshot_and_lost_ack -- --exact --nocapture
```

This launches actual node processes, exercises failover and retains their
configuration and evidence. For manual startup, use the retained per-node
configuration paths, one terminal per voter:

```powershell
cargo run --locked --bin giggles-node -- PATH/TO/node-1.json
```

Start the other voters with their own configurations and database files. Follow
the [consensus guide](crates/giggles-consensus/README.md) for peer identities,
existing-store startup and task listener provisioning, or the
[multi-range guide](crates/giggles-distributed/README.md) for `giggles-ranges`.
These interfaces have different capabilities; the single-host application SQL
quickstart is not a replicated SQL deployment recipe.

## Development

Crates currently have standalone Cargo manifests; there is no root workspace.
For example:

```sh
cargo test --manifest-path crates/giggles-core/Cargo.toml
cargo test --manifest-path crates/giggles-cli/Cargo.toml
```

Some integration suites launch local processes and retain test databases and
logs. Follow the relevant crate's instructions before running those suites.
