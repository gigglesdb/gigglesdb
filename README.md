<p align="center"><img src="brand/gigglesdb-boulder.svg" alt="GigglesDB Boulder logo" width="360"></p>

# GigglesDB

GigglesDB is a database written in Rust. It runs embedded in your process or
as a server that PostgreSQL clients connect to, and it can replicate across
three machines. Agent and service workloads get scoped task operations with
explicit limits, approvals and receipts.

> **Status: under active development.** The embedded SQL engine and the
> development server are usable today. GigglesDB speaks a bounded subset of
> PostgreSQL SQL and wire protocol. It is not a drop-in PostgreSQL
> replacement and is not yet production-qualified. Read the
> [R1 release notes](docs/release-notes/R1.md) before upgrading an existing
> database.

The repository also contains two sibling tools:

- **[Skedaddle](crates/skedaddle/README.md):** a database migration tool with
  an engine-neutral coordinator and PostgreSQL-to-PostgreSQL components.
- **[Nunya](crates/nunya/README.md):** a database gateway for AI agents that
  replaces protected values with session-scoped tokens, so agents can use
  data they never see.

## Contents

- [Choose a path](#choose-a-path)
- [Prerequisites](#prerequisites)
- [Embedded SQL in three commands](#embedded-sql-in-three-commands)
- [Connect a PostgreSQL client](#connect-a-postgresql-client)
- [Run a mTLS service for agents and apps](#run-a-mtls-service-for-agents-and-apps)
- [Run a replicated cluster](#run-a-replicated-cluster)
- [What's in the repository](#whats-in-the-repository)
- [Development](#development)

## Choose a path

| You want to… | Start here |
| --- | --- |
| Try SQL locally with no server | [Embedded SQL](#embedded-sql-in-three-commands) |
| Point psql, DataGrip or an app at it | [Connect a PostgreSQL client](#connect-a-postgresql-client) |
| Serve agents and services over mTLS, HTTPS/JSON or gRPC | [mTLS service](#run-a-mtls-service-for-agents-and-apps) |
| Survive the loss of a machine | [Replicated cluster](#run-a-replicated-cluster) |
| Use GigglesDB from IntelliJ | [IntelliJ run guide](scripts/README.md) |

## Prerequisites

- Rust and Cargo.
- libclang/LLVM, needed by the default RocksDB-backed storage engine.
  - **Windows:** set `LIBCLANG_PATH` to the directory that contains
    `libclang.dll`.
  - **macOS:** install the Xcode Command Line Tools, or `brew install llvm`
    and set `LIBCLANG_PATH` to its `lib` directory if needed.

No LLVM? Build with `--no-default-features` from `crates/giggles-core` or
`crates/giggles-server`. Those binaries use the legacy engine only and refuse
to create v2 databases.

### Storage engines

New databases default to the **v2** engine (`giggles create <path>`,
`giggles-server --dev`). Pass `--engine legacy` to choose the legacy engine.
Existing databases keep their engine; `giggles upgrade <path>` upgrades a
legacy database to v2. Embedders calling `Admin::create` still get legacy by
default.

## Embedded SQL in three commands

From the repository root, with a fresh path for `demo.db`:

```sh
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --create demo.db --sql "CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT)"
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --open demo.db --sql "INSERT INTO notes VALUES (1, 'hello GigglesDB')"
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --open demo.db --sql "SELECT * FROM notes LIMIT 10"
```

For an interactive session:

```sh
cargo run --manifest-path crates/giggles-cli/Cargo.toml -- --open demo.db --repl
```

The local shell has trusted access to the database files. The
[CLI guide](crates/giggles-cli/README.md) covers transactions, parameters and
JSON output; the [SQL reference](crates/giggles-core/SQL.md) lists what SQL is
supported.

## Connect a PostgreSQL client

Start the development server from the repository root:

```powershell
./scripts/dev.ps1 -Task dev-server -Port 5432
```

```sh
./scripts/dev.sh --task dev-server --port 5432
```

Or run `giggles-server --dev --data DIR --port 5432` directly. Development
mode listens on loopback only and uses SCRAM-SHA-256 password authentication
without TLS.

On first start the server generates a random 24-character owner password and
prints it once, inside a connection URL. **Save it**: restarting with the same
data directory keeps the password and does not print it again. To choose the
password yourself, set a nonempty `GIGGLES_OWNER_PASSWORD` before first start.

```text
postgres://postgres:PASSWORD@127.0.0.1:5432/postgres?sslmode=disable
```

Replace `PASSWORD` with the owner password, URL-encoding special characters.
In DataGrip, choose PostgreSQL and enter:

| Field | Value |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `5432` (or the port you chose) |
| User | `postgres` |
| Password | The owner password |
| Database | `postgres` |
| SSL | Disabled |

**Reset the password:** stop the server, then run
`giggles-server --dev --data DIR --reset-owner-password` on the same data
directory (add `--owner NAME` for a nondefault owner). It uses
`GIGGLES_OWNER_PASSWORD` if set, otherwise generates a new password, and
prints the connection URL. Restart the server afterward.

**No password:** add `--trust-local` to the direct command. Any local process,
any Docker container reaching loopback and any port-forwarded peer can then
connect as any role, so use it only on a single-user machine.

More: the [server guide](crates/giggles-server/README.md), the
[network compatibility matrix](crates/giggles-server/COMPATIBILITY.md) and the
[app-style SQL load tester](scripts/app-loadtest/README.md).

## Run a mTLS service for agents and apps

`giggles-server CONFIG.json` opens an existing database and serves it over
mutually authenticated TLS: a framed JSON protocol, a bounded PostgreSQL wire
interface and, optionally, HTTPS/JSON and typed gRPC listeners. These
listeners expose native reads, guarded writes, claims and atomic blocks
alongside parameterized SQL and scoped task operations. Client certificates
map to database roles; there is no password fallback and no plaintext.

The [server guide](crates/giggles-server/README.md#configured-mtls-service)
walks through generating a demo database and certificates, starting the
server, sending queries, configuring PostgreSQL drivers and troubleshooting.
See also the [HTTP/gRPC guide](crates/giggles-server/API.md) and
[credential configuration and reload](crates/giggles-server/CREDENTIALS_ADMISSION.md).

## Run a replicated cluster

Starting several copies of the single-host server does **not** replicate a
database. A replicated database runs `giggles-server` with a `replication`
section on each of three machines. Every commit is acknowledged only once a
majority holds it durably, so losing one machine loses no acknowledged commit
and a new leader takes over automatically.

To see it work, this test launches three real server processes with
generated mTLS credentials, kills the leader and reconnects with a multi-host
connection string:

```powershell
cd crates/giggles-server
cargo test --locked --test all replication_process::three_process_failover -- --exact --nocapture
```

The [replication release notes](docs/release-notes/R4.md) cover
configuration, connecting clients, lost replies and replacing a machine. The
[distributed transactions release notes](docs/release-notes/R5.md) cover
isolation levels across a cluster.

## What's in the repository

**Database**

| Crate | What it is |
| --- | --- |
| [giggles-core](crates/giggles-core/README.md) | Embedded storage engine and SQL: typed tables, composite keys, transactions, scans, joins, aggregates, `EXPLAIN` |
| [giggles-server](crates/giggles-server/README.md) | Network server: PostgreSQL wire, framed JSON, HTTPS/JSON and gRPC |
| [giggles-cli](crates/giggles-cli/README.md) | Embedded SQL shell with JSON and table output |
| [giggles-language](crates/giggles-language/README.md) | Native embedded query language frontend |
| [giggles-replication](crates/giggles-replication/README.md) | Three-voter Raft replication |
| [giggles-transactions](crates/giggles-transactions/README.md) | Locking and recoverable replicated transaction state machines |
| [giggles-ffi](crates/giggles-ffi/README.md) | C ABI for in-process embedding |

**PostgreSQL compatibility**

| Crate | What it is |
| --- | --- |
| [giggles-pg-types](crates/giggles-pg-types/README.md) | Numeric, float and temporal types with PostgreSQL semantics |
| [giggles-pg-json](crates/giggles-pg-json/README.md) | JSON and JSONB |
| [giggles-pg-uuid](crates/giggles-pg-uuid/README.md) | UUID |
| [giggles-icu](crates/giggles-icu/README.md) | PostgreSQL-compatible ICU collations (pinned ICU 76.1) |
| [giggles-pg-catalog](crates/giggles-pg-catalog/README.md) | Read-only PostgreSQL catalog queries for client introspection |
| [giggles-pg-conformance](crates/giggles-pg-conformance/README.md) | Driver conformance runner against real PostgreSQL clients |
| [compat](compat/README.md) | Client output comparison harness against a pinned PostgreSQL |

**Operations and applications**

| Crate | What it is |
| --- | --- |
| [giggles-ops](crates/giggles-ops/README.md) | Snapshot backup catalog and restore |
| [giggles-export](crates/giggles-export/README.md) | Logical export and import |
| [giggles-migrate](crates/giggles-migrate/README.md) | Offline migration rehearsal |
| [giggles-release](crates/giggles-release/README.md) | Deterministic release packaging |
| [giggles-ledger](crates/giggles-ledger/README.md) | Embedded financial ledger |
| [giggles-outbox](crates/giggles-outbox/README.md) | Transactional outbox |
| [giggles-topology](crates/giggles-topology/README.md) | Declarative topology, quorum and dependency analysis |

**Testing and verification**

| Crate | What it is |
| --- | --- |
| [giggles-history](crates/giggles-history/README.md) | Strict-serializability history checker |
| [giggles-faultlab](crates/giggles-faultlab/README.md) | TCP fault-injection proxy |
| [giggles-model](crates/giggles-model/README.md) | Executable reference specification |
| [giggles-engine-bench](crates/giggles-engine-bench/README.md) | Storage engine benchmark |
| [giggles-history-runtime](crates/giggles-history-runtime/README.md) | Retired history harness |

**Sibling tools:** [Skedaddle](crates/skedaddle/README.md) and
[Nunya](crates/nunya/README.md). Brand assets are in [brand](brand/README.md).

Each crate's README states what it supports and its current limits.

## Development

See [Contributing](CONTRIBUTING.md) and [Security](SECURITY.md).

Each crate has its own Cargo manifest; there is no root workspace. Run a
crate's tests with:

```sh
cargo test --manifest-path crates/giggles-core/Cargo.toml
```

Some integration suites launch local processes and keep test databases and
logs; check the crate's README before running them. Shared IntelliJ run
configurations cover setup, the server, SQL and native queries,
backup/restore, failover suites and per-crate tests; see the
[IntelliJ run guide](scripts/README.md).

AI coding agents working in this repository follow [AGENTS.md](AGENTS.md).

## License

GigglesDB is licensed under the [Mozilla Public License 2.0](LICENSE).

You may use, embed and link GigglesDB in closed-source or commercial applications.
If you distribute modified versions of GigglesDB's own files, those files must
remain available under MPL-2.0. Third-party components keep their own licenses;
see [Third-party notices](THIRD_PARTY_NOTICES.md).
