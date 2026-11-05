# GigglesDB

GigglesDB is a Rust database with an embedded SQL engine, distributed consensus,
and scoped task operations with explicit limits, approvals and receipts.

It is under active development. The embedded SQL shell is usable today;
distributed features have different implementation and verification limits.
This is not a production-qualified release or a PostgreSQL-compatible replacement.

## Try it

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

## Development

Crates currently have standalone Cargo manifests; there is no root workspace.
For example:

```sh
cargo test --manifest-path crates/giggles-core/Cargo.toml
cargo test --manifest-path crates/giggles-cli/Cargo.toml
```

Some integration suites launch local processes and retain test databases and
logs. Follow the relevant crate's instructions before running those suites.
