# PostgreSQL catalog queries

This library evaluates a bounded, read-only PostgreSQL metadata query profile.
It does not open a database or access application rows. Callers supply a coherent
`CatalogSnapshot` containing only the tables the current session may inspect.

`plan(sql)` classifies and compiles a query; ordinary application statements
return `None`. `describe(plan)` returns PostgreSQL field types and OIDs.
`execute(plan, snapshot, parameters)` evaluates against the supplied definitions.
Table-free SELECTs such as `SELECT 1` also use this evaluator, including normal
expression typing, predicates and limits. They are not canned success responses.
Plans contain no authorization or cached catalog data. Recheck authorization and
schema consistency before execution and before releasing cached results.

The server adapter supports identity functions, real table/column/primary-key
metadata, JDBC schemas/tables/columns/keys/type information, and six captured
DataGrip discovery queries. PostgreSQL OIDs, arrays and `int2vector` retain their
wire types. Absent procedural languages and event triggers have explicitly
modeled empty relations. Unknown catalogs are rejected, not invented.

The fixed database/user/schema identities are `giggles`/`giggles`/`public`.
Only `public` and `pg_catalog` namespaces are modeled. PostgreSQL MVCC `xmin`
fields are NULL because GigglesDB does not have PostgreSQL tuple transaction IDs.
Application relation OIDs derive from checked stable table IDs.

The engine supports the bounded joins, subqueries, casts, arrays, predicates and
window/array-expansion forms used by the retained driver fixtures. It rejects
mixed catalog/application queries, writes, CTEs, set operations, aggregates,
general `regclass` resolution, lateral sources and unknown functions.
SQL is limited to 64 KiB, results/materializations to 8,192 rows and 4 MiB of
conservative value accounting, and execution to a finite work budget. Parser
depth, expression count and snapshot sizes are bounded separately.

The server currently requires explicit parameter OIDs to match inferred catalog
parameter types. Use an explicit cast such as `$1::int8` when projecting a typed
parameter; incompatible overrides return `0A000` rather than inconsistent
parameter/result descriptions. Application SQL retains its separate binding rules.

Run the library checks with:

```powershell
cargo test --locked --manifest-path crates/giggles-pg-catalog/Cargo.toml
```

Run actual driver probes with `scripts/check-pg-clients.ps1`. Captured query tests
do not qualify the complete DataGrip GUI or general PostgreSQL compatibility.
