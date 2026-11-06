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
metadata, JDBC schemas/tables/columns/keys/type information, and captured
DataGrip global and application discovery queries. PostgreSQL OIDs, arrays,
`int2vector` and `oidvector` retain their wire types and vector subscripts start
at zero. Unknown catalogs are rejected.

The virtual catalog describes one certificate-bound presentation role. Role
inheritance, role expiration and PostgreSQL role connection policies are absent;
the expiration and connection-limit fields are NULL. Managed PostgreSQL tablespaces,
extensions, foreign servers, procedural languages, event triggers, inheritance,
PostgreSQL access methods/operator classes/collations and user-defined casts have
explicit empty catalogs. The timezone catalogs are empty because presentation
labels do not establish a timezone conversion database. The primary-key index
has real ordered key columns; PostgreSQL access-method/operator-class identities
and properties are unavailable. Check constraints are not supplied by the snapshot;
only actual primary-key and NOT NULL definitions are described.

The fixed database/user/schema identities are `giggles`/`giggles`/`public`.
Only `public` and `pg_catalog` namespaces are modeled. PostgreSQL MVCC `xmin`
fields are NULL because GigglesDB does not have PostgreSQL tuple transaction IDs.
Application relation OIDs derive from checked stable table IDs.

The engine supports the bounded joins, subqueries, casts, arrays, predicates and
window/array-expansion forms used by the retained driver fixtures. Bounded
nonrecursive CTEs, UNION/UNION ALL, array_agg with grouping and ordering, ARRAY
subqueries and lateral unnest with ordinality support the captured discovery
queries. Other aggregates/set operations, recursive CTEs, general lateral
queries, mixed catalog/application queries, writes and unknown functions fail
explicitly. varchar casts normalize to text in this finite profile.
SQL is limited to 64 KiB, results/materializations to 8,192 rows and 4 MiB of
conservative value accounting, and execution to a finite work budget. Parser
depth, expression count and snapshot sizes are bounded separately. Before AST
construction, the flat tokenizer admits at most 8,192 nonwhitespace tokens and
256 constructor tokens (keywords/operators/open delimiters/dots). This also
bounds left-associated trees whose recursive destruction could overflow.

The server permits integer/OID wire-width changes for parameters used only outside
SELECT projections, including pgJDBC metadata lookups. Values are decoded using
the declared wire width and range-checked against the inferred engine type.
Parameters appearing in any SELECT projection require matching declared types. Use an explicit cast such as `$1::int8` when projecting a typed
parameter; incompatible overrides return `0A000` rather than inconsistent
parameter/result descriptions. Application SQL retains its separate binding rules.

Run the library checks with:

```powershell
cargo test --locked --manifest-path crates/giggles-pg-catalog/Cargo.toml
```

Run actual driver probes with `scripts/check-pg-clients.ps1`. Captured query tests
do not qualify the complete DataGrip GUI or general PostgreSQL compatibility.

`version()` identifies GigglesDB and its bounded compatibility profile. The exact
DataGrip expression `round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC'))`
returns the hosting service initialization epoch in seconds when supplied through
`execute_with_startup`. The server captures this once when its first Service is
constructed. It is stable across sessions; restarts within one second can share
an epoch. This is not general PostgreSQL timestamp-function support. Standalone
`execute` rejects this expression because it has no hosting service clock.

PostgreSQL lock-age/MVCC queries (`pg_locks`, `age(transactionid)`, `txid_current`)
remain unsupported. DataGrip's observed optional StateNumber probe is among them;
no synthetic transaction marker is returned.
