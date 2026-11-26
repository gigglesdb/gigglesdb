# Storage parameters, UNLOGGED, CLUSTER ON and column options are kept as catalog metadata (V2)

GigglesDB now accepts PostgreSQL's storage settings and keeps them in the catalog, where `pg_class.reloptions`,
`pg_class.relpersistence`, `pg_index.indisclustered`, `pg_attribute.attstattarget` / `attstorage` /
`attcompression` / `attoptions` and psql's `\d+` show them as PostgreSQL 18.6 does. They are metadata only and
change no behaviour.

- `CREATE TABLE ... WITH (fillfactor=70, autovacuum_enabled=false, ...)`, `ALTER TABLE ... SET (...)` /
  `RESET (...)`, and the same for indexes (`CREATE INDEX ... WITH (...)`, `ALTER INDEX ... SET/RESET`), with
  PostgreSQL's option-name and value checks and error texts.
- `CREATE UNLOGGED TABLE` and `ALTER TABLE ... SET LOGGED | SET UNLOGGED`. **UNLOGGED tables stay durable**: their
  rows are written and recovered like any other table's; only `relpersistence` reports `u`.
- `CLUSTER t USING idx` and `ALTER TABLE ... CLUSTER ON idx` mark the index (`indisclustered`); `SET WITHOUT CLUSTER`
  clears it; bare `CLUSTER t` uses the marked index and fails as in PostgreSQL when none is marked. CLUSTER does not
  reorder rows.
- `ALTER TABLE ... ALTER COLUMN ... SET STATISTICS n`, `SET STORAGE`, `SET COMPRESSION`, `SET (n_distinct=...)` /
  `RESET (...)`.
- `ALTER TABLE IF EXISTS <missing table> ...` gives PostgreSQL's "does not exist, skipping" notice.
- pgwire `text[]` values are quoted only where PostgreSQL's array output quotes them (`{fillfactor=70}`,
  `{postgres=UC/postgres}`), instead of quoting every element.

## Database format: new V2 feature bit 40 (`storage_metadata`)

A table that uses none of these settings is stored byte for byte as before, and a database that never uses them
does not get the new bit, so older binaries keep opening it. The first committed table that carries a storage
setting stores a versioned table-metadata tail and sets **V2 feature bit 40, `storage_metadata`**. The bit is not
cleared when the setting is later removed (as for GIN indexes and generated columns). **Older GigglesDB binaries
refuse to open a database with bit 40 set** (unsupported feature); they never misread it. GIN `WITH (...)` options
keep their earlier encoding and do not set the bit.

## Legacy engine

The Legacy engine (format 53) is unchanged. Statements that would store a non-default storage setting there fail
with SQLSTATE 0A000; statements that leave the defaults in place (for example `RESET` of an unset option,
`SET LOGGED` on a logged table, `CLUSTER t USING idx`, which is accepted without recording the marker) succeed.

## Known differences

- `toast.*` options are validated and kept, but GigglesDB has no separate TOAST relation, so there is no TOAST
  `pg_class` row to show them on.
- VACUUM, ANALYZE and CLUSTER remain no-ops (GD-0092 D24 Phase A).
