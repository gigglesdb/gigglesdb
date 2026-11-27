# PRIMARY KEY on char(n), date, time and timestamp columns; UNIQUE on char(n)

GigglesDB now accepts `PRIMARY KEY` on `char(n)`/`bpchar`, `date`, `time`, `timetz`, `timestamp` and `timestamptz`
columns (alone or in composite keys), and `UNIQUE` constraints and indexes on `char(n)`/`bpchar` columns, on both
engines. Schemas such as `country char(2) PRIMARY KEY`, `PRIMARY KEY (country, day date)` or
`at timestamptz PRIMARY KEY` work as in PostgreSQL 18.6:

- Keys sort and compare as PostgreSQL's btree does: `-infinity` < every finite date or timestamp < `infinity`;
  `time` `24:00:00` sorts last; `timetz` compares the UTC time first, then the offset; `timestamptz` keys are
  instants, so `'2024-01-01 13:00+01'` duplicates `'2024-01-01 12:00+00'` (23505) and hits `ON CONFLICT`.
- `char(n)` keys ignore trailing spaces: `'a'`, `'a '` and `'a  '` are the same key, `''` equals `' '`.
- Foreign keys to these parents, range scans, `BETWEEN`, point lookups and `ON CONFLICT` use the keys directly.
- `char(n)`/`bpchar` values now support `=`, `<>`, `<`, `<=`, `>`, `>=` (against char, text, varchar, literals
  and parameters, with PostgreSQL's operator choice), `IN`, joins, `ORDER BY`, `GROUP BY`, `DISTINCT`, `min`/`max`
  and `length`/`char_length`/`octet_length`, whether or not the column is indexed. Concatenation, `LIKE`,
  `upper`/`lower`, `coalesce`/`CASE`/`UNION` and `greatest`/`least` over char values still fail with 0A000.

Still refused with 0A000: `interval` primary keys, primary keys of these types through the native API, and
`ALTER COLUMN ... TYPE` of a char or temporal column that is in a primary key, unique constraint or index.
`timestamp` <-> `timestamptz` and `date` <-> `timestamptz` foreign keys stay refused with 42804 (PostgreSQL's
result there depends on the session TimeZone and can orphan rows).

## Database format: new feature bit 41 (`key_types`)

Stored keys of existing types are byte for byte as before; a database that never uses these keys does not get the
new bit, so older binaries keep opening it. While some table has a primary key on one of these types, or a unique
constraint or index on a `char(n)` column, the database carries **feature bit 41, `key_types`** (V2 and Legacy
format 53, same number). The bit is cleared again when the last such table, constraint or index is dropped.
**Older GigglesDB binaries refuse to open a database with bit 41 set** (unsupported feature); they never misread
its keys.
