# orm

A compatibility harness for contributors comparing ORM migrations and model operations against PostgreSQL.

**Status:** Exercises bounded Django, SQLAlchemy, Rails and Prisma samples; numeric and timestamp indexes are excluded.

Part of [GigglesDB](../../README.md).

Requires Docker Linux containers and a default-feature `giggles-server` binary.
From the repository root:

```powershell
python -I compat/orm/run.py --server-bin <work-dir>/target/debug/giggles-server.exe --orm all
```

Select `django`, `sqlalchemy`, `rails` or `prisma` for one lane; `all` runs all four.
Output defaults to unique directories in the script's fixed Windows work root. Use `--out-root` to choose a directory within that root. Reports contain the immutable client image ID, installed
versions, binary SHA-256, per-step results and captured SQL/parameters/SQLSTATEs.
Exit 0 means every selected step passed; exit 1 includes product or infrastructure
failures. A forward migration failure marks model steps FAIL/blocked; no alternate
schema or backend patches are used. Reverse migration is still attempted.

All four ORMs use users with unique email, orders with FK/status/numeric total/UTC
timestamp, and order items with a composite `(order_id, sku)` key and FK. Integer
IDs are application assigned. Only integer FK and text status columns are indexed.
None of these samples defaults to numeric or timestamp indexes; these unsupported index
types are deliberately excluded and noted in every report.

Django 5.2.17 (LTS), SQLAlchemy 2.0.57, Alembic 1.20.1 and psycopg 3.3.8 use
their standard PostgreSQL backends unchanged. The Python base and PostgreSQL 18.6
oracle digests are in `compat/pins.json`. The runner builds the small sample image,
runs the same code against both targets, starts GigglesDB v2 with password auth,
and cleans up containers, networks and fresh server databases. Credentials enter
children through environment variables and are redacted from artifacts.

Rails uses Ruby 4.0.9, Rails/ActiveRecord 8.1.4.1 and pg 1.7.0. Its existing
`postgres` database connection and `ActiveRecord::MigrationContext#migrate` exercise
the connection/migration path used by Rails database tasks; `migrate(0)` reverses
the sample migration. Models exercise composite keys, `includes`, CRUD, count,
pagination, a nested `requires_new` savepoint and `RecordNotUnique`/23505.

Prisma uses Node 26 LTS and Prisma/Client 6.19.3. The image generates migration SQL
from the schema using `prisma migrate diff --from-empty`; each target runs the
unchanged `prisma migrate deploy`. Client checks include relations, CRUD, count,
pagination, committed/rolled-back `$transaction` and P2002. An explicit down step
drops the three sample tables and `_prisma_migrations` and checks their absence.
Both new lanes have ten report steps and separate pinned Docker images.
Each lane gets a fresh GigglesDB database and an empty PostgreSQL `public` schema;
tracking tables from an earlier ORM cannot interfere with Prisma's empty-schema check.
Rails SQL notifications and Prisma query/engine logging capture statements. When Prisma
suppresses a database error, the runner matches the exact SQL's SHA-256 to the
server's SQLSTATE diagnostic. Startup rejection has `sql: null` because no SQL
reached the server. Eight model steps remain blocked if forward migration fails;
the reverse command is attempted but cannot qualify a round trip on its own.
