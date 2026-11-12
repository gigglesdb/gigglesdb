# Python ORM qualification

Requires Docker Linux containers and a default-feature `giggles-server` binary.
From the repository root:

```powershell
python -I compat/orm/run.py --server-bin J:/gigglesdb-work/p1-orm/target/debug/giggles-server.exe --orm all
```

Select `django` or `sqlalchemy` for one lane. Output defaults to unique directories
under `J:/gigglesdb-work/p1-orm/runs`; `--out-root` must remain under
`J:/gigglesdb-work`. Reports contain the immutable client image ID, installed
versions, binary SHA-256, per-step results and captured SQL/parameters/SQLSTATEs.
Exit 0 means every selected step passed; exit 1 includes product or infrastructure
failures. A forward migration failure marks model steps FAIL/blocked; no alternate
schema or backend patches are used. Reverse migration is still attempted.

Both ORMs use users with unique email, orders with FK/status/numeric total/UTC
timestamp, and order items with a composite `(order_id, sku)` key and FK. Integer
IDs are application assigned. Only integer FK and text status columns are indexed.
Neither ORM defaults to numeric or timestamp indexes; these unsupported index
types are deliberately excluded and noted in every report.

Django 5.2.17 (LTS), SQLAlchemy 2.0.57, Alembic 1.20.1 and psycopg 3.3.8 use
their standard PostgreSQL backends unchanged. The Python base and PostgreSQL 18.6
oracle digests are in `compat/pins.json`. The runner builds the small sample image,
runs the same code against both targets, starts GigglesDB v2 with password auth,
and cleans up containers, networks and fresh server databases. Credentials enter
children through environment variables and are redacted from artifacts.
