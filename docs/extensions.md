# Built-in PostgreSQL extensions

GigglesDB includes plpgsql 1.0 and uuid-ossp 1.1. uuid-ossp is built in: no package or file installation step is needed. plpgsql is always present; enable uuid-ossp in each database with `CREATE EXTENSION "uuid-ossp"`.

```sql
CREATE EXTENSION [IF NOT EXISTS] name [WITH] [SCHEMA schema] [VERSION version] [CASCADE];
DROP EXTENSION [IF EXISTS] name [, ...] [CASCADE | RESTRICT];
```

The CREATE options can appear in any order. Functions belong to the chosen schema, or the first existing schema in search_path. DROP RESTRICT protects column defaults; CASCADE removes dependent defaults. plpgsql cannot be dropped. Other extension names return PostgreSQL's SQLSTATE 0A000 `extension "name" is not available`, with the hint `The extension must first be installed on the system where PostgreSQL is running.`

uuid-ossp provides uuid_nil, uuid_ns_dns, uuid_ns_url, uuid_ns_oid, uuid_ns_x500, uuid_generate_v1, uuid_generate_v1mc, uuid_generate_v3, uuid_generate_v4 and uuid_generate_v5. uuid_generate_v1 uses a random multicast node instead of the host MAC address.

DROP SCHEMA RESTRICT protects extensions installed in that schema. DROP SCHEMA CASCADE removes them and their dependent column defaults. Schema renaming keeps the extension and its functions in the renamed schema; schemas containing relations, types, collations or sequences still reject renaming.

The v2 extension inventory uses catalog key 0x19 (tag 25); feature bit 25 guards its presence. Tag 24 belongs to task credentials.
