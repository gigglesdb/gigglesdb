-- Extracted from jdbc_TypeInfoCache.java sha256 f6f0a8a04946925e8e95bc7c532df34ff06135913ea1616aedbf162f155a3edb
SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_type e ON t.typelem = e.oid JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $1
