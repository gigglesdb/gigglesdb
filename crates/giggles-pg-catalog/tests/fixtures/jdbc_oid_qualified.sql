-- Extracted from jdbc_TypeInfoCache.java sha256 f6f0a8a04946925e8e95bc7c532df34ff06135913ea1616aedbf162f155a3edb
SELECT t.oid, t.typname   FROM pg_catalog.pg_type t  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.typname = $1 AND (n.nspname = $2 OR $3 AND n.nspname = ANY (current_schemas(true))) ORDER BY t.oid DESC LIMIT 1
