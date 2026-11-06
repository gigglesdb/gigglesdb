-- Extracted from jdbc_TypeInfoCache.java sha256 f6f0a8a04946925e8e95bc7c532df34ff06135913ea1616aedbf162f155a3edb
SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = $1 and t.typelem = e.oid
