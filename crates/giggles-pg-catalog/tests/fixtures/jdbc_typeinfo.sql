-- Extracted from jdbc_PgDatabaseMetaData.java sha256 59a6de6b6ba0ef66fa6b382717a398c8709e371b75343c1074049bba311e0ae5
SELECT t.typname,t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid)  WHERE n.nspname  != 'pg_toast' AND  (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
