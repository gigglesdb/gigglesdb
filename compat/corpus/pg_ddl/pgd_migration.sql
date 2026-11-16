-- case: pgd_migration
-- title: migration
-- owner: E1
-- expect: compatible
-- source: PG18 Rails Django Prisma Alembic migration
CREATE TABLE r0c_pgd_migration (id int PRIMARY KEY, name text);
INSERT INTO r0c_pgd_migration VALUES(1,'Ada');
ALTER TABLE r0c_pgd_migration ADD COLUMN slug text DEFAULT lower('PENDING');
UPDATE r0c_pgd_migration SET slug=lower(name);
CREATE INDEX CONCURRENTLY r0c_pgd_migration_i ON r0c_pgd_migration(slug);
ALTER TABLE r0c_pgd_migration RENAME COLUMN name TO display_name;
ALTER TABLE r0c_pgd_migration ALTER COLUMN slug TYPE varchar(100) USING slug::varchar(100);
SELECT * FROM r0c_pgd_migration ORDER BY id;
DROP INDEX r0c_pgd_migration_i;

-- # W4 coverage
-- 
-- All SQL case names below have the `pgd_` prefix. All files are LF.
-- 
-- ## Differences: Tables, indexes and other schema objects
-- 
-- | Row | Cases (prefix omitted) |
-- | --- | --- |
-- | Temporary / unlogged | temp_drop, temp_delete, temp_preserve, unlogged |
-- | CTAS / SELECT INTO | ctas, ctas_no_data, select_into |
-- | LIKE | like_all |
-- | Partitioning / inheritance / storage | partition_range, partition_list, partition_hash, partition_attach, inheritance, storage_fillfactor |
-- | Generated columns | generated_stored, generated_virtual, alter_expression |
-- | Expression defaults | default_function, default_arithmetic, default_user, default_uuid, migration |
-- | Over 64 columns / 64 KiB rows | wide_columns, wide_row |
-- | Date / time / interval / char primary keys | pk_date, pk_time, pk_interval, pk_char |
-- | EXCLUDE / NULLS NOT DISTINCT / deferred UNIQUE / MATCH FULL | exclude, unique_nulls, unique_deferred, fk_match_full |
-- | ALTER COLUMN TYPE | alter_type_using, alter_type_widen, alter_type_narrow, migration |
-- | ALTER TABLE IF EXISTS / RENAME CONSTRAINT / OWNER / identity | alter_if_exists, alter_rename_constraint, alter_owner_schema, alter_identity |
-- | SET SCHEMA | alter_owner_schema |
-- | Noninteger column indexes and UNIQUE | idx_{numeric,real,double,uuid,jsonb,array,char,date,time,timestamp,timestamptz,interval,boolean,bytea}, each also with _unique suffix |
-- | Over eight indexes | idx_nine |
-- | Expression / partial / INCLUDE / descending / methods | idx_expression, idx_partial, idx_include, idx_descending, idx_multi, idx_gin_jsonb, idx_gin_array, idx_gist, idx_hash, idx_brin |
-- | Concurrent indexes / REINDEX | idx_concurrent, idx_reindex, idx_if_exists |
-- | ALTER INDEX / SCHEMA | idx_alter, schema |
-- | Sequence rename / temporary | sequence, sequence_temp |
-- | Views with query clauses | view_{where,join,aggregate,expression,distinct,order,limit,cte,union,replace,security_barrier,security_invoker} |
-- | Writable views | view_insert, view_update, view_delete, view_check |
-- | Materialized views | matview, matview_concurrent |
-- | Other object comments | comment_*; database, large_object |
-- | Long comments | comment_long (70,000 bytes using repeat) |
-- | CREATE DATABASE / TABLESPACE | database, tablespace |
-- | Rules / publications (W4 override) | rule, publication |
-- 
-- Stored logic rows, CALL and DO are otherwise excluded.
-- 
-- ## W4 commands
-- 
-- | Commands | Cases |
-- | --- | --- |
-- | CREATE/ALTER/DROP TABLE | alter_*, partition_*, unlogged, like_all, migration |
-- | CREATE TABLE AS / SELECT INTO | ctas, ctas_no_data, select_into |
-- | CREATE/ALTER/DROP INDEX / REINDEX | idx_* |
-- | CLUSTER | cluster (also ALTER TABLE CLUSTER ON / SET WITHOUT CLUSTER) |
-- | CREATE/ALTER/DROP VIEW | view_replace and other view_* |
-- | CREATE/ALTER/DROP MATERIALIZED VIEW / REFRESH | matview, matview_concurrent |
-- | CREATE/ALTER/DROP SEQUENCE | sequence, sequence_temp |
-- | CREATE/ALTER/DROP SCHEMA | schema |
-- | COMMENT | comment_*, database, large_object |
-- | CREATE/ALTER/DROP DATABASE | database |
-- | CREATE/ALTER/DROP TABLESPACE | tablespace (expected errors for missing directory/object) |
-- | CREATE/ALTER/DROP STATISTICS | statistics |
-- | CREATE/ALTER/DROP RULE | rule |
-- | CREATE/ALTER/DROP PUBLICATION | publication |
-- | CREATE/ALTER/DROP SUBSCRIPTION | subscription (connect=false, detached slot) |
-- | VACUUM / ANALYZE / CHECKPOINT | maintenance; statistics also ANALYZE |
-- | SECURITY LABEL | security_label (no provider, expected error) |
-- | ALTER LARGE OBJECT | large_object |
-- | ALTER SYSTEM | alter_system (SET then RESET, without reload) |
-- 
-- Additional ALTER TABLE cases cover defaults, nullability, column/table renames,
-- constraint validation, foreign-key validation/deferrability, identity options,
-- statistics, storage, compression, attribute options, table options, logging,
-- access method, tablespace, typed tables, inheritance, constraint USING INDEX,
-- replica identity, RLS flags and trigger/rule enable modes.
-- 
-- ## Environment and grammar limits
-- 
-- Extension identifiers must name installed control files, so a case-prefixed
-- extension cannot be created. COMMENT on a missing extension and transform is
-- an expected-error probe in comment_unavailable. Tablespaces require a server
-- filesystem directory; their creation/alter/drop and COMMENT are expected-error
-- probes. No server files or built-in object comments are changed.
-- 
-- Operators require symbolic names, so comment_operator isolates its symbol in
-- a case-prefixed schema. Large objects require numeric identifiers; the two
-- large-object cases use explicit distinct OIDs and unlink them before returning.
-- Foreign tables cannot have primary keys. CTAS adds a primary key immediately;
-- LIKE INCLUDING ALL and partitions inherit/copy their primary keys.
