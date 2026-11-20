-- case: pgr_has_role_tables
-- title: Privilege inquiries over application rows
-- owner: unowned
-- expect: compatible
-- source: PostgreSQL 18.6 oracle-b
CREATE ROLE r0c_pgr_has_role_tables_g;
CREATE ROLE r0c_pgr_has_role_tables_u IN ROLE r0c_pgr_has_role_tables_g;
CREATE ROLE r0c_pgr_has_role_tables_x;
CREATE TABLE r0c_pgr_has_role_tables_t (id int PRIMARY KEY, who text, priv text);
INSERT INTO r0c_pgr_has_role_tables_t VALUES (1,'r0c_pgr_has_role_tables_u','MEMBER'),(2,'r0c_pgr_has_role_tables_x','MEMBER'),(3,'r0c_pgr_has_role_tables_u','USAGE, SET'),(4,NULL,'MEMBER');
GRANT SELECT ON r0c_pgr_has_role_tables_t TO r0c_pgr_has_role_tables_g;
SELECT id, pg_has_role(who, 'r0c_pgr_has_role_tables_g', priv) FROM r0c_pgr_has_role_tables_t ORDER BY id;
SELECT id FROM r0c_pgr_has_role_tables_t WHERE pg_has_role(who, 'r0c_pgr_has_role_tables_g', 'MEMBER') ORDER BY id;
SELECT id, pg_has_role('r0c_pgr_has_role_tables_u','r0c_pgr_has_role_tables_g','MEMBER') AS c FROM r0c_pgr_has_role_tables_t ORDER BY id;
SELECT count(*) FILTER (WHERE pg_has_role(who,'r0c_pgr_has_role_tables_g','USAGE')) FROM r0c_pgr_has_role_tables_t;
SELECT pg_typeof(pg_has_role('r0c_pgr_has_role_tables_u','r0c_pgr_has_role_tables_g','MEMBER')) FROM r0c_pgr_has_role_tables_t WHERE id = 1;
SELECT id, has_table_privilege(who, 'r0c_pgr_has_role_tables_t', 'SELECT') FROM r0c_pgr_has_role_tables_t WHERE who IS NOT NULL ORDER BY id;
SELECT id, has_schema_privilege(who, 'public', 'USAGE') FROM r0c_pgr_has_role_tables_t WHERE who IS NOT NULL ORDER BY id;
SELECT id, has_column_privilege(who, 'r0c_pgr_has_role_tables_t', 'id', 'SELECT') FROM r0c_pgr_has_role_tables_t WHERE who IS NOT NULL ORDER BY id;
SELECT id, has_database_privilege(who, current_database(), 'CONNECT') FROM r0c_pgr_has_role_tables_t WHERE who IS NOT NULL ORDER BY id;
SELECT id, has_any_column_privilege(who, 'r0c_pgr_has_role_tables_t', 'SELECT') FROM r0c_pgr_has_role_tables_t WHERE who IS NOT NULL ORDER BY id;
CREATE SEQUENCE r0c_pgr_has_role_tables_s;
SELECT id, has_sequence_privilege('r0c_pgr_has_role_tables_s', 'USAGE') FROM r0c_pgr_has_role_tables_t ORDER BY id;
DROP SEQUENCE r0c_pgr_has_role_tables_s;
SELECT t.id, r.rolname FROM r0c_pgr_has_role_tables_t t JOIN pg_roles r ON r.rolname = t.who WHERE pg_has_role(r.oid, 'r0c_pgr_has_role_tables_g'::name, 'MEMBER') ORDER BY 1;
-- @error
SELECT id, pg_has_role(who, 'r0c_pgr_has_role_tables_missing', 'MEMBER') FROM r0c_pgr_has_role_tables_t ORDER BY id;
-- @error
SELECT id, pg_has_role(who, 'r0c_pgr_has_role_tables_g', 'BOGUS') FROM r0c_pgr_has_role_tables_t ORDER BY id;
INSERT INTO r0c_pgr_has_role_tables_t SELECT 5, 'r0c_pgr_has_role_tables_x', CASE WHEN pg_has_role('r0c_pgr_has_role_tables_x','r0c_pgr_has_role_tables_g','MEMBER') THEN 'yes' ELSE 'no' END;
UPDATE r0c_pgr_has_role_tables_t SET priv = 'upd' WHERE pg_has_role(who, 'r0c_pgr_has_role_tables_g', 'MEMBER') AND id = 1;
SELECT * FROM r0c_pgr_has_role_tables_t ORDER BY id;
SET ROLE r0c_pgr_has_role_tables_u;
SELECT id, pg_has_role('r0c_pgr_has_role_tables_g','USAGE'), has_table_privilege('r0c_pgr_has_role_tables_t','SELECT') FROM r0c_pgr_has_role_tables_t ORDER BY id;
RESET ROLE;
DROP TABLE r0c_pgr_has_role_tables_t;
DROP ROLE r0c_pgr_has_role_tables_u, r0c_pgr_has_role_tables_g, r0c_pgr_has_role_tables_x;
