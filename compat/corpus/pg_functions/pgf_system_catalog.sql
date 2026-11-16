-- case: pgf_system_catalog
-- title: Catalog definitions comments privileges and sizes
-- owner: DD-0068
-- expect: compatible
-- source: functions.txt System and catalog functions; https://www.postgresql.org/docs/18/functions-info.html
CREATE TABLE r0c_pgf_system_catalog (id integer PRIMARY KEY, s varchar(12));
INSERT INTO r0c_pgf_system_catalog VALUES (1,'a'),(2,NULL);
COMMENT ON TABLE r0c_pgf_system_catalog IS 'parity table';
COMMENT ON COLUMN r0c_pgf_system_catalog.s IS 'parity column';
SELECT id, pg_typeof(s) FROM r0c_pgf_system_catalog ORDER BY id;
SELECT id, pg_column_size(s) FROM r0c_pgf_system_catalog ORDER BY id;
SELECT has_table_privilege('r0c_pgf_system_catalog','SELECT');
SELECT has_column_privilege('r0c_pgf_system_catalog','s','SELECT');
SELECT pg_relation_size('r0c_pgf_system_catalog')>=0;
SELECT to_regclass('r0c_pgf_system_catalog')::text;
SELECT obj_description('r0c_pgf_system_catalog'::regclass,'pg_class');
SELECT col_description('r0c_pgf_system_catalog'::regclass,2);
SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='r0c_pgf_system_catalog'::regclass ORDER BY conname;
SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid='r0c_pgf_system_catalog'::regclass ORDER BY indexrelid::regclass::text;
SELECT format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid='r0c_pgf_system_catalog'::regclass AND attnum>0 ORDER BY attnum;
SELECT pg_get_userbyid(relowner)=current_user FROM pg_class WHERE oid='r0c_pgf_system_catalog'::regclass;
DROP TABLE r0c_pgf_system_catalog;
