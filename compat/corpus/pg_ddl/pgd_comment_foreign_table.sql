-- case: pgd_comment_foreign_table
-- title: comment foreign table
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE FOREIGN DATA WRAPPER r0c_pgd_comment_foreign_table_fdw;
CREATE SERVER r0c_pgd_comment_foreign_table_s FOREIGN DATA WRAPPER r0c_pgd_comment_foreign_table_fdw;
-- primary-key exception: PostgreSQL foreign tables do not support PRIMARY KEY constraints.
CREATE FOREIGN TABLE r0c_pgd_comment_foreign_table (id int) SERVER r0c_pgd_comment_foreign_table_s;
COMMENT ON FOREIGN TABLE r0c_pgd_comment_foreign_table IS 'catalog note';
SELECT obj_description('r0c_pgd_comment_foreign_table'::regclass,'pg_class');
COMMENT ON FOREIGN TABLE r0c_pgd_comment_foreign_table IS NULL;
SELECT obj_description('r0c_pgd_comment_foreign_table'::regclass,'pg_class') IS NULL AS cleared;
DROP FOREIGN TABLE r0c_pgd_comment_foreign_table;
DROP SERVER r0c_pgd_comment_foreign_table_s;
DROP FOREIGN DATA WRAPPER r0c_pgd_comment_foreign_table_fdw;
