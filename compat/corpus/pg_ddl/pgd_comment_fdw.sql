-- case: pgd_comment_fdw
-- title: comment fdw
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE FOREIGN DATA WRAPPER r0c_pgd_comment_fdw;
COMMENT ON FOREIGN DATA WRAPPER r0c_pgd_comment_fdw IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_foreign_data_wrapper WHERE fdwname='r0c_pgd_comment_fdw'),'pg_foreign_data_wrapper');
COMMENT ON FOREIGN DATA WRAPPER r0c_pgd_comment_fdw IS NULL;
SELECT obj_description((SELECT oid FROM pg_foreign_data_wrapper WHERE fdwname='r0c_pgd_comment_fdw'),'pg_foreign_data_wrapper') IS NULL AS cleared;
DROP FOREIGN DATA WRAPPER r0c_pgd_comment_fdw;
