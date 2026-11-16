-- case: pgd_comment_server
-- title: comment server
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE FOREIGN DATA WRAPPER r0c_pgd_comment_server_fdw;
CREATE SERVER r0c_pgd_comment_server FOREIGN DATA WRAPPER r0c_pgd_comment_server_fdw;
COMMENT ON SERVER r0c_pgd_comment_server IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_foreign_server WHERE srvname='r0c_pgd_comment_server'),'pg_foreign_server');
COMMENT ON SERVER r0c_pgd_comment_server IS NULL;
SELECT obj_description((SELECT oid FROM pg_foreign_server WHERE srvname='r0c_pgd_comment_server'),'pg_foreign_server') IS NULL AS cleared;
DROP SERVER r0c_pgd_comment_server;
DROP FOREIGN DATA WRAPPER r0c_pgd_comment_server_fdw;
