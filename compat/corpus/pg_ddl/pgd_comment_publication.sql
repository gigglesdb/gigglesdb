-- case: pgd_comment_publication
-- title: comment publication
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_publication (id int PRIMARY KEY, v int);
CREATE PUBLICATION r0c_pgd_comment_publication_p FOR TABLE r0c_pgd_comment_publication;
COMMENT ON PUBLICATION r0c_pgd_comment_publication_p IS 'migration note';
SELECT obj_description((SELECT oid FROM pg_publication WHERE pubname='r0c_pgd_comment_publication_p'),'pg_publication');
COMMENT ON PUBLICATION r0c_pgd_comment_publication_p IS NULL;
SELECT obj_description((SELECT oid FROM pg_publication WHERE pubname='r0c_pgd_comment_publication_p'),'pg_publication') IS NULL AS cleared;
DROP PUBLICATION r0c_pgd_comment_publication_p;
