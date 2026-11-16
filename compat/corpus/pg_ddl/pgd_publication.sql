-- case: pgd_publication
-- title: publication
-- owner: G
-- expect: compatible
-- source: W4 CREATE ALTER DROP PUBLICATION
CREATE TABLE r0c_pgd_publication (id int PRIMARY KEY, v int);
CREATE PUBLICATION r0c_pgd_publication_p FOR TABLE r0c_pgd_publication;
ALTER PUBLICATION r0c_pgd_publication_p SET (publish='insert,update');
ALTER PUBLICATION r0c_pgd_publication_p DROP TABLE r0c_pgd_publication;
ALTER PUBLICATION r0c_pgd_publication_p ADD TABLE r0c_pgd_publication;
ALTER PUBLICATION r0c_pgd_publication_p RENAME TO r0c_pgd_publication_new;
ALTER PUBLICATION r0c_pgd_publication_new OWNER TO postgres;
SELECT pubname,pubinsert,pubupdate,pubdelete FROM pg_publication WHERE pubname='r0c_pgd_publication_new';
DROP PUBLICATION r0c_pgd_publication_new;
