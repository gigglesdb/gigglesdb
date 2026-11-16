-- case: pgs_viewdef
-- title: View definition default
-- owner: G
-- expect: compatible
-- source: Differences: Read first / pg_get_viewdef
CREATE TABLE r0c_pgs_viewdef_t (id int PRIMARY KEY, v text);
CREATE VIEW r0c_pgs_viewdef AS SELECT id, v FROM r0c_pgs_viewdef_t;
SELECT pg_get_viewdef('r0c_pgs_viewdef');
SELECT pg_get_viewdef('r0c_pgs_viewdef'::regclass);
DROP VIEW r0c_pgs_viewdef;
CREATE VIEW r0c_pgs_viewdef AS SELECT id, v AS label FROM r0c_pgs_viewdef_t WHERE id > 0;
SELECT pg_get_viewdef('r0c_pgs_viewdef'::regclass);
DROP VIEW r0c_pgs_viewdef;
