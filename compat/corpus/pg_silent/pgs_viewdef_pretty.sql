-- case: pgs_viewdef_pretty
-- title: View definition pretty
-- owner: G
-- expect: compatible
-- source: Differences: Read first / pg_get_viewdef
CREATE TABLE r0c_pgs_viewdef_pretty_t (id int PRIMARY KEY, v text);
CREATE VIEW r0c_pgs_viewdef_pretty AS SELECT id, v FROM r0c_pgs_viewdef_pretty_t;
SELECT pg_get_viewdef('r0c_pgs_viewdef_pretty', true);
SELECT pg_get_viewdef('r0c_pgs_viewdef_pretty'::regclass, true);
DROP VIEW r0c_pgs_viewdef_pretty;
CREATE VIEW r0c_pgs_viewdef_pretty AS SELECT id, v AS label FROM r0c_pgs_viewdef_pretty_t WHERE id > 0;
SELECT pg_get_viewdef('r0c_pgs_viewdef_pretty'::regclass, true);
DROP VIEW r0c_pgs_viewdef_pretty;
