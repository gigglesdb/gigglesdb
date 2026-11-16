-- case: pgf_system_view
-- title: View definition pretty argument
-- owner: DD-0068
-- expect: compatible
-- source: functions.txt pg_get_viewdef gap; https://www.postgresql.org/docs/18/functions-info.html
CREATE TABLE r0c_pgf_system_view (id integer PRIMARY KEY, s text);
CREATE VIEW r0c_pgf_system_view_v AS SELECT id,s FROM r0c_pgf_system_view WHERE id>0;
SELECT pg_get_viewdef('r0c_pgf_system_view_v'::regclass);
SELECT pg_get_viewdef('r0c_pgf_system_view_v'::regclass,true);
SELECT pg_get_viewdef('r0c_pgf_system_view_v'::regclass,false);
DROP VIEW r0c_pgf_system_view_v;
DROP TABLE r0c_pgf_system_view;
