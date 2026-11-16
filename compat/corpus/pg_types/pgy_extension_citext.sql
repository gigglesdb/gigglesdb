-- case: pgy_extension_citext
-- title: extension citext
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE SCHEMA r0c_pgy_extension_citext_s;
CREATE EXTENSION citext WITH SCHEMA r0c_pgy_extension_citext_s;
CREATE TABLE r0c_pgy_extension_citext (id int PRIMARY KEY,v r0c_pgy_extension_citext_s.citext);
INSERT INTO r0c_pgy_extension_citext VALUES (1,'Hello');
SET search_path = r0c_pgy_extension_citext_s,pg_catalog;
SELECT v,v = 'hello' FROM public.r0c_pgy_extension_citext ORDER BY id;
RESET search_path;
DROP TABLE r0c_pgy_extension_citext;
DROP EXTENSION citext;
DROP SCHEMA r0c_pgy_extension_citext_s;
