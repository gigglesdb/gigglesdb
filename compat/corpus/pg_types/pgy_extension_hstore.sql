-- case: pgy_extension_hstore
-- title: extension hstore
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE SCHEMA r0c_pgy_extension_hstore_s;
CREATE EXTENSION hstore WITH SCHEMA r0c_pgy_extension_hstore_s;
CREATE TABLE r0c_pgy_extension_hstore (id int PRIMARY KEY,v r0c_pgy_extension_hstore_s.hstore);
INSERT INTO r0c_pgy_extension_hstore VALUES (1,'a=>1');
SET search_path = r0c_pgy_extension_hstore_s,pg_catalog;
SELECT v,v -> 'a' FROM public.r0c_pgy_extension_hstore ORDER BY id;
RESET search_path;
DROP TABLE r0c_pgy_extension_hstore;
DROP EXTENSION hstore;
DROP SCHEMA r0c_pgy_extension_hstore_s;
