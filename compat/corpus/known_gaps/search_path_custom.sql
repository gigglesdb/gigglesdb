-- case: search_path_custom
-- title: Search path custom
-- owner: D
-- expect: compatible
CREATE SCHEMA r0c_sp;
CREATE TABLE r0c_sp.r0c_search_path_custom_t (id int);
SET search_path TO r0c_sp, public;
INSERT INTO r0c_search_path_custom_t VALUES (1);
SELECT id FROM r0c_search_path_custom_t;
SET search_path TO public;
