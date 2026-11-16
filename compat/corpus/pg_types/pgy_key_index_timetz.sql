-- case: pgy_key_index_timetz
-- title: key index timetz
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_timetz (v timetz PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_timetz VALUES ('12:00+00');
CREATE UNIQUE INDEX r0c_pgy_key_index_timetz_idx ON r0c_pgy_key_index_timetz(v);
SELECT v FROM r0c_pgy_key_index_timetz ORDER BY v;
