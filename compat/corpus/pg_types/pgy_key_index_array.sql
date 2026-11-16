-- case: pgy_key_index_array
-- title: key index array
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_array (v integer[] PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_array VALUES ('{1,2}');
CREATE UNIQUE INDEX r0c_pgy_key_index_array_idx ON r0c_pgy_key_index_array(v);
SELECT v FROM r0c_pgy_key_index_array ORDER BY v;
