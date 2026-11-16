-- case: pgy_key_index_real
-- title: key index real
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_real (v real PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_real VALUES (1.2);
CREATE UNIQUE INDEX r0c_pgy_key_index_real_idx ON r0c_pgy_key_index_real(v);
SELECT v FROM r0c_pgy_key_index_real ORDER BY v;
