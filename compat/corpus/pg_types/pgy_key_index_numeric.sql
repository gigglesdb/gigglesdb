-- case: pgy_key_index_numeric
-- title: key index numeric
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_numeric (v numeric PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_numeric VALUES (1.2);
CREATE UNIQUE INDEX r0c_pgy_key_index_numeric_idx ON r0c_pgy_key_index_numeric(v);
SELECT v FROM r0c_pgy_key_index_numeric ORDER BY v;
