-- case: pgy_key_index_double
-- title: key index double
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_double (v double precision PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_double VALUES (1.2);
CREATE UNIQUE INDEX r0c_pgy_key_index_double_idx ON r0c_pgy_key_index_double(v);
SELECT v FROM r0c_pgy_key_index_double ORDER BY v;
