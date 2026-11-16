-- case: pgy_key_index_jsonb
-- title: key index jsonb
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_jsonb (v jsonb PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_jsonb VALUES ('{"x":1}');
CREATE UNIQUE INDEX r0c_pgy_key_index_jsonb_idx ON r0c_pgy_key_index_jsonb(v);
SELECT v FROM r0c_pgy_key_index_jsonb ORDER BY v;
