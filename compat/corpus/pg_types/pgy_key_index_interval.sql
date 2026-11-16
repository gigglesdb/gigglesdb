-- case: pgy_key_index_interval
-- title: key index interval
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_interval (v interval PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_interval VALUES ('1 day');
CREATE UNIQUE INDEX r0c_pgy_key_index_interval_idx ON r0c_pgy_key_index_interval(v);
SELECT v FROM r0c_pgy_key_index_interval ORDER BY v;
