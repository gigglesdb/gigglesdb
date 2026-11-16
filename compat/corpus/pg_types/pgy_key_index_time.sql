-- case: pgy_key_index_time
-- title: key index time
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_time (v time PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_time VALUES ('12:00');
CREATE UNIQUE INDEX r0c_pgy_key_index_time_idx ON r0c_pgy_key_index_time(v);
SELECT v FROM r0c_pgy_key_index_time ORDER BY v;
