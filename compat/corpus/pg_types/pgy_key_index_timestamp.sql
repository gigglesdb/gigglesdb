-- case: pgy_key_index_timestamp
-- title: key index timestamp
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_timestamp (v timestamp PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_timestamp VALUES ('2024-01-02 12:00');
CREATE UNIQUE INDEX r0c_pgy_key_index_timestamp_idx ON r0c_pgy_key_index_timestamp(v);
SELECT v FROM r0c_pgy_key_index_timestamp ORDER BY v;
