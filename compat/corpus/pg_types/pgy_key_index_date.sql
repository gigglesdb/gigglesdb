-- case: pgy_key_index_date
-- title: key index date
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_date (v date PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_date VALUES ('2024-01-02');
CREATE UNIQUE INDEX r0c_pgy_key_index_date_idx ON r0c_pgy_key_index_date(v);
SELECT v FROM r0c_pgy_key_index_date ORDER BY v;
