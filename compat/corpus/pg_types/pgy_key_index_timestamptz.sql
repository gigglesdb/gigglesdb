-- case: pgy_key_index_timestamptz
-- title: key index timestamptz
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_timestamptz (v timestamptz PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_timestamptz VALUES ('2024-01-02 12:00+00');
CREATE UNIQUE INDEX r0c_pgy_key_index_timestamptz_idx ON r0c_pgy_key_index_timestamptz(v);
SELECT v FROM r0c_pgy_key_index_timestamptz ORDER BY v;
