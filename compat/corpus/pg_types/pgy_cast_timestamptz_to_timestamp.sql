-- case: pgy_cast_timestamptz_to_timestamp
-- title: cast timestamptz to timestamp
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_timestamptz_to_timestamp (id int PRIMARY KEY,v timestamptz);
INSERT INTO r0c_pgy_cast_timestamptz_to_timestamp VALUES (1,'2024-01-02 12:34:56.123456+00');
SELECT v::timestamp FROM r0c_pgy_cast_timestamptz_to_timestamp ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_timestamptz_to_timestamp;
