-- case: pgy_cast_timestamp_to_timestamptz
-- title: cast timestamp to timestamptz
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_timestamp_to_timestamptz (id int PRIMARY KEY,v timestamp);
INSERT INTO r0c_pgy_cast_timestamp_to_timestamptz VALUES (1,'2024-01-02 12:34:56.123456');
SELECT v::timestamptz FROM r0c_pgy_cast_timestamp_to_timestamptz ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_timestamp_to_timestamptz;
