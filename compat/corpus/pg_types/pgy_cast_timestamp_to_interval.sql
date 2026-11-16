-- case: pgy_cast_timestamp_to_interval
-- title: cast timestamp to interval
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_timestamp_to_interval (id int PRIMARY KEY,v timestamp);
INSERT INTO r0c_pgy_cast_timestamp_to_interval VALUES (1,'2024-01-02 12:34:56.123456');
-- @error
SELECT v::interval FROM r0c_pgy_cast_timestamp_to_interval ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_timestamp_to_interval;
