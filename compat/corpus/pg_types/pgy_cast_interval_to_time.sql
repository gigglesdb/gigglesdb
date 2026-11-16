-- case: pgy_cast_interval_to_time
-- title: cast interval to time
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_interval_to_time (id int PRIMARY KEY,v interval);
INSERT INTO r0c_pgy_cast_interval_to_time VALUES (1,'1 day 12:34:56.123456');
SELECT v::time FROM r0c_pgy_cast_interval_to_time ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_interval_to_time;
