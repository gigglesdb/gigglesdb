-- case: pgy_cast_interval_to_timetz
-- title: cast interval to timetz
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_interval_to_timetz (id int PRIMARY KEY,v interval);
INSERT INTO r0c_pgy_cast_interval_to_timetz VALUES (1,'1 day 12:34:56.123456');
-- @error
SELECT v::timetz FROM r0c_pgy_cast_interval_to_timetz ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_interval_to_timetz;
