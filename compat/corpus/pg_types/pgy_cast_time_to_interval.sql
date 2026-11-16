-- case: pgy_cast_time_to_interval
-- title: cast time to interval
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_time_to_interval (id int PRIMARY KEY,v time);
INSERT INTO r0c_pgy_cast_time_to_interval VALUES (1,'12:34:56.123456');
SELECT v::interval FROM r0c_pgy_cast_time_to_interval ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_time_to_interval;
