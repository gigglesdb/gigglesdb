-- case: pgy_cast_timestamptz_to_timetz
-- title: cast timestamptz to timetz
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_timestamptz_to_timetz (id int PRIMARY KEY,v timestamptz);
INSERT INTO r0c_pgy_cast_timestamptz_to_timetz VALUES (1,'2024-01-02 12:34:56.123456+00');
SELECT v::timetz FROM r0c_pgy_cast_timestamptz_to_timetz ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_timestamptz_to_timetz;
