-- case: pgy_cast_timetz_to_timestamptz
-- title: cast timetz to timestamptz
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_timetz_to_timestamptz (id int PRIMARY KEY,v timetz);
INSERT INTO r0c_pgy_cast_timetz_to_timestamptz VALUES (1,'12:34:56.123456+00');
-- @error
SELECT v::timestamptz FROM r0c_pgy_cast_timetz_to_timestamptz ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_timetz_to_timestamptz;
