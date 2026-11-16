-- case: pgy_cast_timetz_to_date
-- title: cast timetz to date
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_timetz_to_date (id int PRIMARY KEY,v timetz);
INSERT INTO r0c_pgy_cast_timetz_to_date VALUES (1,'12:34:56.123456+00');
-- @error
SELECT v::date FROM r0c_pgy_cast_timetz_to_date ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_timetz_to_date;
