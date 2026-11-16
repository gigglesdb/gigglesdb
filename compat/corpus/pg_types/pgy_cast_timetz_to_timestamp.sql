-- case: pgy_cast_timetz_to_timestamp
-- title: cast timetz to timestamp
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_timetz_to_timestamp (id int PRIMARY KEY,v timetz);
INSERT INTO r0c_pgy_cast_timetz_to_timestamp VALUES (1,'12:34:56.123456+00');
-- @error
SELECT v::timestamp FROM r0c_pgy_cast_timetz_to_timestamp ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_timetz_to_timestamp;
