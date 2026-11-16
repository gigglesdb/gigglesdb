-- case: pgy_cast_date_to_timetz
-- title: cast date to timetz
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_date_to_timetz (id int PRIMARY KEY,v date);
INSERT INTO r0c_pgy_cast_date_to_timetz VALUES (1,'2024-01-02');
-- @error
SELECT v::timetz FROM r0c_pgy_cast_date_to_timetz ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_date_to_timetz;
