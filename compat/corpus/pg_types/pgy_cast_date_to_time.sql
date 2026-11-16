-- case: pgy_cast_date_to_time
-- title: cast date to time
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: casts between date and time types; types.txt / Casts
CREATE TABLE r0c_pgy_cast_date_to_time (id int PRIMARY KEY,v date);
INSERT INTO r0c_pgy_cast_date_to_time VALUES (1,'2024-01-02');
-- @error
SELECT v::time FROM r0c_pgy_cast_date_to_time ORDER BY id;
SELECT count(*) FROM r0c_pgy_cast_date_to_time;
