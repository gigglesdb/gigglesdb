-- case: pgy_interval_day_second
-- title: interval day second
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_day_second (id int PRIMARY KEY,v interval day to second(2));
INSERT INTO r0c_pgy_interval_day_second VALUES (1,'1 day 02:03:04.5678');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_day_second ORDER BY id;
