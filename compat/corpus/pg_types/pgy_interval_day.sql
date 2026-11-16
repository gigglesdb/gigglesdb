-- case: pgy_interval_day
-- title: interval day
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_day (id int PRIMARY KEY,v interval day);
INSERT INTO r0c_pgy_interval_day VALUES (1,'2 days 03:04:05');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_day ORDER BY id;
