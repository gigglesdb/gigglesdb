-- case: pgy_interval_hour
-- title: interval hour
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_hour (id int PRIMARY KEY,v interval hour);
INSERT INTO r0c_pgy_interval_hour VALUES (1,'2 days 03:04:05');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_hour ORDER BY id;
