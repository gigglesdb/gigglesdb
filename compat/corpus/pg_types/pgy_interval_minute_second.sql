-- case: pgy_interval_minute_second
-- title: interval minute second
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_minute_second (id int PRIMARY KEY,v interval minute to second(3));
INSERT INTO r0c_pgy_interval_minute_second VALUES (1,'2 days 03:04:05.6789');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_minute_second ORDER BY id;
