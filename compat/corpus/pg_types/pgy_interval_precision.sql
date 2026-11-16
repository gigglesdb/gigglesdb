-- case: pgy_interval_precision
-- title: interval precision
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_precision (id int PRIMARY KEY,v interval(3));
INSERT INTO r0c_pgy_interval_precision VALUES (1,'1.123456 seconds');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_precision ORDER BY id;
