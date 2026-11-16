-- case: pgy_interval_month
-- title: interval month
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_month (id int PRIMARY KEY,v interval month);
INSERT INTO r0c_pgy_interval_month VALUES (1,'2 years 3 months');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_month ORDER BY id;
