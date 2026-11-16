-- case: pgy_interval_year
-- title: interval year
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_year (id int PRIMARY KEY,v interval year);
INSERT INTO r0c_pgy_interval_year VALUES (1,'2 years 3 months');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_year ORDER BY id;
