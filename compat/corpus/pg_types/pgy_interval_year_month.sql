-- case: pgy_interval_year_month
-- title: interval year month
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: interval precision and field qualifiers; types.txt / Date and time types
CREATE TABLE r0c_pgy_interval_year_month (id int PRIMARY KEY,v interval year to month);
INSERT INTO r0c_pgy_interval_year_month VALUES (1,'2 years 3 months 4 days');
SELECT v,extract(epoch FROM v) FROM r0c_pgy_interval_year_month ORDER BY id;
