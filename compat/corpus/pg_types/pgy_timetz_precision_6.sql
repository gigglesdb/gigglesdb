-- case: pgy_timetz_precision_6
-- title: timetz precision 6
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_timetz_precision_6 (id int PRIMARY KEY,v timetz(6));
INSERT INTO r0c_pgy_timetz_precision_6 VALUES (1,'12:34:56.123456+00');
SELECT v FROM r0c_pgy_timetz_precision_6 ORDER BY id;
