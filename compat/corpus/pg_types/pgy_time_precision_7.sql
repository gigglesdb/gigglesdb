-- case: pgy_time_precision_7
-- title: time precision 7
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_time_precision_7 (id int PRIMARY KEY,v time(7));
INSERT INTO r0c_pgy_time_precision_7 VALUES (1,'12:34:56.123456');
SELECT v FROM r0c_pgy_time_precision_7 ORDER BY id;
