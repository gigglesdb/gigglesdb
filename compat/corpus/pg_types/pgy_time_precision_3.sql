-- case: pgy_time_precision_3
-- title: time precision 3
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_time_precision_3 (id int PRIMARY KEY,v time(3));
INSERT INTO r0c_pgy_time_precision_3 VALUES (1,'12:34:56.123456');
SELECT v FROM r0c_pgy_time_precision_3 ORDER BY id;
