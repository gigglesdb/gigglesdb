-- case: pgy_time_precision_0
-- title: time precision 0
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_time_precision_0 (id int PRIMARY KEY,v time(0));
INSERT INTO r0c_pgy_time_precision_0 VALUES (1,'12:34:56.123456');
SELECT v FROM r0c_pgy_time_precision_0 ORDER BY id;
