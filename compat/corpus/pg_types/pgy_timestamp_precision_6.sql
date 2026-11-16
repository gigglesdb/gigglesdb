-- case: pgy_timestamp_precision_6
-- title: timestamp precision 6
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_timestamp_precision_6 (id int PRIMARY KEY,v timestamp(6));
INSERT INTO r0c_pgy_timestamp_precision_6 VALUES (1,'2024-01-02 12:34:56.123456');
SELECT v FROM r0c_pgy_timestamp_precision_6 ORDER BY id;
