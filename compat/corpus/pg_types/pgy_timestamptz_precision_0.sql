-- case: pgy_timestamptz_precision_0
-- title: timestamptz precision 0
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_timestamptz_precision_0 (id int PRIMARY KEY,v timestamptz(0));
INSERT INTO r0c_pgy_timestamptz_precision_0 VALUES (1,'2024-01-02 12:34:56.123456+00');
SELECT v FROM r0c_pgy_timestamptz_precision_0 ORDER BY id;
