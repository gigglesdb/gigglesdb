-- case: pgy_timestamptz_precision_7
-- title: timestamptz precision 7
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_timestamptz_precision_7 (id int PRIMARY KEY,v timestamptz(7));
INSERT INTO r0c_pgy_timestamptz_precision_7 VALUES (1,'2024-01-02 12:34:56.123456+00');
SELECT v FROM r0c_pgy_timestamptz_precision_7 ORDER BY id;
