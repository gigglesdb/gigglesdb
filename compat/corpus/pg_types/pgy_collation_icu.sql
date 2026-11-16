-- case: pgy_collation_icu
-- title: collation icu
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: COLLATE; W5 CREATE/ALTER/DROP COLLATION
CREATE COLLATION r0c_pgy_collation_icu_c (provider=icu,locale='und',deterministic=false);
CREATE TABLE r0c_pgy_collation_icu (id int PRIMARY KEY,v text COLLATE r0c_pgy_collation_icu_c);
INSERT INTO r0c_pgy_collation_icu VALUES (1,U&'\00E9'),(2,U&'e\0301');
SELECT a.v=b.v,a.v COLLATE r0c_pgy_collation_icu_c=b.v COLLATE r0c_pgy_collation_icu_c FROM r0c_pgy_collation_icu a JOIN r0c_pgy_collation_icu b ON a.id=1 AND b.id=2;
DROP TABLE r0c_pgy_collation_icu;
DROP COLLATION r0c_pgy_collation_icu_c;
