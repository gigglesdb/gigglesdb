-- case: pgy_varchar_length
-- title: varchar length
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_varchar_length (id int PRIMARY KEY,v varchar(3));
INSERT INTO r0c_pgy_varchar_length VALUES (1,'abc  '),(2,'??');
-- @error
INSERT INTO r0c_pgy_varchar_length VALUES (3,'abcd');
SELECT id,v,length(v),octet_length(v) FROM r0c_pgy_varchar_length ORDER BY id;
SELECT 'abcdef'::varchar(3);
