-- case: pgy_collate_c
-- title: collate c
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: COLLATE; W5 CREATE/ALTER/DROP COLLATION
CREATE TABLE r0c_pgy_collate_c (id int PRIMARY KEY,v text COLLATE "C");
INSERT INTO r0c_pgy_collate_c VALUES (1,'z'),(2,'a'),(3,'A');
SELECT id,v,v COLLATE "C" < 'b' COLLATE "C" FROM r0c_pgy_collate_c ORDER BY v COLLATE "C",id;
