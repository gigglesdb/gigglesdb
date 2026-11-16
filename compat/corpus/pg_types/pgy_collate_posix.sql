-- case: pgy_collate_posix
-- title: collate posix
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: COLLATE; W5 CREATE/ALTER/DROP COLLATION
CREATE TABLE r0c_pgy_collate_posix (id int PRIMARY KEY,v text COLLATE "POSIX");
INSERT INTO r0c_pgy_collate_posix VALUES (1,'z'),(2,'a'),(3,'A');
SELECT id,v,v COLLATE "POSIX" < 'b' COLLATE "POSIX" FROM r0c_pgy_collate_posix ORDER BY v COLLATE "POSIX",id;
