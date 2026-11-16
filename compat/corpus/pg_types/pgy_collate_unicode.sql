-- case: pgy_collate_unicode
-- title: collate unicode
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: COLLATE; W5 CREATE/ALTER/DROP COLLATION
CREATE TABLE r0c_pgy_collate_unicode (id int PRIMARY KEY,v text COLLATE "unicode");
INSERT INTO r0c_pgy_collate_unicode VALUES (1,'z'),(2,'a'),(3,'A');
SELECT id,v,v COLLATE "unicode" < 'b' COLLATE "unicode" FROM r0c_pgy_collate_unicode ORDER BY v COLLATE "unicode",id;
