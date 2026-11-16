-- case: pgs_collation
-- title: Text ordering with explicit ICU collation
-- owner: E2
-- expect: compatible
-- source: Differences: Read first / Text ordering with explicit ICU collation
CREATE TABLE r0c_pgs_collation (id int PRIMARY KEY, v text);
INSERT INTO r0c_pgs_collation VALUES (1, 'z'), (2, 'a'), (3, '?'), (4, 'Z'), (5, 'A');
SELECT v FROM r0c_pgs_collation ORDER BY v COLLATE "und-x-icu", v COLLATE "C";
SELECT '?' COLLATE "und-x-icu" < 'z' COLLATE "und-x-icu";
