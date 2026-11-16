-- case: pgy_char_numeric_cast
-- title: char numeric cast
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: string functions on char(n); types.txt / Character types
CREATE TABLE r0c_pgy_char_numeric_cast (id int PRIMARY KEY,v char(4));
INSERT INTO r0c_pgy_char_numeric_cast VALUES (1,'12');
SELECT v::integer,v::numeric,v::varchar,v::text FROM r0c_pgy_char_numeric_cast ORDER BY id;
