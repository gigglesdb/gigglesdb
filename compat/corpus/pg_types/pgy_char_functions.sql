-- case: pgy_char_functions
-- title: char functions
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: string functions on char(n); types.txt / Character types
CREATE TABLE r0c_pgy_char_functions (id int PRIMARY KEY,v char(5));
INSERT INTO r0c_pgy_char_functions VALUES (1,'ab');
SELECT v,length(v),octet_length(v),lower(v),upper(v),substring(v FROM 1 FOR 2),trim(v),v || 'x',replace(v,'a','z') FROM r0c_pgy_char_functions ORDER BY id;
