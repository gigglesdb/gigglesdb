-- case: pgf_string_substrings
-- title: Position split and substring syntaxes
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-string.html
CREATE TABLE r0c_pgf_string_substrings (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_substrings VALUES (1,'alpha:beta:gamma'),(2,'abc'),(3,NULL);
SELECT id, split_part(s,':',2) FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, split_part(s,':',-1) FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, strpos(s,'b') FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, position('b' IN s) FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, substr(s,2,3) FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, substring(s FROM 2 FOR 3) FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, substring(s FROM '[a-z]+') FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, substring(s SIMILAR '%#"beta#"%' ESCAPE '#') FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, substr('abcdef',-1,4) FROM r0c_pgf_string_substrings ORDER BY id;
SELECT id, substring('abcdef' FROM 2) FROM r0c_pgf_string_substrings ORDER BY id;
DROP TABLE r0c_pgf_string_substrings;
