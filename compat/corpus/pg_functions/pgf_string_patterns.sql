-- case: pgf_string_patterns
-- title: LIKE escapes similar and POSIX operators
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-matching.html
CREATE TABLE r0c_pgf_string_patterns (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_patterns VALUES (1,'Abc_12'),(2,'a%z'),(3,NULL);
SELECT id, s LIKE 'Abc!_%' ESCAPE '!' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, s ILIKE 'abc!_%' ESCAPE '!' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, s NOT LIKE 'x%' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, s SIMILAR TO '(Abc|a)%' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, s ~ '[0-9]+' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, s ~* '^abc' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, s !~ '^z' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, s !~* '^ABC' FROM r0c_pgf_string_patterns ORDER BY id;
SELECT id, 'a_b' LIKE 'a!_b' ESCAPE '!' FROM r0c_pgf_string_patterns ORDER BY id;
DROP TABLE r0c_pgf_string_patterns;
