-- case: pgf_string_regex_scalar
-- title: Regular expression scalar matching and replacement
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-matching.html
CREATE TABLE r0c_pgf_string_regex_scalar (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_regex_scalar VALUES (1,'a12b34'),(2,''),(3,NULL);
SELECT id, regexp_match(s,'([0-9]+)') FROM r0c_pgf_string_regex_scalar ORDER BY id;
SELECT id, regexp_replace(s,'[0-9]+','X','g') FROM r0c_pgf_string_regex_scalar ORDER BY id;
SELECT id, regexp_count(s,'[0-9]+') FROM r0c_pgf_string_regex_scalar ORDER BY id;
SELECT id, regexp_instr(s,'[0-9]+') FROM r0c_pgf_string_regex_scalar ORDER BY id;
SELECT id, regexp_like(s,'^A','i') FROM r0c_pgf_string_regex_scalar ORDER BY id;
SELECT id, regexp_substr(s,'[0-9]+',1,2) FROM r0c_pgf_string_regex_scalar ORDER BY id;
SELECT id, regexp_split_to_array(s,'[0-9]+') FROM r0c_pgf_string_regex_scalar ORDER BY id;
SELECT id, regexp_replace('abc123','([a-z]+)([0-9]+)',E'\\2-\\1') FROM r0c_pgf_string_regex_scalar ORDER BY id;
DROP TABLE r0c_pgf_string_regex_scalar;
