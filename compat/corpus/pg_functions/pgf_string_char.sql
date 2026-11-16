-- case: pgf_string_char
-- title: Character arguments and trim variants
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-string.html
CREATE TABLE r0c_pgf_string_char (id integer PRIMARY KEY, s char(8));
INSERT INTO r0c_pgf_string_char VALUES (1,' abc '),(2,'xy'),(3,NULL);
SELECT id, length(s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, lower(s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, upper(s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, left(s,2) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, trim(BOTH ' ' FROM s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, trim(LEADING ' ' FROM s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, trim(TRAILING ' ' FROM s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, btrim(s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, ltrim(s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, rtrim(s) FROM r0c_pgf_string_char ORDER BY id;
SELECT id, lpad(s,10,'-') FROM r0c_pgf_string_char ORDER BY id;
SELECT id, reverse(s) FROM r0c_pgf_string_char ORDER BY id;
DROP TABLE r0c_pgf_string_char;
