-- case: pgf_string_edit
-- title: Text slicing padding replacement and case
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-string.html
CREATE TABLE r0c_pgf_string_edit (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_edit VALUES (1,'hello world'),(2,U&'\00E9clair'),(3,NULL);
SELECT id, left(s,3) FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, right(s,-2) FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, lpad(s,15,'xy') FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, rpad(s,15,'.') FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, repeat(s,2) FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, replace(s,'l','LL') FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, reverse(s) FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, translate(s,'el','EL') FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, initcap(s) FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, overlay(s PLACING 'X' FROM 2 FOR 2) FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, lower(s) FROM r0c_pgf_string_edit ORDER BY id;
SELECT id, upper(s) FROM r0c_pgf_string_edit ORDER BY id;
DROP TABLE r0c_pgf_string_edit;
