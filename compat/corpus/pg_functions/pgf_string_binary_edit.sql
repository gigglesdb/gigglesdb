-- case: pgf_string_binary_edit
-- title: Binary substring trimming and reverse
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-binarystring.html
CREATE TABLE r0c_pgf_string_binary_edit (id integer PRIMARY KEY, b bytea);
INSERT INTO r0c_pgf_string_binary_edit VALUES (1,decode('0061626300','hex')),(2,decode('6162','hex')),(3,NULL);
SELECT id, length(b) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, octet_length(b) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, substring(b FROM 2 FOR 2) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, substr(b,2,2) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, position('\x61'::bytea IN b) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, btrim(b,'\x00'::bytea) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, ltrim(b,'\x00'::bytea) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, rtrim(b,'\x00'::bytea) FROM r0c_pgf_string_binary_edit ORDER BY id;
SELECT id, reverse(b) FROM r0c_pgf_string_binary_edit ORDER BY id;
DROP TABLE r0c_pgf_string_binary_edit;
