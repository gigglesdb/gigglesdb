-- case: pgf_array_edit
-- title: Array constructors and element modification
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-array.html
CREATE TABLE r0c_pgf_array_edit (id integer PRIMARY KEY, a integer[]);
INSERT INTO r0c_pgf_array_edit VALUES (1,'{1,2,2,NULL}'),(2,'{}'),(3,NULL);
SELECT id, ARRAY[1,2,NULL] FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, ARRAY[]::integer[] FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_append(a,3) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_prepend(0,a) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_cat(a,ARRAY[8,9]) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_remove(a,2) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_replace(a,2,7) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_position(a,2) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_positions(a,2) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, array_position(a,NULL) FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, a || ARRAY[5] FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, a || 5 FROM r0c_pgf_array_edit ORDER BY id;
SELECT id, 0 || a FROM r0c_pgf_array_edit ORDER BY id;
DROP TABLE r0c_pgf_array_edit;
