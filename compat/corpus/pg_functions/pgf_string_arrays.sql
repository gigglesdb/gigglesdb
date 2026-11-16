-- case: pgf_string_arrays
-- title: String array conversion and NULL tokens
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-string.html
CREATE TABLE r0c_pgf_string_arrays (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_arrays VALUES (1,'a,NULL,b'),(2,''),(3,NULL);
SELECT id, string_to_array(s,',') FROM r0c_pgf_string_arrays ORDER BY id;
SELECT id, string_to_array(s,',','NULL') FROM r0c_pgf_string_arrays ORDER BY id;
SELECT id, string_to_array(s,NULL) FROM r0c_pgf_string_arrays ORDER BY id;
SELECT id, array_to_string(ARRAY[s,'x',NULL],':') FROM r0c_pgf_string_arrays ORDER BY id;
SELECT id, array_to_string(ARRAY[s,'x',NULL],':','missing') FROM r0c_pgf_string_arrays ORDER BY id;
SELECT id, string_to_array('ab','') FROM r0c_pgf_string_arrays ORDER BY id;
SELECT id, array_to_string(ARRAY[]::text[],',') FROM r0c_pgf_string_arrays ORDER BY id;
DROP TABLE r0c_pgf_string_arrays;
