-- case: pgs_array_literal
-- title: array concatenation with literal
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / || between array column and string literal
CREATE TABLE r0c_pgs_array_literal (id int PRIMARY KEY, v text[]);
INSERT INTO r0c_pgs_array_literal VALUES (1, '{a,b}');
SELECT id, v || '{c}' FROM r0c_pgs_array_literal ORDER BY id;
SELECT id, pg_typeof(v || '{c}') AS result_type FROM r0c_pgs_array_literal ORDER BY id;
