-- case: pgs_array_typed
-- title: array concatenation with typed
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / || between array column and string literal
CREATE TABLE r0c_pgs_array_typed (id int PRIMARY KEY, v text[]);
INSERT INTO r0c_pgs_array_typed VALUES (1, '{a,b}');
SELECT v || '{c}'::text[] AS merged, pg_typeof(v || '{c}'::text[]) AS result_type FROM r0c_pgs_array_typed;
