-- case: pgs_array_element
-- title: Array concatenation with typed scalar
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Array concatenation with typed scalar
CREATE TABLE r0c_pgs_array_element (id int PRIMARY KEY, v text[]);
INSERT INTO r0c_pgs_array_element VALUES (1, '{a,b}');
SELECT id, v || 'c'::text FROM r0c_pgs_array_element ORDER BY id;
SELECT id, 'z'::text || v FROM r0c_pgs_array_element ORDER BY id;
