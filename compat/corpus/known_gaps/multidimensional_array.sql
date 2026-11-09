-- case: multidimensional_array
-- title: Arrays with more than one dimension
-- owner: E2
-- expect: intentional Arrays with more than one dimension
CREATE TABLE r0c_multidimensional_array (id int PRIMARY KEY, v int[]);
-- expect-sqlstate: 22P02
INSERT INTO r0c_multidimensional_array VALUES (1, '{{1,2},{3,4}}');
