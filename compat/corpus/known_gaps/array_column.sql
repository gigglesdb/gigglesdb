-- case: array_column
-- title: Array column
-- owner: E2
-- expect: compatible
CREATE TABLE r0c_array_column_t (arr int[]);
INSERT INTO r0c_array_column_t VALUES ('{1,2,3}');
SELECT arr[2], array_length(arr,1) FROM r0c_array_column_t;
