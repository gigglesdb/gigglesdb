-- case: insert_column_subset_nulls
-- title: Fill omitted nullable columns with null
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_insert_column_subset_nulls_t (id integer, name text, score integer);
INSERT INTO r0c_insert_column_subset_nulls_t (id, name) VALUES (1, 'Ada');
INSERT INTO r0c_insert_column_subset_nulls_t (id, score) VALUES (2, 20);
SELECT id, name, score FROM r0c_insert_column_subset_nulls_t ORDER BY id;
