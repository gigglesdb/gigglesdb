-- case: create_table_without_primary_key
-- title: Create and query a table without a primary key
-- owner: F
-- expect: compatible
-- primary-key exception: Absence of a primary key is the feature under test.
CREATE TABLE r0c_create_table_without_primary_key_t (id int, name text);
INSERT INTO r0c_create_table_without_primary_key_t VALUES (1, 'Ada');
INSERT INTO r0c_create_table_without_primary_key_t VALUES (2, 'Bea');
SELECT id, name FROM r0c_create_table_without_primary_key_t WHERE name <> 'missing' ORDER BY id;
