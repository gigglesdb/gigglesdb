-- case: information_schema_columns
-- title: Information schema columns
-- owner: G
-- expect: compatible
CREATE TABLE r0c_information_schema_columns_t (id int NOT NULL, label text);
SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'r0c_information_schema_columns_t' ORDER BY ordinal_position;
