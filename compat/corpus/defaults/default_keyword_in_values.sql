-- case: default_keyword_in_values
-- title: Apply DEFAULT in values and default values insertion
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_default_keyword_in_values_t (row_id serial PRIMARY KEY, id integer DEFAULT 7, name text DEFAULT 'fallback');
INSERT INTO r0c_default_keyword_in_values_t (id, name) VALUES (DEFAULT, 'x');
INSERT INTO r0c_default_keyword_in_values_t DEFAULT VALUES;
SELECT id, name FROM r0c_default_keyword_in_values_t ORDER BY id, name;
