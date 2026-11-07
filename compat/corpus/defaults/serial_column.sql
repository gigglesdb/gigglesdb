-- case: serial_column
-- title: Generate identifiers with serial
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_serial_column_t (id serial PRIMARY KEY, name text);
INSERT INTO r0c_serial_column_t (name) VALUES ('Ada');
INSERT INTO r0c_serial_column_t (name) VALUES ('Bea');
SELECT id, name FROM r0c_serial_column_t ORDER BY id;
