-- case: bigserial_column
-- title: Generate identifiers with bigserial
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_bigserial_column_t (id bigserial PRIMARY KEY, name text);
INSERT INTO r0c_bigserial_column_t (name) VALUES ('Ada');
INSERT INTO r0c_bigserial_column_t (name) VALUES ('Bea');
SELECT id, name FROM r0c_bigserial_column_t ORDER BY id;
