-- case: pgq_insert_default_values
-- title: insert default values
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_insert_default_values (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label text DEFAULT 'draft');
INSERT INTO r0c_pgq_insert_default_values DEFAULT VALUES RETURNING id,label;
