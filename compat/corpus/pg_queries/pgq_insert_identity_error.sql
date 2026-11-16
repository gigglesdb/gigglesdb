-- case: pgq_insert_identity_error
-- title: insert identity error
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_insert_identity_error (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY);
-- @error
INSERT INTO r0c_pgq_insert_identity_error VALUES (10);
