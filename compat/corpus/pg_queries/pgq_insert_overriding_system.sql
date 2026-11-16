-- case: pgq_insert_overriding_system
-- title: insert overriding system
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_insert_overriding_system (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label text);
INSERT INTO r0c_pgq_insert_overriding_system OVERRIDING SYSTEM VALUE VALUES (10,'imported') RETURNING id,label;
