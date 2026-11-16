-- case: pgq_truncate_continue_identity
-- title: truncate continue identity
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 TRUNCATE identity
CREATE TABLE r0c_pgq_truncate_continue_identity (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY);
INSERT INTO r0c_pgq_truncate_continue_identity DEFAULT VALUES;
TRUNCATE r0c_pgq_truncate_continue_identity CONTINUE IDENTITY;
INSERT INTO r0c_pgq_truncate_continue_identity DEFAULT VALUES RETURNING id;
