-- case: pgq_truncate_restart_identity
-- title: truncate restart identity
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 TRUNCATE identity
CREATE TABLE r0c_pgq_truncate_restart_identity (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY);
INSERT INTO r0c_pgq_truncate_restart_identity DEFAULT VALUES;
TRUNCATE r0c_pgq_truncate_restart_identity RESTART IDENTITY;
INSERT INTO r0c_pgq_truncate_restart_identity DEFAULT VALUES RETURNING id;
