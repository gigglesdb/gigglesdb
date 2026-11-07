-- case: error_sqlstate
-- title: Duplicate primary key reports a SQLSTATE and allows subsequent queries
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_error_sqlstate_t (id int primary key);
INSERT INTO r0c_error_sqlstate_t VALUES (1);
-- @error
-- expect-sqlstate: 23505
INSERT INTO r0c_error_sqlstate_t VALUES (1);
SELECT count(*) FROM r0c_error_sqlstate_t;
