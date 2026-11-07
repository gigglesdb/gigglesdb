-- case: not_null_violation
-- title: Not null violation
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_not_null_violation_t (id int PRIMARY KEY, name text NOT NULL);
-- @error
-- expect-sqlstate: 23502
INSERT INTO r0c_not_null_violation_t VALUES (1, NULL);
SELECT count(*) FROM r0c_not_null_violation_t;
