-- case: check_violation
-- title: Check violation
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_check_violation_t (id int PRIMARY KEY, score int CHECK (score >= 0), limit_value int, CONSTRAINT r0c_check_violation_uq CHECK (limit_value <= 10));
-- @error
-- expect-sqlstate: 23514
INSERT INTO r0c_check_violation_t VALUES (1, -1, 5);
-- @error
-- expect-sqlstate: 23514
INSERT INTO r0c_check_violation_t VALUES (2, 1, 11);
INSERT INTO r0c_check_violation_t VALUES (3, 0, 10);
SELECT id, score, limit_value FROM r0c_check_violation_t ORDER BY id;
