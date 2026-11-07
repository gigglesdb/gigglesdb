-- case: unique_violation
-- title: Unique violation
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_unique_violation_t (id int PRIMARY KEY, name text UNIQUE);
INSERT INTO r0c_unique_violation_t VALUES (1, 'same');
-- @error
-- expect-sqlstate: 23505
INSERT INTO r0c_unique_violation_t VALUES (2, 'same');
SELECT id, name FROM r0c_unique_violation_t ORDER BY id;
