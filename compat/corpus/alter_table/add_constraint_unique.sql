-- case: add_constraint_unique
-- title: Add constraint unique
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_add_constraint_unique_t (id int PRIMARY KEY, name text);
INSERT INTO r0c_add_constraint_unique_t VALUES (1, 'same');
ALTER TABLE r0c_add_constraint_unique_t ADD CONSTRAINT r0c_add_constraint_unique_uq UNIQUE (name);
-- @error
-- expect-sqlstate: 23505
INSERT INTO r0c_add_constraint_unique_t VALUES (2, 'same');
SELECT id, name FROM r0c_add_constraint_unique_t ORDER BY id;
