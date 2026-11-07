-- case: on_conflict_on_constraint
-- title: On conflict on constraint
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_on_conflict_on_constraint_t (id int PRIMARY KEY, name text, CONSTRAINT r0c_on_conflict_on_constraint_uq UNIQUE (name));
INSERT INTO r0c_on_conflict_on_constraint_t VALUES (1, 'same');
INSERT INTO r0c_on_conflict_on_constraint_t VALUES (2, 'same') ON CONFLICT ON CONSTRAINT r0c_on_conflict_on_constraint_uq DO UPDATE SET id = EXCLUDED.id RETURNING id, name;
SELECT id, name FROM r0c_on_conflict_on_constraint_t ORDER BY id;
