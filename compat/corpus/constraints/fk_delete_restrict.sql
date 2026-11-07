-- case: fk_delete_restrict
-- title: Fk delete restrict
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_fk_delete_restrict_t (id int PRIMARY KEY);
CREATE TABLE r0c_fk_delete_restrict_u (id int PRIMARY KEY, parent_id int REFERENCES r0c_fk_delete_restrict_t(id) ON DELETE RESTRICT);
INSERT INTO r0c_fk_delete_restrict_t VALUES (1);
INSERT INTO r0c_fk_delete_restrict_u VALUES (10, 1);
-- @error
-- expect-sqlstate: 23001
DELETE FROM r0c_fk_delete_restrict_t WHERE id = 1;
SELECT id FROM r0c_fk_delete_restrict_t ORDER BY id;
SELECT id, parent_id FROM r0c_fk_delete_restrict_u ORDER BY id;
