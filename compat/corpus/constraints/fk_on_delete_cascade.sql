-- case: fk_on_delete_cascade
-- title: Fk on delete cascade
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_fk_on_delete_cascade_t (id int PRIMARY KEY);
CREATE TABLE r0c_fk_on_delete_cascade_u (id int PRIMARY KEY, parent_id int REFERENCES r0c_fk_on_delete_cascade_t(id) ON DELETE CASCADE);
INSERT INTO r0c_fk_on_delete_cascade_t VALUES (1);
INSERT INTO r0c_fk_on_delete_cascade_u VALUES (10, 1);
DELETE FROM r0c_fk_on_delete_cascade_t WHERE id = 1;
SELECT id FROM r0c_fk_on_delete_cascade_t ORDER BY id;
SELECT id, parent_id FROM r0c_fk_on_delete_cascade_u ORDER BY id;
