-- case: on_conflict_do_update_excluded
-- title: On conflict do update excluded
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_on_conflict_do_update_excluded_t (id int PRIMARY KEY, name text);
INSERT INTO r0c_on_conflict_do_update_excluded_t VALUES (1, 'before');
INSERT INTO r0c_on_conflict_do_update_excluded_t VALUES (1, 'after') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id, name;
SELECT id, name FROM r0c_on_conflict_do_update_excluded_t ORDER BY id;
