-- case: on_conflict_do_update_where
-- title: On conflict do update where
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_on_conflict_do_update_where_t (id int PRIMARY KEY, score int);
INSERT INTO r0c_on_conflict_do_update_where_t VALUES (1, 10);
INSERT INTO r0c_on_conflict_do_update_where_t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score WHERE EXCLUDED.score > r0c_on_conflict_do_update_where_t.score;
SELECT id, score FROM r0c_on_conflict_do_update_where_t ORDER BY id;
INSERT INTO r0c_on_conflict_do_update_where_t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score WHERE EXCLUDED.score > r0c_on_conflict_do_update_where_t.score;
SELECT id, score FROM r0c_on_conflict_do_update_where_t ORDER BY id;
