-- case: on_conflict_do_nothing
-- title: On conflict do nothing
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_on_conflict_do_nothing_t (id int PRIMARY KEY, name text UNIQUE);
INSERT INTO r0c_on_conflict_do_nothing_t VALUES (1, 'first');
INSERT INTO r0c_on_conflict_do_nothing_t VALUES (1, 'second') ON CONFLICT (id) DO NOTHING;
INSERT INTO r0c_on_conflict_do_nothing_t VALUES (2, 'first') ON CONFLICT DO NOTHING;
SELECT id, name FROM r0c_on_conflict_do_nothing_t ORDER BY id;
