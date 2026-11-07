-- case: update_where_between
-- title: Update where between
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_update_where_between_t (id int PRIMARY KEY, score int);
INSERT INTO r0c_update_where_between_t VALUES (1, 10);
INSERT INTO r0c_update_where_between_t VALUES (2, 20);
INSERT INTO r0c_update_where_between_t VALUES (3, 30);
UPDATE r0c_update_where_between_t SET score = 99 WHERE score BETWEEN 10 AND 20;
SELECT id, score FROM r0c_update_where_between_t ORDER BY id;
