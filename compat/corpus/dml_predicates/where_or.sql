-- case: where_or
-- title: Where or
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_where_or_t (id int PRIMARY KEY, score int);
INSERT INTO r0c_where_or_t VALUES (1, 10);
INSERT INTO r0c_where_or_t VALUES (2, 20);
INSERT INTO r0c_where_or_t VALUES (3, 30);
SELECT id FROM r0c_where_or_t WHERE score = 10 OR score = 30 ORDER BY id;
