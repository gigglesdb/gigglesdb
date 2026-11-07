-- case: where_like_and_ilike
-- title: Where like and ilike
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_where_like_and_ilike_t (id int PRIMARY KEY, name text);
INSERT INTO r0c_where_like_and_ilike_t VALUES (1, 'Alpha');
INSERT INTO r0c_where_like_and_ilike_t VALUES (2, 'alpha');
INSERT INTO r0c_where_like_and_ilike_t VALUES (3, 'Beta');
SELECT id FROM r0c_where_like_and_ilike_t WHERE name LIKE 'Al%' ORDER BY id;
SELECT id FROM r0c_where_like_and_ilike_t WHERE name ILIKE 'al%' ORDER BY id;
