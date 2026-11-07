-- case: delete_with_in_subquery
-- title: Delete with in subquery
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_delete_with_in_subquery_t (id int PRIMARY KEY);
CREATE TABLE r0c_delete_with_in_subquery_u (id int PRIMARY KEY);
INSERT INTO r0c_delete_with_in_subquery_t VALUES (1);
INSERT INTO r0c_delete_with_in_subquery_t VALUES (2);
INSERT INTO r0c_delete_with_in_subquery_u VALUES (2);
DELETE FROM r0c_delete_with_in_subquery_t WHERE id IN (SELECT id FROM r0c_delete_with_in_subquery_u);
SELECT id FROM r0c_delete_with_in_subquery_t ORDER BY id;
