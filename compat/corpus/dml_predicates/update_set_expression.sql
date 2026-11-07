-- case: update_set_expression
-- title: Update set expression
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_update_set_expression_t (id int PRIMARY KEY, score int, name text);
INSERT INTO r0c_update_set_expression_t VALUES (1, 10, 'alpha');
UPDATE r0c_update_set_expression_t SET score = score + 1 WHERE id = 1;
UPDATE r0c_update_set_expression_t SET name = upper(name) WHERE id = 1;
SELECT id, score, name FROM r0c_update_set_expression_t ORDER BY id;
