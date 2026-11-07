-- case: insert_returning_cols
-- title: Insert and return columns expressions and aliases
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_insert_returning_cols_t (id integer PRIMARY KEY, score integer, name text);
INSERT INTO r0c_insert_returning_cols_t VALUES (1, 7, 'Ada') RETURNING id, score + 1 AS next_score, name AS display_name;
SELECT id, score, name FROM r0c_insert_returning_cols_t ORDER BY id;
