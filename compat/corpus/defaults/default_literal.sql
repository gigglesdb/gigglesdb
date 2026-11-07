-- case: default_literal
-- title: Apply text numeric and boolean literal defaults
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_default_literal_t (id integer PRIMARY KEY, label text DEFAULT 'x', score integer DEFAULT 0, active boolean DEFAULT false);
INSERT INTO r0c_default_literal_t (id) VALUES (1);
SELECT id, label, score, active FROM r0c_default_literal_t ORDER BY id;
