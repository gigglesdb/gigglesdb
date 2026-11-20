-- case: pgq_copy_default_marker
-- title: COPY runtime pgq_copy_default_marker
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_default_marker (id serial, a text DEFAULT 'da');
COPY t_pgq_copy_default_marker FROM PROGRAM 'echo D,D&& echo 100,x' WITH (FORMAT csv, DEFAULT 'D');
SELECT * FROM t_pgq_copy_default_marker ORDER BY id;
