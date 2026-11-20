-- case: pgq_copy_options_legacy
-- title: COPY options legacy
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_legacy (id int PRIMARY KEY, a text, b text);
COPY t_pgq_copy_options_legacy FROM PROGRAM 'echo id,a,b&& echo 1,,~~' CSV HEADER QUOTE AS '~' FORCE NOT NULL a FORCE NULL b;
SELECT * FROM t_pgq_copy_options_legacy ORDER BY id;
