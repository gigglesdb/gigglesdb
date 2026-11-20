-- case: pgq_copy_options_escape
-- title: COPY options escape
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_escape (id int PRIMARY KEY, a text, b text);
COPY t_pgq_copy_options_escape FROM PROGRAM 'echo id,a,b&& echo 1,~x!~y~,~z!!q~' WITH (FORMAT csv, HEADER, QUOTE '~', ESCAPE '!');
SELECT * FROM t_pgq_copy_options_escape ORDER BY id;
