-- case: pgq_copy_options_header_match
-- title: COPY options header_match
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_header_match (id int PRIMARY KEY, a text, b text);
COPY t_pgq_copy_options_header_match FROM PROGRAM 'echo id,a,b&& echo 1,~x,y~,z' WITH (FORMAT csv, HEADER MATCH, QUOTE '~');
SELECT * FROM t_pgq_copy_options_header_match ORDER BY id;
