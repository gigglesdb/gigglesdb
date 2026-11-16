-- case: pgf_string_regex_sets
-- title: Regex matches and split set returning functions
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-matching.html
CREATE TABLE r0c_pgf_string_regex_sets (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_regex_sets VALUES (1,'a12b34'),(2,''),(3,NULL);
SELECT id,m,ord FROM r0c_pgf_string_regex_sets CROSS JOIN LATERAL regexp_matches(s,'([0-9]+)','g') WITH ORDINALITY AS r(m,ord) ORDER BY id,ord;
SELECT id,p,ord FROM r0c_pgf_string_regex_sets CROSS JOIN LATERAL regexp_split_to_table(s,'[0-9]+') WITH ORDINALITY AS r(p,ord) ORDER BY id,ord;
SELECT m,ord FROM regexp_matches('ab12cd34','([a-z]+)([0-9]+)','g') WITH ORDINALITY AS r(m,ord) ORDER BY ord;
SELECT p,ord FROM regexp_split_to_table('a,b,c',',') WITH ORDINALITY AS r(p,ord) ORDER BY ord;
DROP TABLE r0c_pgf_string_regex_sets;
