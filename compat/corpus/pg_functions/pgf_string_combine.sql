-- case: pgf_string_combine
-- title: Concatenation formatting and SQL quoting
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-string.html
CREATE TABLE r0c_pgf_string_combine (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_combine VALUES (1,'O''Reilly'),(2,''),(3,NULL);
SELECT id, concat(s,'!',NULL,42) FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, concat_ws(':',s,NULL,'x') FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, format('%s|%I|%L|%%',s,'Order',s) FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, format('%2$s/%1$5s','a','b') FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, quote_ident(s) FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, quote_literal(s) FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, quote_nullable(s) FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, concat('a',NULL,'b') FROM r0c_pgf_string_combine ORDER BY id;
SELECT id, quote_nullable(NULL::text) FROM r0c_pgf_string_combine ORDER BY id;
DROP TABLE r0c_pgf_string_combine;
