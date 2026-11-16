-- case: pgf_text_search
-- title: Full text parsing matching ranking and headline
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-textsearch.html
CREATE TABLE r0c_pgf_text_search (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_text_search VALUES (1,'The quick brown fox jumps'),(2,'A slow dog'),(3,NULL);
SELECT id, to_tsvector('english',s) FROM r0c_pgf_text_search ORDER BY id;
SELECT id, (CASE id WHEN 1 THEN '''brown'':3 ''fox'':4 ''jump'':5 ''quick'':2'::tsvector WHEN 2 THEN '''dog'':3 ''slow'':2'::tsvector ELSE NULL::tsvector END) @@ '''quick'' & ''fox'''::tsquery FROM r0c_pgf_text_search ORDER BY id;
SELECT id, (CASE id WHEN 1 THEN '''brown'':3 ''fox'':4 ''jump'':5 ''quick'':2'::tsvector WHEN 2 THEN '''dog'':3 ''slow'':2'::tsvector ELSE NULL::tsvector END) @@ '''quick'' & ''fox'''::tsquery FROM r0c_pgf_text_search ORDER BY id;
SELECT id, (CASE id WHEN 1 THEN '''brown'':3 ''fox'':4 ''jump'':5 ''quick'':2'::tsvector WHEN 2 THEN '''dog'':3 ''slow'':2'::tsvector ELSE NULL::tsvector END) @@ '''quick'' <-> ''brown'' | ''dog'''::tsquery FROM r0c_pgf_text_search ORDER BY id;
SELECT id, ts_rank((CASE id WHEN 1 THEN '''brown'':3 ''fox'':4 ''jump'':5 ''quick'':2'::tsvector WHEN 2 THEN '''dog'':3 ''slow'':2'::tsvector ELSE NULL::tsvector END),'''fox'''::tsquery) FROM r0c_pgf_text_search ORDER BY id;
SELECT id, ts_headline('english',s,'''fox'''::tsquery,'StartSel=<b>,StopSel=</b>') FROM r0c_pgf_text_search ORDER BY id;
SELECT to_tsquery('english','fox & !dog');
SELECT plainto_tsquery('english','the brown fox');
SELECT websearch_to_tsquery('english','fox -dog');
SELECT id,to_tsquery('english','quick & fox') FROM r0c_pgf_text_search ORDER BY id;
SELECT id,plainto_tsquery('english','quick fox') FROM r0c_pgf_text_search ORDER BY id;
SELECT id,websearch_to_tsquery('english','"quick brown" OR dog') FROM r0c_pgf_text_search ORDER BY id;
DROP TABLE r0c_pgf_text_search;
