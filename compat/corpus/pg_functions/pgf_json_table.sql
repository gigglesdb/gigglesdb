-- case: pgf_json_table
-- title: SQL JSON table relational projection
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-json.html
CREATE TABLE r0c_pgf_json_table (id integer PRIMARY KEY, j jsonb);
INSERT INTO r0c_pgf_json_table VALUES (1,'[{"name":"a","qty":2},{"name":"b"}]'),(2,'[]'),(3,NULL);
SELECT t.id,q.n,q.name,q.qty,q.has_qty FROM r0c_pgf_json_table AS t CROSS JOIN JSON_TABLE(t.j,'$[*]' COLUMNS(n FOR ORDINALITY,name text PATH '$.name',qty integer PATH '$.qty' DEFAULT 0 ON EMPTY,has_qty boolean EXISTS PATH '$.qty')) AS q ORDER BY t.id,q.n;
SELECT q.n,q.x FROM JSON_TABLE('[1,2,null]','$[*]' COLUMNS(n FOR ORDINALITY,x integer PATH '$')) AS q ORDER BY q.n;
DROP TABLE r0c_pgf_json_table;
