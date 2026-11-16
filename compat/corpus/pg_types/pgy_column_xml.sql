-- case: pgy_column_xml
-- title: column xml
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_xml (id int PRIMARY KEY, v xml);
INSERT INTO r0c_pgy_column_xml VALUES (1, '<a>hello</a>'), (2, '<a>hello</a>');
SELECT id, v, v::text=v::text, xpath('/a/text()',v) FROM r0c_pgy_column_xml ORDER BY v::text, id;
