-- case: pgy_text_edges
-- title: text edges
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
SELECT ''::text,length('??'::text),octet_length('??'::text),E'a\nb',U&'\0061\0301';
-- @error
SELECT chr(0);
