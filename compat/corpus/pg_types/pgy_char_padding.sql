-- case: pgy_char_padding
-- title: char padding
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: string functions on char(n); types.txt / Character types
CREATE TABLE r0c_pgy_char_padding (id int PRIMARY KEY,v char(4));
INSERT INTO r0c_pgy_char_padding VALUES (1,'a'),(2,'a  '),(3,'b');
SELECT id,v,v='a',v='a  ',v::text,octet_length(v),v LIKE 'a' FROM r0c_pgy_char_padding ORDER BY r0c_pgy_char_padding.v,id;
SELECT 'abcde'::char(3), 'ab  '::char(4) = 'ab'::char(2);
