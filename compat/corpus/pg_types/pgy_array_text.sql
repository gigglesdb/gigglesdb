-- case: pgy_array_text
-- title: array text
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_text (id int PRIMARY KEY, v text[]);
INSERT INTO r0c_pgy_array_text VALUES (1, '{a,b}'), (2, '{a,b}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_text ORDER BY v, id;
