-- case: pgy_numeric_special
-- title: numeric special
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_numeric_special (id int PRIMARY KEY,v numeric);
INSERT INTO r0c_pgy_numeric_special VALUES (1,'NaN'),(2,'Infinity'),(3,'-Infinity'),(4,0);
SELECT id,v,v=v,v>0 FROM r0c_pgy_numeric_special ORDER BY v,id;
SELECT 'Infinity'::numeric + '-Infinity'::numeric;
