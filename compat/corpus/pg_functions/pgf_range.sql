-- case: pgf_range
-- title: Range bounds containment overlap intersection and dates
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-range.html
CREATE TABLE r0c_pgf_range (id integer PRIMARY KEY, r int4range);
INSERT INTO r0c_pgf_range VALUES (1,int4range(1,5)),(2,int4range(5,5)),(3,NULL);
SELECT id, lower(r) FROM r0c_pgf_range ORDER BY id;
SELECT id, upper(r) FROM r0c_pgf_range ORDER BY id;
SELECT id, isempty(r) FROM r0c_pgf_range ORDER BY id;
SELECT id, lower_inc(r) FROM r0c_pgf_range ORDER BY id;
SELECT id, upper_inc(r) FROM r0c_pgf_range ORDER BY id;
SELECT id, r @> 3 FROM r0c_pgf_range ORDER BY id;
SELECT id, r && '[3,8)'::int4range FROM r0c_pgf_range ORDER BY id;
SELECT id, r * '[3,8)'::int4range FROM r0c_pgf_range ORDER BY id;
SELECT id, '[2024-01-01,2024-02-01)'::daterange @> date '2024-01-15' FROM r0c_pgf_range ORDER BY id;
SELECT id, upper('[2024-01-01,2024-01-03)'::daterange) FROM r0c_pgf_range ORDER BY id;
SELECT daterange(date '2024-01-01',date '2024-02-01');
SELECT daterange('2024-01-01','2024-01-02','[]');
SELECT int4range(3,8);
DROP TABLE r0c_pgf_range;
