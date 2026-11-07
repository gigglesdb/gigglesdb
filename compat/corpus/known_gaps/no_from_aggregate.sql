-- case: no_from_aggregate
-- title: No from aggregate
-- owner: E3
-- expect: compatible
SELECT count(*) AS c;
SELECT max(1) AS m;
