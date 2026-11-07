-- case: no_from_aggregate
-- title: No from aggregate
-- owner: DD-0068
-- expect: compatible
SELECT count(*) AS c;
SELECT max(1) AS m;
