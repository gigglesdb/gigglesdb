-- case: explicit_bytea_cast
-- title: Explicit bytea cast
-- owner: E2
-- expect: compatible
SELECT position('\x02'::bytea IN '\x010203'::bytea) AS p;
