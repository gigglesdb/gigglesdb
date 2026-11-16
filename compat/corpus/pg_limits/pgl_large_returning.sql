-- case: pgl_large_returning
-- title: RETURNING emits a deterministic value larger than one MiB
-- owner: E1
-- expect: compatible
-- source: Maintenance: RETURNING output larger than 1 MiB
CREATE TABLE r0c_pgl_large_returning (id integer PRIMARY KEY, payload text);
INSERT INTO r0c_pgl_large_returning VALUES (1, repeat('x', 1048577)) RETURNING id, payload;
SELECT octet_length(payload) FROM r0c_pgl_large_returning WHERE id = 1;
DROP TABLE r0c_pgl_large_returning;
