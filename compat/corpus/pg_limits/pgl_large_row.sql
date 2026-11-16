-- case: pgl_large_row
-- title: Two hundred KiB text values survive storage and update
-- owner: E1
-- expect: compatible
-- source: Tables: rows larger than 64 KiB
CREATE TABLE r0c_pgl_large_row (id integer PRIMARY KEY, payload text);
INSERT INTO r0c_pgl_large_row VALUES (1, repeat('x', 204800));
SELECT id, octet_length(payload), payload = repeat('x', 204800) FROM r0c_pgl_large_row ORDER BY id;
UPDATE r0c_pgl_large_row SET payload = repeat('y', 204800) WHERE id = 1;
SELECT id, octet_length(payload), payload = repeat('y', 204800) FROM r0c_pgl_large_row ORDER BY id;
DROP TABLE r0c_pgl_large_row;
