-- case: pgy_key_index_uuid
-- title: key index uuid
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_key_index_uuid (v uuid PRIMARY KEY);
INSERT INTO r0c_pgy_key_index_uuid VALUES ('123e4567-e89b-12d3-a456-426614174000');
CREATE UNIQUE INDEX r0c_pgy_key_index_uuid_idx ON r0c_pgy_key_index_uuid(v);
SELECT v FROM r0c_pgy_key_index_uuid ORDER BY v;
