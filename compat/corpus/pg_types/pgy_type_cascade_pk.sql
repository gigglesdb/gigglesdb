-- case: pgy_type_cascade_pk
-- title: type cascade pk
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: DROP TYPE CASCADE on primary-key column
CREATE TYPE r0c_pgy_type_cascade_pk_e AS ENUM ('x');
CREATE TABLE r0c_pgy_type_cascade_pk (v r0c_pgy_type_cascade_pk_e PRIMARY KEY,payload text);
INSERT INTO r0c_pgy_type_cascade_pk VALUES ('x','kept');
DROP TYPE r0c_pgy_type_cascade_pk_e CASCADE;
SELECT payload FROM r0c_pgy_type_cascade_pk ORDER BY payload;
SELECT column_name FROM information_schema.columns WHERE table_name='r0c_pgy_type_cascade_pk' ORDER BY ordinal_position;
DROP TABLE r0c_pgy_type_cascade_pk;
