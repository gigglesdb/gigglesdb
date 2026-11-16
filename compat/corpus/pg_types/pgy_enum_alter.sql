-- case: pgy_enum_alter
-- title: enum alter
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: ALTER TYPE and enum comparison with explicit cast; types.txt / Enum types
CREATE TYPE r0c_pgy_enum_alter_e AS ENUM ('sad','happy');
ALTER TYPE r0c_pgy_enum_alter_e ADD VALUE 'ok' BEFORE 'happy';
ALTER TYPE r0c_pgy_enum_alter_e ADD VALUE 'great' AFTER 'happy';
ALTER TYPE r0c_pgy_enum_alter_e RENAME VALUE 'great' TO 'joyful';
CREATE TABLE r0c_pgy_enum_alter (id int PRIMARY KEY,v r0c_pgy_enum_alter_e);
INSERT INTO r0c_pgy_enum_alter VALUES (1,'happy'),(2,'ok'),(3,'sad'),(4,'joyful');
SELECT v, v > 'sad'::r0c_pgy_enum_alter_e, v < 'happy', v <= 'ok', v >= 'ok' FROM r0c_pgy_enum_alter ORDER BY v;
SELECT min(v), max(v), array_agg(v ORDER BY v) FROM r0c_pgy_enum_alter;
SELECT a.v,b.v,a.v > b.v FROM r0c_pgy_enum_alter a CROSS JOIN r0c_pgy_enum_alter b ORDER BY a.id,b.id;
-- @error
INSERT INTO r0c_pgy_enum_alter VALUES (5,'unknown');
DROP TABLE r0c_pgy_enum_alter;
DROP TYPE r0c_pgy_enum_alter_e;
