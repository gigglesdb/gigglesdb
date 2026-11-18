-- case: pgq_copy_program_roundtrip
-- title: copy to program, from relative file and from program as superuser
-- owner: E1
-- expect: compatible
-- source: GD-0079 Q6 COPY server files and programs (superuser)
CREATE TABLE r0c_pgq_copy_program_roundtrip (id int PRIMARY KEY, t text);
INSERT INTO r0c_pgq_copy_program_roundtrip VALUES (1,'a'),(2,'b c'),(3,NULL);
CREATE TABLE r0c_pgq_copy_program_roundtrip_dst (id int PRIMARY KEY, t text);
COPY r0c_pgq_copy_program_roundtrip TO PROGRAM 'sort > r0c_pgq_copy_program_roundtrip.txt';
COPY r0c_pgq_copy_program_roundtrip_dst FROM 'r0c_pgq_copy_program_roundtrip.txt';
SELECT id, t, t IS NULL AS t_null FROM r0c_pgq_copy_program_roundtrip_dst ORDER BY id;
DELETE FROM r0c_pgq_copy_program_roundtrip_dst;
COPY r0c_pgq_copy_program_roundtrip_dst FROM PROGRAM 'sort r0c_pgq_copy_program_roundtrip.txt';
SELECT id, t, t IS NULL AS t_null FROM r0c_pgq_copy_program_roundtrip_dst ORDER BY id;
DELETE FROM r0c_pgq_copy_program_roundtrip_dst;
COPY (SELECT id, upper(t) FROM r0c_pgq_copy_program_roundtrip WHERE id < 3) TO PROGRAM 'sort > r0c_pgq_copy_program_roundtrip.csv' WITH (FORMAT csv);
COPY r0c_pgq_copy_program_roundtrip_dst (id, t) FROM 'r0c_pgq_copy_program_roundtrip.csv' WITH (FORMAT csv);
SELECT id, t FROM r0c_pgq_copy_program_roundtrip_dst ORDER BY id;
COPY r0c_pgq_copy_program_roundtrip_dst FROM PROGRAM 'exit 0';
-- @error
COPY r0c_pgq_copy_program_roundtrip_dst FROM 'r0c_pgq_copy_program_roundtrip.csv' WITH (FORMAT csv);
