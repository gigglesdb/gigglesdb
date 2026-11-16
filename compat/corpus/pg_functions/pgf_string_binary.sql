-- case: pgf_string_binary
-- title: Binary encoding hashes and byte operations
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-binarystring.html
CREATE TABLE r0c_pgf_string_binary (id integer PRIMARY KEY, b bytea);
INSERT INTO r0c_pgf_string_binary VALUES (1,decode('61626300','hex')),(2,decode('ff01','hex')),(3,NULL);
SELECT id, encode(b,'hex') FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, encode(b,'base64') FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, md5(b) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, sha256(b) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, sha224(b) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, sha384(b) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, sha512(b) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, get_byte(b,0) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, get_bit(b,0) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, set_byte(b,0,65) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, set_bit(b,0,1) FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, md5('abc') FROM r0c_pgf_string_binary ORDER BY id;
SELECT id, decode('YWJj','base64') FROM r0c_pgf_string_binary ORDER BY id;
DROP TABLE r0c_pgf_string_binary;
