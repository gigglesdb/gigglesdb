-- case: pgf_string_lengths
-- title: Unicode lengths codepoints normalization and prefixes
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-string.html
CREATE TABLE r0c_pgf_string_lengths (id integer PRIMARY KEY, s text);
INSERT INTO r0c_pgf_string_lengths VALUES (1,U&'\00E9clair'),(2,'abc'),(3,NULL);
SELECT id, ascii(s) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, chr(233) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, octet_length(s) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, bit_length(s) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, char_length(s) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, character_length(s) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, starts_with(s,U&'\00E9') FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, s ^@ 'a' FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, normalize(s,NFC) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, normalize(U&'e\0301',NFC) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, unistr('\0061\+01F600') FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, to_hex(255) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, to_oct(64) FROM r0c_pgf_string_lengths ORDER BY id;
SELECT id, to_bin(5) FROM r0c_pgf_string_lengths ORDER BY id;
DROP TABLE r0c_pgf_string_lengths;
