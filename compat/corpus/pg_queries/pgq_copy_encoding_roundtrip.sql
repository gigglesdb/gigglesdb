-- case: pgq_copy_encoding_roundtrip
-- title: Single-byte COPY encoding with PROGRAM
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_encoding_roundtrip (s text PRIMARY KEY);
COPY t_pgq_copy_encoding_roundtrip FROM PROGRAM 'echo cafe' (ENCODING 'LATIN1');
COPY t_pgq_copy_encoding_roundtrip TO STDOUT (ENCODING 'WIN1252');
SELECT * FROM t_pgq_copy_encoding_roundtrip;
