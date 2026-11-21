-- case: pgq_copy_encoding_names
-- title: COPY encoding alias lookup and unsupported multibyte names
-- owner: E1
-- expect: compatible
COPY (SELECT 'abc') TO STDOUT (ENCODING 'latin-1');
COPY (SELECT 'abc') TO STDOUT (ENCODING 'ISO_8859_1');
COPY (SELECT 'abc') TO STDOUT (ENCODING 'windows1252');
COPY (SELECT 'abc') TO STDOUT (ENCODING 'win');
COPY (SELECT 'abc') TO STDOUT (ENCODING 'unicode');
COPY (SELECT 'abc') TO STDOUT (ENCODING 'alt');
COPY (SELECT 'abc') TO STDOUT (ENCODING 'tcvn');
-- SQLSTATE 22023
-- @error
COPY (SELECT 'abc') TO STDOUT (ENCODING 'cp1252');
-- SQLSTATE 22023
-- @error
COPY (SELECT 'abc') TO STDOUT (ENCODING 'UTF16');
