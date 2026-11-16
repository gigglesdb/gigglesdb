-- case: pgt_encoding
-- title: Client encoding other than UTF8
-- owner: D
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Client encoding other than UTF8
SET client_encoding TO 'LATIN1';
SHOW client_encoding;
SELECT 'ascii only' AS payload;
RESET client_encoding;
SHOW client_encoding;
