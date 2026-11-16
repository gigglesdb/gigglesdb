-- case: pgt_cursor_binary
-- title: Binary and forward only cursor
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Binary and forward only cursor
BEGIN;
DECLARE r0c_pgt_cursor_binary BINARY NO SCROLL CURSOR FOR SELECT 42::int4 AS id;
SELECT name, is_binary, is_scrollable, is_holdable FROM pg_cursors WHERE name = 'r0c_pgt_cursor_binary' ORDER BY name;
FETCH NEXT FROM r0c_pgt_cursor_binary;
-- @error
FETCH BACKWARD FROM r0c_pgt_cursor_binary;
ROLLBACK;
