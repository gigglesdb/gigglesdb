-- case: pgt_cursor_scroll
-- title: Scrollable cursor fetch every direction and move
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Scrollable cursor fetch every direction and move
BEGIN;
DECLARE r0c_pgt_cursor_scroll SCROLL CURSOR FOR SELECT n FROM (VALUES (1),(2),(3),(4)) AS x(n) ORDER BY n;
FETCH NEXT FROM r0c_pgt_cursor_scroll;
FETCH PRIOR FROM r0c_pgt_cursor_scroll;
FETCH FIRST FROM r0c_pgt_cursor_scroll;
FETCH LAST FROM r0c_pgt_cursor_scroll;
FETCH ABSOLUTE 2 FROM r0c_pgt_cursor_scroll;
FETCH RELATIVE -1 FROM r0c_pgt_cursor_scroll;
FETCH FORWARD 2 FROM r0c_pgt_cursor_scroll;
FETCH BACKWARD 2 FROM r0c_pgt_cursor_scroll;
FETCH FORWARD ALL FROM r0c_pgt_cursor_scroll;
FETCH BACKWARD ALL FROM r0c_pgt_cursor_scroll;
FETCH ABSOLUTE -1 FROM r0c_pgt_cursor_scroll;
FETCH BACKWARD FROM r0c_pgt_cursor_scroll;
FETCH FORWARD FROM r0c_pgt_cursor_scroll;
FETCH 0 FROM r0c_pgt_cursor_scroll;
MOVE ABSOLUTE 0 FROM r0c_pgt_cursor_scroll;
FETCH ALL FROM r0c_pgt_cursor_scroll;
MOVE BACKWARD 2 FROM r0c_pgt_cursor_scroll;
MOVE FORWARD 1 FROM r0c_pgt_cursor_scroll;
FETCH NEXT FROM r0c_pgt_cursor_scroll;
CLOSE r0c_pgt_cursor_scroll;
COMMIT;
