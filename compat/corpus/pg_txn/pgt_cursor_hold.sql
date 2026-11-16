-- case: pgt_cursor_hold
-- title: With hold cursor survives commit
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / With hold cursor survives commit
BEGIN;
DECLARE r0c_pgt_cursor_hold CURSOR WITH HOLD FOR SELECT id FROM (VALUES (1),(2),(3)) AS x(id) ORDER BY id;
FETCH NEXT FROM r0c_pgt_cursor_hold;
COMMIT;
FETCH FORWARD ALL FROM r0c_pgt_cursor_hold;
CLOSE ALL;
