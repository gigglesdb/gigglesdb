-- case: pgt_roles
-- title: Set role and session authorization
-- owner: unowned
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Set role and session authorization
SET ROLE postgres;
SELECT current_user;
RESET ROLE;
SET SESSION AUTHORIZATION postgres;
SELECT session_user, current_user;
SET SESSION AUTHORIZATION DEFAULT;
RESET SESSION AUTHORIZATION;
