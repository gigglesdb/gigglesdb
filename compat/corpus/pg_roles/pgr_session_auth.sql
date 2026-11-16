-- case: pgr_session_auth
-- title: Session authorization changes current and session user
-- owner: unowned
-- expect: compatible
-- source: W6 SET SESSION AUTHORIZATION
CREATE ROLE r0c_pgr_session_auth_u;
SET SESSION AUTHORIZATION r0c_pgr_session_auth_u;
SELECT current_user, session_user;
-- @error
CREATE ROLE r0c_pgr_session_auth_denied;
RESET SESSION AUTHORIZATION;
SELECT current_user = 'postgres', session_user = 'postgres';
DROP ROLE r0c_pgr_session_auth_u;
