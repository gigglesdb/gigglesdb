-- case: reset_all
-- title: Reset session settings and show the default search path
-- owner: D
-- expect: compatible

SET application_name TO 'r0c_reset_all';
SET search_path TO public;
RESET ALL;
SHOW search_path;
