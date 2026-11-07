-- case: set_show_search_path
-- title: Set and show the session search path
-- owner: D
-- expect: compatible

SET search_path TO public;
SHOW search_path;
