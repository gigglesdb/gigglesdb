-- case: set_show_timezone
-- title: Set and show the session time zone
-- owner: D
-- expect: compatible

SET TIME ZONE 'UTC';
SHOW TimeZone;
