-- case: pgs_timezone_posix
-- title: Time zone posix affects timestamp display
-- owner: D
-- expect: compatible
-- source: Differences: Read first / Time zone posix affects timestamp display
SET TIME ZONE 'UTC+3';
SHOW TimeZone;
SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00';
RESET TimeZone;
