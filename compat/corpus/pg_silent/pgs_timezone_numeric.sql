-- case: pgs_timezone_numeric
-- title: Time zone numeric affects timestamp display
-- owner: D
-- expect: compatible
-- source: Differences: Read first / Time zone numeric affects timestamp display
SET TIME ZONE 2;
SHOW TimeZone;
SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00';
RESET TimeZone;
