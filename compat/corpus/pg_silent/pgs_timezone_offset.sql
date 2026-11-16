-- case: pgs_timezone_offset
-- title: Time zone offset affects timestamp display
-- owner: D
-- expect: compatible
-- source: Differences: Read first / Time zone offset affects timestamp display
SET TIME ZONE '+05:30';
SHOW TimeZone;
SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00';
RESET TimeZone;
