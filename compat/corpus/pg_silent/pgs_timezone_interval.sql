-- case: pgs_timezone_interval
-- title: Time zone interval affects timestamp display
-- owner: D
-- expect: compatible
-- source: Differences: Read first / Time zone interval affects timestamp display
SET TIME ZONE INTERVAL '+05:30' HOUR TO MINUTE;
SHOW TimeZone;
SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00';
RESET TimeZone;
