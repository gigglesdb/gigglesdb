-- case: pgs_timezone_unknown
-- title: Unknown time zone is rejected
-- owner: D
-- expect: compatible
-- source: Differences: Read first / Unknown time zone is rejected
-- @error
SET TIME ZONE 'Not/AZone';
SHOW TimeZone;
SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00';
