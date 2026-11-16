-- id: pgt_startup_timezone
-- title: Startup pgt_startup_timezone
-- owner: D
-- env: PGTZ=Asia/Kolkata
SELECT current_setting('TimeZone'), TIMESTAMPTZ '2024-01-02 00:00:00+00';
