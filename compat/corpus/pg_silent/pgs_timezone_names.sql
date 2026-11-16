-- case: pgs_timezone_names
-- title: Time zone names catalog
-- owner: G
-- expect: compatible
-- source: Differences: Read first / Time zone names catalog
SELECT name, abbrev, utc_offset, is_dst FROM pg_timezone_names WHERE name IN ('UTC','Etc/GMT+3') ORDER BY name;
