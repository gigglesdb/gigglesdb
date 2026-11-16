-- case: pgs_version
-- title: Version prefix and server version number
-- owner: G
-- expect: compatible
-- source: Differences: Read first / Version prefix and server version number
SELECT split_part(version(), ' ', 1), current_setting('server_version_num');
