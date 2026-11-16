-- case: pgs_collation
-- title: Text ordering with explicit ICU collation
-- owner: E2
-- expect: compatible
-- source: Differences: Read first / Text ordering with explicit ICU collation
SELECT v FROM (VALUES ('z'), ('a'), ('?'), ('Z'), ('A')) AS words(v) ORDER BY v COLLATE "und-x-icu", v COLLATE "C";
SELECT '?' COLLATE "und-x-icu" < 'z' COLLATE "und-x-icu";
