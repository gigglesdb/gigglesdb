-- case: pgs_underscores
-- title: Underscores in numeric literals
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Underscores in numeric literals
SELECT 1_000;
SELECT 12_345_678, 1_000.25_00, 1_2e1_0, 0xF_F, 0o7_7, 0b10_01;
SELECT -1_000, 1_000::bigint + 2_000;
