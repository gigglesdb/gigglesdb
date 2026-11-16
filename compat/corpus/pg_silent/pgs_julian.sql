-- case: pgs_julian
-- title: Julian extraction output format
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Julian extraction output format
SELECT extract(julian FROM DATE '2000-01-01'), extract(julian FROM DATE '2024-02-29'), extract(julian FROM DATE '1900-01-01');
