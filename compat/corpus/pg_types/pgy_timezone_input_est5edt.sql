-- case: pgy_timezone_input_est5edt
-- title: timezone input est5edt
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: time zone abbreviations in input; types.txt / Input formats
SELECT '2024-01-02 03:04:05 EST5EDT'::timestamptz, '2024-07-02 03:04:05 EST5EDT'::timestamptz;
