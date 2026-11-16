-- case: pgy_date_dmy_julian
-- title: date dmy julian
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'DMY';
SELECT 'J2451187'::date;
RESET datestyle;
