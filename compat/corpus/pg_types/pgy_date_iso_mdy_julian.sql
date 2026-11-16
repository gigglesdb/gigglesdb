-- case: pgy_date_iso_mdy_julian
-- title: date iso mdy julian
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'ISO, MDY';
SELECT 'J2451187'::date;
RESET datestyle;
