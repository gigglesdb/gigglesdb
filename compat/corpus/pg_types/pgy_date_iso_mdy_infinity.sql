-- case: pgy_date_iso_mdy_infinity
-- title: date iso mdy infinity
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'ISO, MDY';
SELECT 'infinity'::date;
RESET datestyle;
