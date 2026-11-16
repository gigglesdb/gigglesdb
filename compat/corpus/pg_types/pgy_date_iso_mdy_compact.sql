-- case: pgy_date_iso_mdy_compact
-- title: date iso mdy compact
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'ISO, MDY';
SELECT '20240101'::date;
RESET datestyle;
