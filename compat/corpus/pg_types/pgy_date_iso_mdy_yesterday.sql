-- case: pgy_date_iso_mdy_yesterday
-- title: date iso mdy yesterday
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'ISO, MDY';
SELECT 'yesterday'::date = CURRENT_DATE - 1;
RESET datestyle;
