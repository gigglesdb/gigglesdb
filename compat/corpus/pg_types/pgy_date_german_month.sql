-- case: pgy_date_german_month
-- title: date german month
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'German';
SELECT 'Jan 2 2024'::date;
RESET datestyle;
