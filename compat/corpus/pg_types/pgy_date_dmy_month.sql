-- case: pgy_date_dmy_month
-- title: date dmy month
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'DMY';
SELECT 'Jan 2 2024'::date;
RESET datestyle;
