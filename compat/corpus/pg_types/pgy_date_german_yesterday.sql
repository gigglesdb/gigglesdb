-- case: pgy_date_german_yesterday
-- title: date german yesterday
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'German';
SELECT 'yesterday'::date = CURRENT_DATE - 1;
RESET datestyle;
