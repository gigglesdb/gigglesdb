-- case: pgy_date_german_compact
-- title: date german compact
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'German';
SELECT '20240101'::date;
RESET datestyle;
