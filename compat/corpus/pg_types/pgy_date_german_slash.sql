-- case: pgy_date_german_slash
-- title: date german slash
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'German';
SELECT '01/02/2024'::date;
RESET datestyle;
