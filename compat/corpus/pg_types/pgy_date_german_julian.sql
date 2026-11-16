-- case: pgy_date_german_julian
-- title: date german julian
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'German';
SELECT 'J2451187'::date;
RESET datestyle;
