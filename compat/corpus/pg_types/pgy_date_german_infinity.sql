-- case: pgy_date_german_infinity
-- title: date german infinity
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'German';
SELECT 'infinity'::date;
RESET datestyle;
