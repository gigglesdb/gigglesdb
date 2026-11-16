-- case: pgy_date_german_epoch
-- title: date german epoch
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'German';
SELECT 'epoch'::date;
RESET datestyle;
