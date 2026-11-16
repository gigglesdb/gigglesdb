-- case: pgy_date_iso_mdy_epoch
-- title: date iso mdy epoch
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'ISO, MDY';
SELECT 'epoch'::date;
RESET datestyle;
