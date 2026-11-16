-- case: pgy_date_dmy_epoch
-- title: date dmy epoch
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'DMY';
SELECT 'epoch'::date;
RESET datestyle;
