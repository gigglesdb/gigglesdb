-- case: pgy_date_sql_dmy_epoch
-- title: date sql dmy epoch
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'SQL, DMY';
SELECT 'epoch'::date;
RESET datestyle;
