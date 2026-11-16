-- case: pgy_date_postgres_epoch
-- title: date postgres epoch
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'Postgres';
SELECT 'epoch'::date;
RESET datestyle;
