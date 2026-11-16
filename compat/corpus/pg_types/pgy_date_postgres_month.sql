-- case: pgy_date_postgres_month
-- title: date postgres month
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'Postgres';
SELECT 'Jan 2 2024'::date;
RESET datestyle;
