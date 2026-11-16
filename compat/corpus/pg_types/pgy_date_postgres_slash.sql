-- case: pgy_date_postgres_slash
-- title: date postgres slash
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'Postgres';
SELECT '01/02/2024'::date;
RESET datestyle;
