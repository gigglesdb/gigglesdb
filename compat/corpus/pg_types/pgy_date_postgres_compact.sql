-- case: pgy_date_postgres_compact
-- title: date postgres compact
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'Postgres';
SELECT '20240101'::date;
RESET datestyle;
