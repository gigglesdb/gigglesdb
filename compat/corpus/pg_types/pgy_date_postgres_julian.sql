-- case: pgy_date_postgres_julian
-- title: date postgres julian
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'Postgres';
SELECT 'J2451187'::date;
RESET datestyle;
