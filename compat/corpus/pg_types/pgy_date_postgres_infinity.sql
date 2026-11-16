-- case: pgy_date_postgres_infinity
-- title: date postgres infinity
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'Postgres';
SELECT 'infinity'::date;
RESET datestyle;
