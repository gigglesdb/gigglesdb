-- case: pgy_date_postgres_yesterday
-- title: date postgres yesterday
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'Postgres';
SELECT 'yesterday'::date = CURRENT_DATE - 1;
RESET datestyle;
