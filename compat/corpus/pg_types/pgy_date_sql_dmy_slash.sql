-- case: pgy_date_sql_dmy_slash
-- title: date sql dmy slash
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'SQL, DMY';
SELECT '01/02/2024'::date;
RESET datestyle;
