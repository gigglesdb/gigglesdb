-- case: pgy_date_sql_dmy_compact
-- title: date sql dmy compact
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'SQL, DMY';
SELECT '20240101'::date;
RESET datestyle;
