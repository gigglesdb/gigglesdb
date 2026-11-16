-- case: pgy_date_sql_dmy_infinity
-- title: date sql dmy infinity
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'SQL, DMY';
SELECT 'infinity'::date;
RESET datestyle;
