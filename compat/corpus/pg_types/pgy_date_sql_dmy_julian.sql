-- case: pgy_date_sql_dmy_julian
-- title: date sql dmy julian
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'SQL, DMY';
SELECT 'J2451187'::date;
RESET datestyle;
