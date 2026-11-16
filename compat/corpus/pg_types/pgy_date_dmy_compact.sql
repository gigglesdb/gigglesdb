-- case: pgy_date_dmy_compact
-- title: date dmy compact
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'DMY';
SELECT '20240101'::date;
RESET datestyle;
