-- case: pgy_date_dmy_infinity
-- title: date dmy infinity
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'DMY';
SELECT 'infinity'::date;
RESET datestyle;
