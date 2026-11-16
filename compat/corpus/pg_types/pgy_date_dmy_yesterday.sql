-- case: pgy_date_dmy_yesterday
-- title: date dmy yesterday
-- owner: DD-0068
-- expect: compatible
-- source: Differences / Data types: non-ISO date input and DateStyle; types.txt / Input formats
SET datestyle = 'DMY';
SELECT 'yesterday'::date = CURRENT_DATE - 1;
RESET datestyle;
