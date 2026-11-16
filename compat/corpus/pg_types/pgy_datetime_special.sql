-- case: pgy_datetime_special
-- title: datetime special
-- owner: DD-0068
-- expect: compatible
-- source: Data types; types.txt
SELECT '-infinity'::date,'infinity'::timestamp,'-infinity'::timestamptz,'0001-01-01 BC'::date;
SELECT 'allballs'::time, 'P1Y2M'::interval, 'today'::date = current_date;
-- @error
SELECT '2024-02-30'::date;
