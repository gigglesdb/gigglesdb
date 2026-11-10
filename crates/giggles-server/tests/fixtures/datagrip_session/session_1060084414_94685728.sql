select name, is_dst from pg_catalog.pg_timezone_names
union distinct
select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs
