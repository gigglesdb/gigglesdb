-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select t.oid as id,
       t.xmin as state_number,
       t.evtname as name,
       t.evtevent as event,
       t.evtfoid as routine_id,
       pg_catalog.pg_get_userbyid(t.evtowner) as owner,
       t.evttags as tags,
       case when t.evtenabled = 'D' then 1 else 0 end as is_disabled
from pg_catalog.pg_event_trigger t;
