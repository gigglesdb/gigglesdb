select t.oid as id,
       t.xmin as state_number,
       t.evtname as name,
       t.evtevent as event,
       t.evtfoid as routine_id,
       pg_catalog.pg_get_userbyid(t.evtowner) as owner,
       t.evttags as tags,
       case when t.evtenabled = 'D' then 1 else 0 end as is_disabled
from pg_catalog.pg_event_trigger t
--  where pg_catalog.age(t.xmin) <= #TXAGE
