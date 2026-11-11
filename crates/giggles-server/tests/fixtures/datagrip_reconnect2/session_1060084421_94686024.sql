select E.oid   as extension_id,
       D.objid as member_id
from pg_extension E
     join pg_depend D on E.oid = D.refobjid and
                         D.refclassid = 'pg_extension'::regclass::oid
where D.deptype = 'e'
order by extension_id