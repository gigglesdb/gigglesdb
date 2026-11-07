select D.objoid id, pg_catalog.array_agg(D.objsubid) sub_ids
from pg_catalog.pg_description D
  join pg_catalog.pg_class C on D.objoid = C.oid
where C.relnamespace = $1::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass
group by D.objoid
union all
select T.oid id, pg_catalog.array_agg(D.objsubid)
from pg_catalog.pg_description D
  join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid
  left join pg_catalog.pg_class C on T.typrelid = C.oid
where T.typnamespace = $2::oid and (C.relkind = 'c' or C.relkind is null)
group by T.oid
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_constraint C on D.objoid = C.oid
where C.connamespace = $3::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_trigger T on T.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = T.tgrelid
where C.relnamespace = $4::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_rewrite R on R.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = R.ev_class
where C.relnamespace = $5::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_proc P on P.oid = D.objoid
where P.pronamespace = $6::oid and D.classoid = 'pg_catalog.pg_proc'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_operator O on O.oid = D.objoid
where O.oprnamespace = $7::oid and D.classoid = 'pg_catalog.pg_operator'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_opclass O on O.oid = D.objoid
where O.opcnamespace = $8::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_opfamily O on O.oid = D.objoid
where O.opfnamespace = $9::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_collation C on C.oid = D.objoid
where C.collnamespace = $10::oid and D.classoid = 'pg_catalog.pg_collation'::regclass

union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_policy P on P.oid = D.objoid
  join pg_catalog.pg_class C on P.polrelid = C.oid
where C.relnamespace = $11::oid  and D.classoid = 'pg_catalog.pg_policy'::regclass
