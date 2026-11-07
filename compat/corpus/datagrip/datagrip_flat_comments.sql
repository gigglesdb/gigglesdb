-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_flat_comments
-- title: datagrip_flat_comments.sql#datagrip_flat_comments sha256:3aa36507c523
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_flat_comments.sql#datagrip_flat_comments
-- source-sha256: 3aa36507c523d1ae91a089f6631485f19d80062e95f40acec38990b33047e438
-- parameters: $1=2200, $2=2200, $3=2200, $4=2200, $5=2200, $6=2200, $7=2200, $8=2200, $9=2200, $10=2200, $11=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_flat_comments_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_flat_comments_body ON r0c_datagrip_flat_comments_app (body);
select D.objoid id, pg_catalog.array_agg(D.objsubid) sub_ids
from pg_catalog.pg_description D
  join pg_catalog.pg_class C on D.objoid = C.oid
where C.relnamespace = 2200::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass
group by D.objoid
union all
select T.oid id, pg_catalog.array_agg(D.objsubid)
from pg_catalog.pg_description D
  join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid
  left join pg_catalog.pg_class C on T.typrelid = C.oid
where T.typnamespace = 2200::oid and (C.relkind = 'c' or C.relkind is null)
group by T.oid
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_constraint C on D.objoid = C.oid
where C.connamespace = 2200::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_trigger T on T.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = T.tgrelid
where C.relnamespace = 2200::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_rewrite R on R.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = R.ev_class
where C.relnamespace = 2200::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_proc P on P.oid = D.objoid
where P.pronamespace = 2200::oid and D.classoid = 'pg_catalog.pg_proc'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_operator O on O.oid = D.objoid
where O.oprnamespace = 2200::oid and D.classoid = 'pg_catalog.pg_operator'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_opclass O on O.oid = D.objoid
where O.opcnamespace = 2200::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_opfamily O on O.oid = D.objoid
where O.opfnamespace = 2200::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass
union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_collation C on C.oid = D.objoid
where C.collnamespace = 2200::oid and D.classoid = 'pg_catalog.pg_collation'::regclass

union all
select D.objoid id, array[D.objsubid]
from pg_catalog.pg_description D
  join pg_catalog.pg_policy P on P.oid = D.objoid
  join pg_catalog.pg_class C on P.polrelid = C.oid
where C.relnamespace = 2200::oid  and D.classoid = 'pg_catalog.pg_policy'::regclass

;
