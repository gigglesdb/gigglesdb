select C.oid,
       C.xmin as state_number,
       C.castsource as castsource_id,
       pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name,
       C.casttarget as casttarget_id,
       pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name,
       C.castfunc as castfunc_id,
       pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name,
       C.castcontext,
       C.castmethod
from pg_cast C
     left outer join pg_proc F on C.castfunc::oid = F.oid
     left outer join pg_namespace FN on F.pronamespace = FN.oid
     join pg_type S on C.castsource::oid = S.oid
     join pg_namespace SN on S.typnamespace = SN.oid
     join pg_type T on C.casttarget::oid = T.oid
     join pg_namespace TN on T.typnamespace = TN.oid
--  where pg_catalog.age(C.xmin) <= #TXAGE