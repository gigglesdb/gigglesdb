-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrievecolumns
-- title: app-expanded-queries.json#dg_App_RetrieveColumns sha256:70d694dc074e
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/app-expanded-queries.json#dg_App_RetrieveColumns
-- source-sha256: 70d694dc074e404daf5a93f85a4483140df613924c1c15441f310ce4858c154d
-- parameters: :schema_id=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrievecolumns_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrievecolumns_body ON r0c_datagrip_retrievecolumns_app (body);
with T as ( select 
                  T.oid as table_id, T.relname as table_name
            from pg_catalog.pg_class T
                 
            where T.relnamespace = 2200::oid
              and T.relkind in ('r', 'm', 'v', 'f', 'p')
              
              
             
            )
select T.table_id,
       C.attnum as column_position,
       C.attname as column_name,
       C.xmin as column_state_number,
       C.atttypmod as type_mod,
       C.attndims as dimensions_number,
       pg_catalog.format_type(C.atttypid, C.atttypmod) as type_spec,
       C.atttypid as type_id,
       C.attnotnull as mandatory,
       pg_catalog.pg_get_expr(D.adbin, T.table_id) as column_default_expression,
       not C.attislocal as column_is_inherited,
        C.attfdwoptions as options,
       C.attisdropped as column_is_dropped,
       C.attidentity as identity_kind,
       C.attgenerated as generated
from T
  join pg_catalog.pg_attribute C on T.table_id = C.attrelid
  left join pg_catalog.pg_attrdef D on (C.attrelid, C.attnum) = (D.adrelid, D.adnum)
where attnum > 0
order by table_id, attnum
;
