-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveforeignservers
-- title: latest-expanded-queries.json#dg_Batch_RetrieveForeignServers sha256:fc31e7569a27
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveForeignServers
-- source-sha256: fc31e7569a27d20d68ad56168aa007001b6e54e843cb55098bcaa6a2fe074a7e
CREATE TABLE r0c_datagrip_retrieveforeignservers_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveforeignservers_body ON r0c_datagrip_retrieveforeignservers_app (body);
select srv.oid as id,
       srv.srvfdw as fdw_id,
       srv.xmin as state_number,
       srv.srvname as name,
       srv.srvtype as type,
       srv.srvversion as version,
       srv.srvoptions as options,
       pg_catalog.pg_get_userbyid(srv.srvowner) as "owner"
from pg_catalog.pg_foreign_server srv
--  where pg_catalog.age(srv.xmin) <= #TXAGE
;
