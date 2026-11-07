-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveexistentconstraints
-- title: app-expanded-queries.json#dg_App_RetrieveExistentConstraints sha256:21519045d4f3
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/app-expanded-queries.json#dg_App_RetrieveExistentConstraints
-- source-sha256: 21519045d4f3c8c7bc20e3e17814f029765ae038370e14e66bad7599062223c7
-- parameters: :schema_id=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrieveexistentconstraints_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveexistentconstraints_body ON r0c_datagrip_retrieveexistentconstraints_app (body);
select oid
from pg_catalog.pg_constraint
where conrelid != 0 and connamespace = 2200::oid
;
