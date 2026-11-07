-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveexistenttables_38c1901282c3
-- title: datagrip_RetrieveExistentTables.sql#datagrip_RetrieveExistentTables sha256:38c1901282c3
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_RetrieveExistentTables.sql#datagrip_RetrieveExistentTables
-- source-sha256: 38c1901282c3eaa7322991752810066030a13188870c246af8b28db411895f48
-- parameters: $1=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrieveexistenttables_38c1901282c3_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveexistenttables_38c1901282c3_body ON r0c_datagrip_retrieveexistenttables_38c1901282c3_app (body);
select oid from pg_catalog.pg_class C
where relkind in ('r', 'm', 'v', 'p', 'f')
  and relnamespace = 2200::oid
;
