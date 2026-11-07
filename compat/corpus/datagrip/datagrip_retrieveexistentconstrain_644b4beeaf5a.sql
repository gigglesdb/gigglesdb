-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveexistentconstrain_644b4beeaf5a
-- title: datagrip_RetrieveExistentConstraints.sql#datagrip_RetrieveExistentConstraints sha256:644b4beeaf5a
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_RetrieveExistentConstraints.sql#datagrip_RetrieveExistentConstraints
-- source-sha256: 644b4beeaf5ab4c86d56de2a476269abbeec078b609a5344bdec53ec58642fa3
-- parameters: $1=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrieveexistentconstrain_644b4beeaf5a_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveexistentconstrain_644b4beeaf5a_body ON r0c_datagrip_retrieveexistentconstrain_644b4beeaf5a_app (body);
select oid
from pg_catalog.pg_constraint
where conrelid != 0 and connamespace = 2200::oid
;
