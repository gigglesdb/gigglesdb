-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_datestyle
-- title: latest-expanded-queries.json#dg_Batch_DateStyle sha256:47f8353fef2e
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_DateStyle
-- source-sha256: 47f8353fef2e3865283ddb6d687f385ec78425bd332ef823c371daa23298ac6c
CREATE TABLE r0c_datagrip_datestyle_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_datestyle_body ON r0c_datagrip_datestyle_app (body);
show DateStyle
;
