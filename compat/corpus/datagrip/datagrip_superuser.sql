-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_superuser
-- title: datagrip_superuser.sql#datagrip_superuser sha256:fd6d0fc6798a
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_superuser.sql#datagrip_superuser
-- source-sha256: fd6d0fc6798a6d3bf0349d345ca7e9ce5dab45268345424cf19ee5226acdd947
CREATE TABLE r0c_datagrip_superuser_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_superuser_body ON r0c_datagrip_superuser_app (body);
-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select usesuper
from pg_user
where usename = current_user;
