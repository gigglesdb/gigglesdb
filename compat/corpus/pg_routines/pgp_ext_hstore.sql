-- case: pgp_ext_hstore
-- title: Extension hstore deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "hstore";
SELECT 'a=>1,b=>2'::hstore -> 'b';
SELECT 'a=>1,b=>2'::hstore @> 'a=>1'::hstore;
DROP EXTENSION "hstore";
