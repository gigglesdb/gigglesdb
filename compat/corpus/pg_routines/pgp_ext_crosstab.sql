-- case: pgp_ext_crosstab
-- title: Extension tablefunc deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "tablefunc";
SELECT * FROM crosstab('SELECT * FROM (VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40)) v(id,k,n) ORDER BY id,k') AS t(id integer,a integer,b integer) ORDER BY id;
DROP EXTENSION "tablefunc";
