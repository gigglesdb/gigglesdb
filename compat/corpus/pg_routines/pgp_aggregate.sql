-- case: pgp_aggregate
-- title: User defined sum aggregate lifecycle
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE FUNCTION r0c_pgp_aggregate_step(integer,integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1+$2$$;
CREATE AGGREGATE r0c_pgp_aggregate(integer) (SFUNC=r0c_pgp_aggregate_step,STYPE=integer,INITCOND='0');
SELECT r0c_pgp_aggregate(v) FROM (VALUES(2),(3)) t(v);
ALTER AGGREGATE r0c_pgp_aggregate(integer) RENAME TO r0c_pgp_aggregate_renamed;
SELECT r0c_pgp_aggregate_renamed(v) FROM (VALUES(4),(5)) t(v);
DROP AGGREGATE r0c_pgp_aggregate_renamed(integer);
DROP FUNCTION r0c_pgp_aggregate_step(integer,integer);
