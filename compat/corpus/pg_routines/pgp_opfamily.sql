-- case: pgp_opfamily
-- title: Operator family add drop comparator
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE OPERATOR FAMILY r0c_pgp_opfamily USING btree;
ALTER OPERATOR FAMILY r0c_pgp_opfamily USING btree ADD OPERATOR 1 < (integer,integer),FUNCTION 1 (integer,integer) btint4cmp(integer,integer);
SELECT am.amname, f.opfname FROM pg_opfamily f JOIN pg_am am ON am.oid=f.opfmethod WHERE f.opfname='r0c_pgp_opfamily';
ALTER OPERATOR FAMILY r0c_pgp_opfamily USING btree DROP OPERATOR 1 (integer,integer),FUNCTION 1 (integer,integer);
ALTER OPERATOR FAMILY r0c_pgp_opfamily USING btree RENAME TO r0c_pgp_opfamily_renamed;
DROP OPERATOR FAMILY r0c_pgp_opfamily_renamed USING btree;
