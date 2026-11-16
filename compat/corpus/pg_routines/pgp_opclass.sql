-- case: pgp_opclass
-- title: User btree operator class lifecycle
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE OPERATOR FAMILY r0c_pgp_opclass_fam USING btree;
CREATE OPERATOR CLASS r0c_pgp_opclass FOR TYPE integer USING btree FAMILY r0c_pgp_opclass_fam AS OPERATOR 1 < (integer,integer),OPERATOR 2 <= (integer,integer),OPERATOR 3 = (integer,integer),OPERATOR 4 >= (integer,integer),OPERATOR 5 > (integer,integer),FUNCTION 1 btint4cmp(integer,integer);
ALTER OPERATOR CLASS r0c_pgp_opclass USING btree RENAME TO r0c_pgp_opclass_renamed;
CREATE TABLE r0c_pgp_opclass_t(id integer PRIMARY KEY,v integer);
CREATE INDEX r0c_pgp_opclass_idx ON r0c_pgp_opclass_t USING btree(v r0c_pgp_opclass_renamed);
INSERT INTO r0c_pgp_opclass_t VALUES(1,3),(2,2);
SELECT v FROM r0c_pgp_opclass_t ORDER BY v;
DROP TABLE r0c_pgp_opclass_t;
DROP OPERATOR CLASS r0c_pgp_opclass_renamed USING btree;
DROP OPERATOR FAMILY r0c_pgp_opclass_fam USING btree;
