-- case: pgp_ext_btree_gist
-- title: Extension btree_gist deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "btree_gist";
CREATE TABLE r0c_pgp_ext_btree_gist(id integer PRIMARY KEY,room integer,slot int4range,EXCLUDE USING gist(room WITH =,slot WITH &&));
INSERT INTO r0c_pgp_ext_btree_gist VALUES(1,10,'[1,3)');
-- @error
INSERT INTO r0c_pgp_ext_btree_gist VALUES(2,10,'[2,4)');
SELECT id FROM r0c_pgp_ext_btree_gist ORDER BY id;
DROP TABLE r0c_pgp_ext_btree_gist;
DROP EXTENSION "btree_gist";
