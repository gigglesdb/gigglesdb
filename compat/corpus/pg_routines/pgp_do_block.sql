-- case: pgp_do_block
-- title: Anonymous PLpgSQL block side effect
-- owner: unowned
-- expect: compatible
-- source: Differences row 124; W7 DO
CREATE TABLE r0c_pgp_do_block(id integer PRIMARY KEY);
DO $$BEGIN INSERT INTO r0c_pgp_do_block VALUES(1); END;$$ LANGUAGE plpgsql;
SELECT id FROM r0c_pgp_do_block ORDER BY id;
DROP TABLE r0c_pgp_do_block;
