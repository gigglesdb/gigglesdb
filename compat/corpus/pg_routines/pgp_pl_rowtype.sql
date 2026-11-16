-- case: pgp_pl_rowtype
-- title: PLpgSQL percent TYPE and ROWTYPE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE TABLE r0c_pgp_pl_rowtype(id integer PRIMARY KEY, label text);
INSERT INTO r0c_pgp_pl_rowtype VALUES (1,'one');
CREATE FUNCTION r0c_pgp_pl_rowtype_f() RETURNS text LANGUAGE plpgsql AS $$DECLARE r r0c_pgp_pl_rowtype%ROWTYPE; k r0c_pgp_pl_rowtype.id%TYPE := 1; BEGIN SELECT * INTO r FROM r0c_pgp_pl_rowtype WHERE id=k; RETURN r.label; END;$$;
SELECT r0c_pgp_pl_rowtype_f();
DROP FUNCTION r0c_pgp_pl_rowtype_f();
DROP TABLE r0c_pgp_pl_rowtype;
