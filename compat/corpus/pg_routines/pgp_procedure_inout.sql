-- case: pgp_procedure_inout
-- title: CALL with INOUT parameter
-- owner: unowned
-- expect: compatible
-- source: Differences row 122; W7 CALL PROCEDURE
CREATE PROCEDURE r0c_pgp_procedure_inout(INOUT x integer) LANGUAGE plpgsql AS $$BEGIN x := x+10; END;$$;
CALL r0c_pgp_procedure_inout(5);
ALTER PROCEDURE r0c_pgp_procedure_inout(integer) SET search_path TO pg_catalog;
DROP PROCEDURE r0c_pgp_procedure_inout(integer);
