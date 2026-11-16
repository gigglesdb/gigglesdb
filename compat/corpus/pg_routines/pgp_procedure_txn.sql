-- case: pgp_procedure_txn
-- title: Procedure commits and rolls back internally
-- owner: unowned
-- expect: compatible
-- source: Differences rows 121,122; W7 PROCEDURE CALL
CREATE TABLE r0c_pgp_procedure_txn(id integer PRIMARY KEY);
CREATE PROCEDURE r0c_pgp_procedure_txn_p() LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_procedure_txn VALUES(1); COMMIT; INSERT INTO r0c_pgp_procedure_txn VALUES(2); ROLLBACK; END;$$;
CALL r0c_pgp_procedure_txn_p();
SELECT id FROM r0c_pgp_procedure_txn ORDER BY id;
ALTER PROCEDURE r0c_pgp_procedure_txn_p() RENAME TO r0c_pgp_procedure_txn_renamed;
DROP PROCEDURE r0c_pgp_procedure_txn_renamed();
DROP TABLE r0c_pgp_procedure_txn;
