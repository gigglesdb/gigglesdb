-- case: pgt_constraints
-- title: Deferred constraints checked when made immediate
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Deferred constraints checked when made immediate
CREATE TABLE r0c_pgt_constraints (id int PRIMARY KEY, parent int, CONSTRAINT r0c_pgt_constraints_fk FOREIGN KEY (parent) REFERENCES r0c_pgt_constraints(id) DEFERRABLE INITIALLY IMMEDIATE);
BEGIN;
SET CONSTRAINTS r0c_pgt_constraints_fk DEFERRED;
INSERT INTO r0c_pgt_constraints VALUES (1,2);
INSERT INTO r0c_pgt_constraints VALUES (2,NULL);
SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO r0c_pgt_constraints VALUES (3,4);
-- @error
SET CONSTRAINTS ALL IMMEDIATE;
ROLLBACK;
SELECT id, parent FROM r0c_pgt_constraints ORDER BY id;
