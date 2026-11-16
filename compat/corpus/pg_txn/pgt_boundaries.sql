-- case: pgt_boundaries
-- title: Begin start abort commit end rollback
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Begin start abort commit end rollback
CREATE TABLE r0c_pgt_boundaries (id int PRIMARY KEY);
BEGIN WORK;
INSERT INTO r0c_pgt_boundaries VALUES (1);
ABORT WORK;
START TRANSACTION;
INSERT INTO r0c_pgt_boundaries VALUES (2);
END WORK;
BEGIN TRANSACTION;
INSERT INTO r0c_pgt_boundaries VALUES (3);
ROLLBACK WORK;
SELECT id FROM r0c_pgt_boundaries ORDER BY id;
