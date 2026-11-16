-- case: pgr_columns
-- title: Column SELECT and UPDATE restrict whole row access
-- owner: unowned
-- expect: compatible
-- source: Roles: column privileges
CREATE ROLE r0c_pgr_columns_u;
CREATE TABLE r0c_pgr_columns (id integer PRIMARY KEY, allowed text, secret text);
INSERT INTO r0c_pgr_columns VALUES (1, 'visible', 'hidden');
GRANT SELECT (id, allowed), UPDATE (allowed) ON r0c_pgr_columns TO r0c_pgr_columns_u;
SET ROLE r0c_pgr_columns_u;
SELECT id, allowed FROM r0c_pgr_columns ORDER BY id;
UPDATE r0c_pgr_columns SET allowed = 'changed' WHERE id = 1 RETURNING id, allowed;
-- @error
SELECT * FROM r0c_pgr_columns;
-- @error
UPDATE r0c_pgr_columns SET secret = 'bad' WHERE id = 1;
RESET ROLE;
DROP TABLE r0c_pgr_columns;
DROP ROLE r0c_pgr_columns_u;
