-- case: pgr_public
-- title: PUBLIC grants and revocation
-- owner: unowned
-- expect: compatible
-- source: Roles: grants to PUBLIC; W6 GRANT REVOKE
CREATE ROLE r0c_pgr_public_u;
CREATE TABLE r0c_pgr_public (id integer PRIMARY KEY);
INSERT INTO r0c_pgr_public VALUES (1);
GRANT SELECT ON r0c_pgr_public TO PUBLIC;
SET ROLE r0c_pgr_public_u;
SELECT * FROM r0c_pgr_public ORDER BY id;
RESET ROLE;
REVOKE SELECT ON r0c_pgr_public FROM PUBLIC;
SET ROLE r0c_pgr_public_u;
-- @error
SELECT * FROM r0c_pgr_public;
RESET ROLE;
DROP TABLE r0c_pgr_public;
DROP ROLE r0c_pgr_public_u;
