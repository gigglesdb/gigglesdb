-- case: pgr_all_sequences
-- title: Bulk grants on all sequences in a schema
-- owner: unowned
-- expect: compatible
-- source: Roles: GRANT ON ALL TABLES SEQUENCES FUNCTIONS IN SCHEMA
CREATE ROLE r0c_pgr_all_sequences_u;
CREATE SCHEMA r0c_pgr_all_sequences;
GRANT USAGE ON SCHEMA r0c_pgr_all_sequences TO r0c_pgr_all_sequences_u;
CREATE SEQUENCE r0c_pgr_all_sequences.r0c_pgr_all_sequences_s;
SET ROLE r0c_pgr_all_sequences_u;
-- @error
SELECT nextval('r0c_pgr_all_sequences.r0c_pgr_all_sequences_s');
RESET ROLE;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA r0c_pgr_all_sequences TO r0c_pgr_all_sequences_u;
SET ROLE r0c_pgr_all_sequences_u;
SELECT nextval('r0c_pgr_all_sequences.r0c_pgr_all_sequences_s');
RESET ROLE;
REVOKE USAGE ON ALL SEQUENCES IN SCHEMA r0c_pgr_all_sequences FROM r0c_pgr_all_sequences_u;
DROP SEQUENCE r0c_pgr_all_sequences.r0c_pgr_all_sequences_s;
DROP SCHEMA r0c_pgr_all_sequences;
DROP ROLE r0c_pgr_all_sequences_u;
