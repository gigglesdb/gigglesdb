-- case: pgr_owned
-- title: Ownership reassignment and dropping owned objects and grants
-- owner: unowned
-- expect: compatible
-- source: Roles: REASSIGN OWNED DROP OWNED
CREATE ROLE r0c_pgr_owned_old;
CREATE ROLE r0c_pgr_owned_new;
CREATE TABLE r0c_pgr_owned (id integer PRIMARY KEY);
ALTER TABLE r0c_pgr_owned OWNER TO r0c_pgr_owned_old;
REASSIGN OWNED BY r0c_pgr_owned_old TO r0c_pgr_owned_new;
SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = 'r0c_pgr_owned';
CREATE TABLE r0c_pgr_owned_keep (id integer PRIMARY KEY);
GRANT SELECT ON r0c_pgr_owned_keep TO r0c_pgr_owned_new;
DROP OWNED BY r0c_pgr_owned_new;
SELECT to_regclass('public.r0c_pgr_owned') IS NULL AS removed, has_table_privilege('r0c_pgr_owned_new', 'r0c_pgr_owned_keep', 'SELECT') AS remaining_grant;
DROP TABLE r0c_pgr_owned_keep;
DROP ROLE r0c_pgr_owned_old, r0c_pgr_owned_new;
