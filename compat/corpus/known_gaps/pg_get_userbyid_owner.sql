-- case: pg_get_userbyid_owner
-- title: Pg get userbyid owner
-- owner: G
-- expect: compatible
CREATE TABLE r0c_pg_get_userbyid_owner_t (id int PRIMARY KEY);
SELECT pg_catalog.pg_get_userbyid(c.relowner) FROM pg_catalog.pg_class c WHERE c.relname = 'r0c_pg_get_userbyid_owner_t';
