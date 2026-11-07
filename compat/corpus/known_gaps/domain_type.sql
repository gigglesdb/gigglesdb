-- case: domain_type
-- title: Domain type
-- owner: E2
-- expect: compatible
CREATE DOMAIN r0c_domain_type_d AS integer CHECK (VALUE > 0);
CREATE TABLE r0c_domain_type_t (row_id int PRIMARY KEY, id r0c_domain_type_d);
-- @error
-- expect-sqlstate: 23514
INSERT INTO r0c_domain_type_t VALUES (1, 0);
