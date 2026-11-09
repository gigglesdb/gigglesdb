-- case: domain_cast
-- title: Casts to a domain type
-- owner: E2
-- expect: intentional Casts to a domain type
CREATE DOMAIN r0c_domain_cast AS integer;
-- expect-sqlstate: 42704
SELECT 1::r0c_domain_cast;
