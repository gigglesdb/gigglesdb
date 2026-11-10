-- case: domain_cast
-- title: Casts to a domain type
-- owner: E2
-- expect: compatible
CREATE DOMAIN r0c_domain_cast AS integer;
SELECT 1::r0c_domain_cast;
