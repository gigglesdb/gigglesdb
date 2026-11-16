-- case: pgy_domain_nested
-- title: domain nested
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: domains over domains; W5 CREATE/ALTER/DROP DOMAIN; types.txt / Domains
CREATE DOMAIN r0c_pgy_domain_nested_base AS integer CHECK (VALUE > 0);
CREATE DOMAIN r0c_pgy_domain_nested_child AS r0c_pgy_domain_nested_base NOT NULL CHECK (VALUE < 100);
CREATE TABLE r0c_pgy_domain_nested (id int PRIMARY KEY, v r0c_pgy_domain_nested_child);
INSERT INTO r0c_pgy_domain_nested VALUES (1,7);
SELECT v, v+1, 8::r0c_pgy_domain_nested_child FROM r0c_pgy_domain_nested ORDER BY id;
-- @error
SELECT 0::r0c_pgy_domain_nested_child;
-- @error
SELECT NULL::r0c_pgy_domain_nested_child;
-- @error
INSERT INTO r0c_pgy_domain_nested VALUES (2,100);
DROP TABLE r0c_pgy_domain_nested;
DROP DOMAIN r0c_pgy_domain_nested_child;
DROP DOMAIN r0c_pgy_domain_nested_base;
