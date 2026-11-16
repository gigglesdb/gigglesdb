-- case: pgy_domain_alter
-- title: domain alter
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: domains over domains; W5 CREATE/ALTER/DROP DOMAIN; types.txt / Domains
CREATE DOMAIN r0c_pgy_domain_alter_d AS int DEFAULT 2;
ALTER DOMAIN r0c_pgy_domain_alter_d SET NOT NULL;
ALTER DOMAIN r0c_pgy_domain_alter_d ADD CONSTRAINT r0c_pgy_domain_alter_check CHECK (VALUE > 0) NOT VALID;
ALTER DOMAIN r0c_pgy_domain_alter_d VALIDATE CONSTRAINT r0c_pgy_domain_alter_check;
CREATE TABLE r0c_pgy_domain_alter (id int PRIMARY KEY, v r0c_pgy_domain_alter_d DEFAULT 2);
INSERT INTO r0c_pgy_domain_alter (id) VALUES (1);
SELECT v FROM r0c_pgy_domain_alter ORDER BY id;
ALTER DOMAIN r0c_pgy_domain_alter_d DROP CONSTRAINT r0c_pgy_domain_alter_check;
ALTER DOMAIN r0c_pgy_domain_alter_d DROP NOT NULL;
ALTER DOMAIN r0c_pgy_domain_alter_d SET DEFAULT 3;
SELECT 3::r0c_pgy_domain_alter_d;
ALTER DOMAIN r0c_pgy_domain_alter_d DROP DEFAULT;
ALTER DOMAIN r0c_pgy_domain_alter_d OWNER TO postgres;
ALTER DOMAIN r0c_pgy_domain_alter_d RENAME TO r0c_pgy_domain_alter_renamed;
CREATE SCHEMA r0c_pgy_domain_alter_s;
ALTER DOMAIN r0c_pgy_domain_alter_renamed SET SCHEMA r0c_pgy_domain_alter_s;
SELECT 4::r0c_pgy_domain_alter_s.r0c_pgy_domain_alter_renamed;
DROP TABLE r0c_pgy_domain_alter;
DROP DOMAIN r0c_pgy_domain_alter_s.r0c_pgy_domain_alter_renamed;
DROP SCHEMA r0c_pgy_domain_alter_s;
