-- case: pgd_security_label
-- title: security label
-- owner: E1
-- expect: compatible
-- source: W4 SECURITY LABEL without provider
CREATE TABLE r0c_pgd_security_label (id int PRIMARY KEY, v int);
-- @error
SECURITY LABEL ON TABLE r0c_pgd_security_label IS 'confidential';
SELECT count(*) FROM r0c_pgd_security_label;
