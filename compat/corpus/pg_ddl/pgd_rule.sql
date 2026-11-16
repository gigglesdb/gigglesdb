-- case: pgd_rule
-- title: Rule audit insert and rename
-- owner: E1
-- expect: compatible
-- source: W4 CREATE ALTER DROP RULE
CREATE TABLE r0c_pgd_rule (id int PRIMARY KEY,v int);
CREATE TABLE r0c_pgd_rule_audit (id int PRIMARY KEY,v int);
CREATE RULE r0c_pgd_rule_rule AS ON INSERT TO r0c_pgd_rule DO ALSO INSERT INTO r0c_pgd_rule_audit VALUES(NEW.id,NEW.v);
ALTER RULE r0c_pgd_rule_rule ON r0c_pgd_rule RENAME TO r0c_pgd_rule_renamed;
INSERT INTO r0c_pgd_rule VALUES(1,8);
SELECT * FROM r0c_pgd_rule_audit ORDER BY id;
DROP RULE r0c_pgd_rule_renamed ON r0c_pgd_rule;
INSERT INTO r0c_pgd_rule VALUES(2,9);
SELECT * FROM r0c_pgd_rule ORDER BY id;
SELECT * FROM r0c_pgd_rule_audit ORDER BY id;
DROP TABLE r0c_pgd_rule_audit;
