-- case: pgd_pk_char
-- title: pk char
-- owner: E1
-- expect: compatible
-- source: Differences: date time interval char primary keys
CREATE TABLE r0c_pgd_pk_char (id char(4) PRIMARY KEY, v text);
INSERT INTO r0c_pgd_pk_char VALUES ('ab','first'),('cd','second');
-- @error
INSERT INTO r0c_pgd_pk_char VALUES ('ab','duplicate');
SELECT * FROM r0c_pgd_pk_char ORDER BY id;
