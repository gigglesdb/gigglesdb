-- case: pgd_pk_interval
-- title: pk interval
-- owner: E1
-- expect: compatible
-- source: Differences: date time interval char primary keys
CREATE TABLE r0c_pgd_pk_interval (id interval PRIMARY KEY, v text);
INSERT INTO r0c_pgd_pk_interval VALUES ('1 day','first'),('2 days','second');
-- @error
INSERT INTO r0c_pgd_pk_interval VALUES ('1 day','duplicate');
SELECT * FROM r0c_pgd_pk_interval ORDER BY id;
