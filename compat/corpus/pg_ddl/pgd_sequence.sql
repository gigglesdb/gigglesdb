-- case: pgd_sequence
-- title: sequence
-- owner: E1
-- expect: compatible
-- source: Differences: ALTER SEQUENCE RENAME temporary sequences
CREATE TABLE r0c_pgd_sequence (id int PRIMARY KEY, v int);
CREATE SEQUENCE r0c_pgd_sequence_s START WITH 3 INCREMENT BY 2;
SELECT nextval('r0c_pgd_sequence_s');
ALTER SEQUENCE r0c_pgd_sequence_s RENAME TO r0c_pgd_sequence_new;
ALTER SEQUENCE r0c_pgd_sequence_new RESTART WITH 10;
ALTER SEQUENCE r0c_pgd_sequence_new OWNED BY r0c_pgd_sequence.id;
ALTER SEQUENCE r0c_pgd_sequence_new INCREMENT BY 5 MINVALUE 1 MAXVALUE 100 CACHE 1 CYCLE;
SELECT nextval('r0c_pgd_sequence_new'),currval('r0c_pgd_sequence_new');
ALTER SEQUENCE r0c_pgd_sequence_new OWNED BY NONE;
DROP SEQUENCE r0c_pgd_sequence_new;
