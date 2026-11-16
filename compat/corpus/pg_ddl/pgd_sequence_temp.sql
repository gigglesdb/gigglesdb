-- case: pgd_sequence_temp
-- title: sequence temp
-- owner: E1
-- expect: compatible
-- source: Differences: temporary sequences
CREATE TEMPORARY SEQUENCE r0c_pgd_sequence_temp START WITH 4;
SELECT nextval('r0c_pgd_sequence_temp');
ALTER SEQUENCE r0c_pgd_sequence_temp RESTART WITH 20;
SELECT nextval('r0c_pgd_sequence_temp');
DROP SEQUENCE r0c_pgd_sequence_temp;
