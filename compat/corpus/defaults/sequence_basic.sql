-- case: sequence_basic
-- title: Advance inspect and reset a sequence deterministically
-- owner: E1
-- expect: compatible

CREATE SEQUENCE r0c_sequence_basic_s START WITH 5 INCREMENT BY 2;
SELECT nextval('r0c_sequence_basic_s') AS first_value;
SELECT currval('r0c_sequence_basic_s') AS current_value;
SELECT setval('r0c_sequence_basic_s', 20, true) AS reset_value;
SELECT nextval('r0c_sequence_basic_s') AS next_value;
