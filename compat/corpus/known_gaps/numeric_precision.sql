-- case: numeric_precision
-- title: Numeric precision
-- owner: E2
-- expect: compatible
CREATE TABLE r0c_numeric_precision_t (id int PRIMARY KEY, value numeric(10,2));
INSERT INTO r0c_numeric_precision_t VALUES (1, 1.005);
INSERT INTO r0c_numeric_precision_t VALUES (2, 12345678.9);
SELECT value FROM r0c_numeric_precision_t ORDER BY value;
