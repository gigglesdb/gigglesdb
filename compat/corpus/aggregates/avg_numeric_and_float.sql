-- case: avg_numeric_and_float
-- title: Average integer numeric and floating values
-- owner: E3
-- expect: compatible
-- Numeric values preserve the PG numeric(10,3) input scales without typmod DDL.
CREATE TABLE r0c_avg_numeric_and_float_t(id int PRIMARY KEY,v int,n numeric,f float8);
INSERT INTO r0c_avg_numeric_and_float_t VALUES(1,1,1.500,1),(2,2,2.250,2),(3,4,NULL,4),(4,NULL,NULL,NULL);
SELECT avg(v),avg(n),avg(f),avg(f::float4) FROM r0c_avg_numeric_and_float_t;
SELECT avg(v),avg(n),avg(f) FROM r0c_avg_numeric_and_float_t WHERE id<0;