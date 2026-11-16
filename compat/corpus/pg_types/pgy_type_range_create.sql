-- case: pgy_type_range_create
-- title: type range create
-- owner: E2
-- expect: compatible
-- source: W5 CREATE/ALTER/DROP TYPE; types.txt
CREATE TYPE r0c_pgy_type_range_create_r AS RANGE (subtype=int4,subtype_diff=int4range_subdiff,multirange_type_name=r0c_pgy_type_range_create_m);
CREATE TABLE r0c_pgy_type_range_create (id int PRIMARY KEY,v r0c_pgy_type_range_create_r);
INSERT INTO r0c_pgy_type_range_create VALUES (1,'[1,5)');
SELECT v,v @> 2,lower(v),upper(v), r0c_pgy_type_range_create_m(v) FROM r0c_pgy_type_range_create ORDER BY v,id;
ALTER TYPE r0c_pgy_type_range_create_r RENAME TO r0c_pgy_type_range_create_new;
SELECT '[3,9)'::r0c_pgy_type_range_create_new;
DROP TABLE r0c_pgy_type_range_create;
DROP TYPE r0c_pgy_type_range_create_new;
SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='r0c_pgy_type_range_create_m');
