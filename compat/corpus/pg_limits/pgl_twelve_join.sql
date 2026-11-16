-- case: pgl_twelve_join
-- title: Twelve-table equijoin completes
-- owner: E3
-- expect: compatible
-- source: Queries: more than 8 joined tables
CREATE TABLE r0c_pgl_twelve_join_t1 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t1 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t2 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t2 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t3 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t3 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t4 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t4 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t5 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t5 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t6 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t6 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t7 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t7 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t8 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t8 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t9 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t9 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t10 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t10 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t11 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t11 VALUES (1), (2);
CREATE TABLE r0c_pgl_twelve_join_t12 (id integer PRIMARY KEY);
INSERT INTO r0c_pgl_twelve_join_t12 VALUES (1), (2);
SELECT t1.id FROM r0c_pgl_twelve_join_t1 t1 JOIN r0c_pgl_twelve_join_t2 t2 ON t2.id = t1.id JOIN r0c_pgl_twelve_join_t3 t3 ON t3.id = t1.id JOIN r0c_pgl_twelve_join_t4 t4 ON t4.id = t1.id JOIN r0c_pgl_twelve_join_t5 t5 ON t5.id = t1.id JOIN r0c_pgl_twelve_join_t6 t6 ON t6.id = t1.id JOIN r0c_pgl_twelve_join_t7 t7 ON t7.id = t1.id JOIN r0c_pgl_twelve_join_t8 t8 ON t8.id = t1.id JOIN r0c_pgl_twelve_join_t9 t9 ON t9.id = t1.id JOIN r0c_pgl_twelve_join_t10 t10 ON t10.id = t1.id JOIN r0c_pgl_twelve_join_t11 t11 ON t11.id = t1.id JOIN r0c_pgl_twelve_join_t12 t12 ON t12.id = t1.id ORDER BY t1.id;
DROP TABLE r0c_pgl_twelve_join_t1;
DROP TABLE r0c_pgl_twelve_join_t2;
DROP TABLE r0c_pgl_twelve_join_t3;
DROP TABLE r0c_pgl_twelve_join_t4;
DROP TABLE r0c_pgl_twelve_join_t5;
DROP TABLE r0c_pgl_twelve_join_t6;
DROP TABLE r0c_pgl_twelve_join_t7;
DROP TABLE r0c_pgl_twelve_join_t8;
DROP TABLE r0c_pgl_twelve_join_t9;
DROP TABLE r0c_pgl_twelve_join_t10;
DROP TABLE r0c_pgl_twelve_join_t11;
DROP TABLE r0c_pgl_twelve_join_t12;
