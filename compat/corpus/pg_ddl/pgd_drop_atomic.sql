-- case: pgd_drop_atomic
-- title: Multi-name DROP TABLE preserves earlier tables on failure
-- owner: unowned
-- expect: compatible
-- source: PostgreSQL 18.6 dropatomic oracle
CREATE TABLE r0c_drop_atomic_a (id int PRIMARY KEY, body text);
INSERT INTO r0c_drop_atomic_a VALUES (1, 'kept');
CREATE TABLE r0c_drop_atomic_b (id int PRIMARY KEY);
CREATE TABLE r0c_drop_atomic_child (id int REFERENCES r0c_drop_atomic_b(id));
-- @error
DROP TABLE r0c_drop_atomic_a, r0c_drop_atomic_b;
SELECT * FROM r0c_drop_atomic_a;
-- @error
DROP TABLE r0c_drop_atomic_a, r0c_drop_atomic_b RESTRICT;
SELECT * FROM r0c_drop_atomic_a;
-- @error
DROP TABLE r0c_drop_atomic_a, r0c_drop_atomic_missing;
SELECT * FROM r0c_drop_atomic_a;
-- @error
DROP TABLE IF EXISTS r0c_drop_atomic_missing, r0c_drop_atomic_a, r0c_drop_atomic_b;
SELECT * FROM r0c_drop_atomic_a;
CREATE TABLE r0c_drop_atomic_dup (id int);
DROP TABLE r0c_drop_atomic_dup, r0c_drop_atomic_dup;
BEGIN;
-- @error
DROP TABLE r0c_drop_atomic_a, r0c_drop_atomic_b;
ROLLBACK;
SELECT * FROM r0c_drop_atomic_a;
BEGIN;
DROP TABLE r0c_drop_atomic_a, r0c_drop_atomic_b CASCADE;
ROLLBACK;
SELECT * FROM r0c_drop_atomic_a;
DROP TABLE r0c_drop_atomic_a, r0c_drop_atomic_b CASCADE;
SELECT * FROM r0c_drop_atomic_child;
DROP TABLE r0c_drop_atomic_child;
CREATE ROLE r0c_drop_atomic_owner;
GRANT CREATE ON SCHEMA public TO r0c_drop_atomic_owner;
SET ROLE r0c_drop_atomic_owner;
CREATE TABLE r0c_drop_atomic_mine (id int PRIMARY KEY, body text);
INSERT INTO r0c_drop_atomic_mine VALUES (1, 'owned');
RESET ROLE;
CREATE TABLE r0c_drop_atomic_other (id int);
SET ROLE r0c_drop_atomic_owner;
-- @error
DROP TABLE r0c_drop_atomic_mine, r0c_drop_atomic_other;
SELECT * FROM r0c_drop_atomic_mine;
RESET ROLE;
DROP TABLE r0c_drop_atomic_mine, r0c_drop_atomic_other;
REVOKE CREATE ON SCHEMA public FROM r0c_drop_atomic_owner;
DROP ROLE r0c_drop_atomic_owner;
