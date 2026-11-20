-- case: pgd_drop_atomic
-- title: Multi-name DROP TABLE preserves earlier tables on failure
-- owner: unowned
-- expect: compatible
-- source: PostgreSQL 18.6 dropatomic oracle
CREATE TABLE pgd_drop_atomic_a (id int PRIMARY KEY, body text);
INSERT INTO pgd_drop_atomic_a VALUES (1, 'kept');
CREATE TABLE pgd_drop_atomic_b (id int PRIMARY KEY);
CREATE TABLE pgd_drop_atomic_child (id int PRIMARY KEY REFERENCES pgd_drop_atomic_b(id));
-- @error
DROP TABLE pgd_drop_atomic_a, pgd_drop_atomic_b;
SELECT * FROM pgd_drop_atomic_a;
-- @error
DROP TABLE pgd_drop_atomic_a, pgd_drop_atomic_b RESTRICT;
SELECT * FROM pgd_drop_atomic_a;
-- @error
DROP TABLE pgd_drop_atomic_a, pgd_drop_atomic_missing;
SELECT * FROM pgd_drop_atomic_a;
-- @error
DROP TABLE IF EXISTS pgd_drop_atomic_missing, pgd_drop_atomic_a, pgd_drop_atomic_b;
SELECT * FROM pgd_drop_atomic_a;
CREATE TABLE pgd_drop_atomic_dup (id int PRIMARY KEY);
DROP TABLE pgd_drop_atomic_dup, pgd_drop_atomic_dup;
BEGIN;
-- @error
DROP TABLE pgd_drop_atomic_a, pgd_drop_atomic_b;
ROLLBACK;
SELECT * FROM pgd_drop_atomic_a;
BEGIN;
DROP TABLE pgd_drop_atomic_a, pgd_drop_atomic_b CASCADE;
ROLLBACK;
SELECT * FROM pgd_drop_atomic_a;
DROP TABLE pgd_drop_atomic_a, pgd_drop_atomic_b CASCADE;
SELECT * FROM pgd_drop_atomic_child;
DROP TABLE pgd_drop_atomic_child;
CREATE ROLE pgd_drop_atomic_owner;
GRANT CREATE ON SCHEMA public TO pgd_drop_atomic_owner;
SET ROLE pgd_drop_atomic_owner;
CREATE TABLE pgd_drop_atomic_mine (id int PRIMARY KEY, body text);
INSERT INTO pgd_drop_atomic_mine VALUES (1, 'owned');
RESET ROLE;
CREATE TABLE pgd_drop_atomic_other (id int PRIMARY KEY);
SET ROLE pgd_drop_atomic_owner;
-- @error
DROP TABLE pgd_drop_atomic_mine, pgd_drop_atomic_other;
SELECT * FROM pgd_drop_atomic_mine;
RESET ROLE;
DROP TABLE pgd_drop_atomic_mine, pgd_drop_atomic_other;
REVOKE CREATE ON SCHEMA public FROM pgd_drop_atomic_owner;
DROP ROLE pgd_drop_atomic_owner;
