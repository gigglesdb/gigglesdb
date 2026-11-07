-- case: fk_violation
-- title: Fk violation
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_fk_violation_t (id int PRIMARY KEY);
CREATE TABLE r0c_fk_violation_u (id int PRIMARY KEY, parent_id int REFERENCES r0c_fk_violation_t(id));
-- @error
-- expect-sqlstate: 23503
INSERT INTO r0c_fk_violation_u VALUES (1, 99);
SELECT count(*) FROM r0c_fk_violation_u;
