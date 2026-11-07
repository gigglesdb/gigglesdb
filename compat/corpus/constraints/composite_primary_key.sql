-- case: composite_primary_key
-- title: Composite primary key
-- owner: E1
-- expect: compatible
-- primary-key exception: The composite key is the feature under test; retain its two columns.
CREATE TABLE r0c_composite_primary_key_t (a int, b int, PRIMARY KEY (a, b));
INSERT INTO r0c_composite_primary_key_t VALUES (1, 1);
INSERT INTO r0c_composite_primary_key_t VALUES (1, 2);
-- @error
-- expect-sqlstate: 23505
INSERT INTO r0c_composite_primary_key_t VALUES (1, 1);
SELECT a, b FROM r0c_composite_primary_key_t ORDER BY a, b;
