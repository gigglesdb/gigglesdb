-- case: pgy_array_slices
-- title: array slices
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_slices (id int PRIMARY KEY, v int[]);
INSERT INTO r0c_pgy_array_slices VALUES (1,'{1,2,3,4}');
UPDATE r0c_pgy_array_slices SET v[2:3] = ARRAY[8,9] WHERE id=1;
SELECT v, v[2:3], v[:2], v[3:], v[99] FROM r0c_pgy_array_slices ORDER BY id;
