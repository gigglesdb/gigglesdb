-- case: pgy_array_constructors
-- title: array constructors
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
SELECT ARRAY[1,2,NULL], ARRAY[ARRAY[1,2],ARRAY[3,4]], ARRAY[]::int[], ARRAY['a','b']::varchar[];
