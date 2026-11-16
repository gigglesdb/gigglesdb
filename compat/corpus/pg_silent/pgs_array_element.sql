-- case: pgs_array_element
-- title: Array concatenation with typed scalar
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / Array concatenation with typed scalar
SELECT ARRAY['a','b'] || 'c'::text, 'z'::text || ARRAY['a','b'];
