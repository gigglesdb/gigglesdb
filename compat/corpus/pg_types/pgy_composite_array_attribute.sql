-- case: pgy_composite_array_attribute
-- title: composite array attribute
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: composite attributes of user-defined types; types.txt / Composite types
CREATE TYPE r0c_pgy_composite_array_attribute_pair AS (a int);
CREATE TYPE r0c_pgy_composite_array_attribute_outer AS (items r0c_pgy_composite_array_attribute_pair[]);
SELECT (ROW(ARRAY[ROW(1)::r0c_pgy_composite_array_attribute_pair])::r0c_pgy_composite_array_attribute_outer).items;
DROP TYPE r0c_pgy_composite_array_attribute_outer;
DROP TYPE r0c_pgy_composite_array_attribute_pair;
