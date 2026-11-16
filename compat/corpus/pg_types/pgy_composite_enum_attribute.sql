-- case: pgy_composite_enum_attribute
-- title: composite enum attribute
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: composite attributes of user-defined types; types.txt / Composite types
CREATE TYPE r0c_pgy_composite_enum_attribute_e AS ENUM ('z','a');
CREATE TYPE r0c_pgy_composite_enum_attribute_c AS (mood r0c_pgy_composite_enum_attribute_e);
SELECT (ROW('z'::r0c_pgy_composite_enum_attribute_e)::r0c_pgy_composite_enum_attribute_c).mood;
DROP TYPE r0c_pgy_composite_enum_attribute_c;
DROP TYPE r0c_pgy_composite_enum_attribute_e;
