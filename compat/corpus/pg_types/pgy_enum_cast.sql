-- case: pgy_enum_cast
-- title: enum cast
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: ALTER TYPE and enum comparison with explicit cast; types.txt / Enum types
CREATE TYPE r0c_pgy_enum_cast_e AS ENUM ('z','a');
SELECT 'z'::r0c_pgy_enum_cast_e, 'z'::r0c_pgy_enum_cast_e < 'a'::r0c_pgy_enum_cast_e, ARRAY['z','a']::r0c_pgy_enum_cast_e[];
DROP TYPE r0c_pgy_enum_cast_e;
