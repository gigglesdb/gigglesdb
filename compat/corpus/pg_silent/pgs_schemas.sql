-- case: pgs_schemas
-- title: Current schemas output format
-- owner: G
-- expect: compatible
-- source: Differences: Read first / Current schemas output format
SELECT current_schemas(false), current_schemas(true);
CREATE SCHEMA r0c_pgs_schemas;
SET search_path TO r0c_pgs_schemas, public;
SELECT current_schemas(false), current_schemas(true);
RESET search_path;
DROP SCHEMA r0c_pgs_schemas;
