-- case: pgp_ext_earth
-- title: Earthdistance extension fixed great circle properties
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by control files.
CREATE EXTENSION cube;
CREATE EXTENSION earthdistance;
SELECT round(earth_distance(ll_to_earth(0,0),ll_to_earth(0,0))::numeric,3);
DROP EXTENSION earthdistance;
DROP EXTENSION cube;
