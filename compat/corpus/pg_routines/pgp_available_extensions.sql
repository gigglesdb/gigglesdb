-- case: pgp_available_extensions
-- title: Specific available extension names without version dependency
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
SELECT name,installed_version IS NULL AS uninstalled FROM pg_available_extensions WHERE name IN ('pgcrypto','citext','hstore','pg_trgm','uuid-ossp','btree_gist','tablefunc','unaccent','fuzzystrmatch','intarray','ltree','cube','earthdistance','pg_stat_statements','postgres_fdw','file_fdw') ORDER BY name;
