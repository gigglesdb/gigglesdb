-- case: pgp_fts_config
-- title: Text search configuration creation mapping and use
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE TEXT SEARCH CONFIGURATION r0c_pgp_fts_config (COPY=pg_catalog.simple);
ALTER TEXT SEARCH CONFIGURATION r0c_pgp_fts_config ALTER MAPPING FOR asciiword WITH pg_catalog.english_stem;
SELECT to_tsvector('r0c_pgp_fts_config','Running cats');
ALTER TEXT SEARCH CONFIGURATION r0c_pgp_fts_config RENAME TO r0c_pgp_fts_config_renamed;
DROP TEXT SEARCH CONFIGURATION r0c_pgp_fts_config_renamed;
