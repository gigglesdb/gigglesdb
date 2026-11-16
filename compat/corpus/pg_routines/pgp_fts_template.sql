-- case: pgp_fts_template
-- title: Custom text search template dictionary and lexize
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE TEXT SEARCH TEMPLATE r0c_pgp_fts_template (INIT=pg_catalog.dsimple_init,LEXIZE=pg_catalog.dsimple_lexize);
CREATE TEXT SEARCH DICTIONARY r0c_pgp_fts_template_dict (TEMPLATE=r0c_pgp_fts_template);
SELECT ts_lexize('r0c_pgp_fts_template_dict','HeLLo');
DROP TEXT SEARCH DICTIONARY r0c_pgp_fts_template_dict;
ALTER TEXT SEARCH TEMPLATE r0c_pgp_fts_template RENAME TO r0c_pgp_fts_template_renamed;
DROP TEXT SEARCH TEMPLATE r0c_pgp_fts_template_renamed;
