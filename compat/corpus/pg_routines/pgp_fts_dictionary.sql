-- case: pgp_fts_dictionary
-- title: Text search dictionary creation options and use
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE TEXT SEARCH DICTIONARY r0c_pgp_fts_dictionary (TEMPLATE=pg_catalog.simple,STOPWORDS=english);
SELECT ts_lexize('r0c_pgp_fts_dictionary','Hello');
SELECT ts_lexize('r0c_pgp_fts_dictionary','the');
ALTER TEXT SEARCH DICTIONARY r0c_pgp_fts_dictionary (STOPWORDS=english,ACCEPT=true);
ALTER TEXT SEARCH DICTIONARY r0c_pgp_fts_dictionary RENAME TO r0c_pgp_fts_dictionary_renamed;
DROP TEXT SEARCH DICTIONARY r0c_pgp_fts_dictionary_renamed;
