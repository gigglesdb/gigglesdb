-- case: pgp_fts_parser
-- title: Custom text search parser using built in callbacks
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE TEXT SEARCH PARSER r0c_pgp_fts_parser (START=pg_catalog.prsd_start,GETTOKEN=pg_catalog.prsd_nexttoken,END=pg_catalog.prsd_end,HEADLINE=pg_catalog.prsd_headline,LEXTYPES=pg_catalog.prsd_lextype);
SELECT tokid,token FROM ts_parse('r0c_pgp_fts_parser','hello world') ORDER BY tokid,token;
ALTER TEXT SEARCH PARSER r0c_pgp_fts_parser RENAME TO r0c_pgp_fts_parser_renamed;
DROP TEXT SEARCH PARSER r0c_pgp_fts_parser_renamed;
