-- case: pgd_comment_ts_parser
-- title: comment ts parser
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE TEXT SEARCH PARSER r0c_pgd_comment_ts_parser (START=prsd_start,GETTOKEN=prsd_nexttoken,END=prsd_end,LEXTYPES=prsd_lextype,HEADLINE=prsd_headline);
COMMENT ON TEXT SEARCH PARSER r0c_pgd_comment_ts_parser IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_ts_parser WHERE prsname='r0c_pgd_comment_ts_parser'),'pg_ts_parser');
COMMENT ON TEXT SEARCH PARSER r0c_pgd_comment_ts_parser IS NULL;
SELECT obj_description((SELECT oid FROM pg_ts_parser WHERE prsname='r0c_pgd_comment_ts_parser'),'pg_ts_parser') IS NULL AS cleared;
DROP TEXT SEARCH PARSER r0c_pgd_comment_ts_parser;
