-- case: pgd_comment_unavailable
-- title: comment unavailable
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT ON objects unavailable in the pinned environment
-- @error
COMMENT ON TABLESPACE r0c_pgd_comment_unavailable IS 'missing tablespace';
-- @error
COMMENT ON EXTENSION r0c_pgd_comment_unavailable IS 'missing extension';
-- @error
COMMENT ON LANGUAGE r0c_pgd_comment_unavailable IS 'missing language';
-- @error
COMMENT ON TRANSFORM FOR int LANGUAGE sql IS 'missing transform';
SELECT 1 AS survived;
