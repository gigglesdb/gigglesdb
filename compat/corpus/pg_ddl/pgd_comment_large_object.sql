-- case: pgd_comment_large_object
-- title: comment large object
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT ON LARGE OBJECT
SELECT lo_from_bytea(1900040002,decode('ab','hex'))=1900040002 AS created;
COMMENT ON LARGE OBJECT 1900040002 IS 'payload';
SELECT obj_description(1900040002,'pg_largeobject');
COMMENT ON LARGE OBJECT 1900040002 IS NULL;
SELECT obj_description(1900040002,'pg_largeobject') IS NULL;
SELECT lo_unlink(1900040002);
