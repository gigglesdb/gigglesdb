-- case: pgd_large_object
-- title: large object
-- owner: E1
-- expect: compatible
-- source: W4 ALTER LARGE OBJECT
SELECT lo_from_bytea(1900040001,decode('abcd','hex')) = 1900040001 AS created;
ALTER LARGE OBJECT 1900040001 OWNER TO postgres;
COMMENT ON LARGE OBJECT 1900040001 IS 'owned test payload';
SELECT encode(lo_get(1900040001),'hex');
SELECT lo_unlink(1900040001);
-- @error
ALTER LARGE OBJECT 1900040001 OWNER TO postgres;
