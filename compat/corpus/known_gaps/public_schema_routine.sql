-- case: public_schema_routine
-- title: Public schema routine
-- owner: E2
-- expect: compatible
SELECT pg_catalog.quote_ident('a b') AS q;
-- @error
-- expect-sqlstate: 42883
SELECT public.quote_ident('a');
