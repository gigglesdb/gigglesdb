-- case: pgy_bytea_invalid
-- title: bytea invalid
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: bytea_output escape; types.txt / bytea
-- @error
SELECT '\x0'::bytea;
-- @error
SELECT '\xGG'::bytea;
-- @error
SELECT E'\\999'::bytea;
SELECT ''::bytea, '\x'::bytea, encode('plain'::bytea,'hex');
