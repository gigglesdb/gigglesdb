-- case: pgy_boolean_input
-- title: boolean input
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
SELECT 'true'::boolean,'TRUE'::boolean,'t'::boolean,'yes'::boolean,'y'::boolean,'on'::boolean,'1'::boolean;
SELECT 'false'::boolean,'FALSE'::boolean,'f'::boolean,'no'::boolean,'n'::boolean,'off'::boolean,'0'::boolean;
-- @error
SELECT 'maybe'::boolean;
SELECT true::int, 0::boolean, 2::boolean;
