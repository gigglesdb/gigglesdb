-- DD-0068: capture the complete installed catalog, not a hand-picked name list.
SET search_path = pg_catalog;
SELECT jsonb_build_object(
  'server_version_num', current_setting('server_version_num')::integer,
  'server_version', version(),
  'settings', (SELECT jsonb_object_agg(name, setting ORDER BY name) FROM pg_settings
    WHERE name IN ('server_encoding', 'lc_collate', 'lc_ctype', 'TimeZone',
                   'DateStyle', 'IntervalStyle', 'standard_conforming_strings')),
  'routines', (SELECT jsonb_agg(to_jsonb(r) ORDER BY r.identity) FROM (
    SELECT format('%I.%I(%s)/%s', n.nspname, p.proname,
                  oidvectortypes(p.proargtypes), p.prokind) AS identity,
           n.nspname AS schema, to_jsonb(p) AS catalog,
           pg_get_function_arguments(p.oid) AS arguments,
           pg_get_function_result(p.oid) AS result,
           l.lanname AS language, e.extname AS extension, e.extversion AS extension_version
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_language l ON l.oid = p.prolang
    LEFT JOIN pg_depend d ON d.classid = 'pg_proc'::regclass AND d.objid = p.oid
      AND d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e'
    LEFT JOIN pg_extension e ON e.oid = d.refobjid
  ) r),
  'types', (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_type t),
  'casts', (SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_cast c),
  'operators', (SELECT jsonb_agg(to_jsonb(o) ORDER BY o.oid) FROM pg_operator o),
  'aggregates', (SELECT jsonb_agg(to_jsonb(a) ORDER BY a.aggfnoid) FROM pg_aggregate a),
  'namespaces', (SELECT jsonb_agg(to_jsonb(n) ORDER BY n.oid) FROM pg_namespace n),
  'extensions', (SELECT jsonb_agg(to_jsonb(e) ORDER BY e.oid) FROM pg_extension e)
);
