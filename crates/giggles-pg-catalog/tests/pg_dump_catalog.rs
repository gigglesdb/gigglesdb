use giggles_pg_catalog::*;

fn run(sql: &str) -> Result<ResultSet, Error> {
    execute(
        &plan(sql)?.expect("catalog query"),
        &CatalogSnapshot::default(),
        &[],
    )
}

#[test]
fn pg18_dump_sequence_inventory_is_typed_and_empty() {
    let result = run("SELECT seqrelid, format_type(seqtypid, NULL), seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, last_value, is_called FROM pg_catalog.pg_sequence, pg_get_sequence_data(seqrelid) ORDER BY seqrelid").unwrap();
    assert!(result.rows.is_empty());
    assert_eq!(result.fields[8].data_type, PgType::Int8);
    assert_eq!(result.fields[9].data_type, PgType::Bool);
    assert!(plan("SELECT * FROM pg_get_sequence_data(123::oid)").is_err());
    assert!(!matches!(
        plan("SELECT * FROM public.pg_get_sequence_data(123::oid)"),
        Ok(Some(_))
    ));
}

#[test]
fn pg18_dump_not_null_constraint_join_and_deparse() {
    let snapshot = CatalogSnapshot {
        tables: vec![TableDef {
            id: 10,
            name: "version_probe".into(),
            primary_key: vec![0],
            columns: vec![
                ColumnDef {
                    id: 100,
                    name: "id".into(),
                    data_type: ScalarType::Integer,
                    not_null: false,
                },
                ColumnDef {
                    id: 101,
                    name: "value".into(),
                    data_type: ScalarType::Text,
                    not_null: true,
                },
                ColumnDef {
                    id: 102,
                    name: "optional".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
            ],
        }],
        ..CatalogSnapshot::default()
    };
    // PG18.6 pg_dump reads these columns with this NOT NULL join condition.
    let sql = "SELECT a.attname,a.attstattarget,co.conname,co.conenforced,co.conislocal,co.coninhcount,co.connoinherit,pg_get_constraintdef(co.oid),c.relallfrozen FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid LEFT JOIN pg_constraint co ON (a.attrelid=co.conrelid AND co.contype='n' AND co.conkey=array[a.attnum]) WHERE c.relname='version_probe' AND a.attnum>0 ORDER BY a.attnum";
    let result = execute(&plan(sql).unwrap().unwrap(), &snapshot, &[]).unwrap();
    assert_eq!(result.fields[8].data_type, PgType::Int4);
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Text("id".into()),
                Value::Null,
                Value::Text("version_probe_id_not_null".into()),
                Value::Bool(true),
                Value::Bool(true),
                Value::Int(0),
                Value::Bool(false),
                Value::Text("NOT NULL id".into()),
                Value::Int(0)
            ],
            vec![
                Value::Text("value".into()),
                Value::Null,
                Value::Text("version_probe_value_not_null".into()),
                Value::Bool(true),
                Value::Bool(true),
                Value::Int(0),
                Value::Bool(false),
                Value::Text("NOT NULL value".into()),
                Value::Int(0)
            ],
            vec![
                Value::Text("optional".into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int(0)
            ],
        ]
    );
}

#[test]
fn namespace_inventory_computes_defaults_without_fabricating_acl_metadata() {
    let result = run("SELECT n.tableoid, n.oid, n.nspname, n.nspowner, n.nspacl, acldefault('n', n.nspowner) AS acldefault FROM pg_namespace n").unwrap();
    assert_eq!(result.fields[4].data_type, PgType::AclItemArray);
    assert_eq!(result.fields[5].data_type, PgType::AclItemArray);
    assert_eq!(result.fields[5].data_type.oid(), 1034);
    assert_eq!(result.rows.len(), 3);
    for row in result.rows {
        let owner = if row[3] == Value::Int(6171) {
            "pg_database_owner"
        } else {
            assert_eq!(row[3], Value::Int(10));
            "giggles"
        };
        assert_eq!(row[0], Value::Int(2615));
        assert_eq!(row[4], Value::Null);
        assert_eq!(
            row[5],
            Value::Array(vec![Value::Text(format!("{owner}=UC/{owner}"))])
        );
    }
}

#[test]
fn schema_acl_defaults_are_strict_and_resolve_owner_names() {
    let result = run("SELECT pg_catalog.acldefault(NULL,10), acldefault('n',NULL), acldefault('n',6171), acldefault('n',4294967295), acldefault('n',0)").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Null,
            Value::Null,
            Value::Array(vec![Value::Text(
                "pg_database_owner=UC/pg_database_owner".into()
            )]),
            Value::Array(vec![Value::Text("4294967295=UC/4294967295".into())]),
            Value::Array(vec![Value::Text("=UC/0".into())]),
        ]]
    );
    assert!(
        result
            .fields
            .iter()
            .all(|f| f.data_type == PgType::AclItemArray)
    );
    assert!(matches!(
        run("SELECT acldefault('x',10)"),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        plan("SELECT acldefault('n')"),
        Err(Error::Unsupported)
    ));
}

#[test]
fn schema_acl_defaults_quote_acl_role_names() {
    // ACL quoting differs from SQL identifier quoting: uppercase, leading
    // digits and keywords are safe; non-ASCII and punctuation require quotes.
    for (name, encoded) in [
        ("SELECT", "SELECT"),
        ("123owner", "123owner"),
        ("a b", "\"a b\""),
        ("a\"b", "\"a\"\"b\""),
        ("a\\b", "\"a\\b\""),
        ("é", "\"é\""),
    ] {
        let mut snapshot = CatalogSnapshot::default();
        snapshot.roles[0].name = name.into();
        let result = execute(
            &plan("SELECT acldefault('n',10)").unwrap().unwrap(),
            &snapshot,
            &[],
        )
        .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Value::Array(vec![Value::Text(format!(
                "{encoded}=UC/{encoded}"
            ))])]],
            "{name}"
        );
    }
}

#[test]
fn extension_inventory_matches_fresh_postgres() {
    let result = run("SELECT x.tableoid, x.oid, x.extname, n.nspname, x.extrelocatable, x.extversion, x.extconfig, x.extcondition FROM pg_extension x JOIN pg_namespace n ON n.oid = x.extnamespace").unwrap();
    // PostgreSQL 18.6, fresh database: plpgsql is installed by default.
    assert_eq!(result.rows, vec![vec![
        Value::Int(3079), Value::Int(13643), Value::Text("plpgsql".into()),
        Value::Text("pg_catalog".into()), Value::Bool(false),
        Value::Text("1.0".into()), Value::Null, Value::Null,
    ]]);
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        vec![
            PgType::Oid,
            PgType::Oid,
            PgType::Name,
            PgType::Name,
            PgType::Bool,
            PgType::Text,
            PgType::OidArray,
            PgType::TextArray
        ]
    );
}

#[test]
fn tableoid_binds_as_oid_and_preserves_outer_join_nulls() {
    let result = run("SELECT n.tableoid,n.oid,n.nspname,x.tableoid FROM pg_namespace n LEFT JOIN pg_extension x ON n.oid=x.extnamespace WHERE n.nspname='public'").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(2615),
            Value::Int(2200),
            Value::Text("public".into()),
            Value::Null
        ]]
    );
    assert_eq!(result.fields[0].data_type, PgType::Oid);
    assert_eq!(result.fields[3].data_type, PgType::Oid);
    assert!(matches!(
        plan("SELECT tableoid FROM pg_namespace n JOIN pg_extension x ON true"),
        Err(Error::AmbiguousColumn)
    ));
    assert!(matches!(
        plan("SELECT pg_namespace.tableoid FROM pg_namespace n"),
        Err(Error::UnknownColumn)
    ));
    assert!(matches!(
        plan("SELECT n.tableoid FROM (SELECT oid FROM pg_namespace) n"),
        Err(Error::UnknownColumn)
    ));
}

#[test]
fn tableoid_is_hidden_from_stars_views_and_ordinary_attributes() {
    let result = run("SELECT * FROM pg_extension").unwrap();
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "oid",
            "extname",
            "extowner",
            "extnamespace",
            "extrelocatable",
            "extversion",
            "extconfig",
            "extcondition"
        ]
    );
    let comma = run("SELECT * FROM pg_namespace n, pg_extension x").unwrap();
    assert!(!comma.fields.iter().any(|f| f.name == "tableoid"));
    let natural = run("SELECT * FROM pg_namespace n NATURAL JOIN pg_extension x").unwrap();
    assert!(!natural.fields.iter().any(|f| f.name == "tableoid"));
    for relation in ["pg_roles", "pg_indexes", "information_schema.tables"] {
        assert!(
            matches!(
                plan(&format!("SELECT tableoid FROM {relation}")),
                Err(Error::UnknownColumn)
            ),
            "{relation}"
        );
    }
    assert!(run("SELECT attname FROM pg_attribute WHERE attrelid=3079 AND attname='tableoid' AND attnum>0").unwrap().rows.is_empty());
}

#[test]
fn relation_dump_statistics_and_helpers() {
    let result = run("SELECT relpages,reltuples,relallvisible,relfrozenxid,relispopulated,relminmxid FROM pg_class WHERE relname='pg_class'").unwrap();
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        [
            PgType::Int4,
            PgType::Float4,
            PgType::Int4,
            PgType::Xid,
            PgType::Bool,
            PgType::Xid
        ]
    );
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(0),
            Value::Float4(giggles_pg_types::Float4::parse("-1").unwrap()),
            Value::Int(0),
            Value::Int(2),
            Value::Bool(true),
            Value::Int(1)
        ]]
    );
    let result = run("SELECT acldefault('r',10),acldefault('s',10),array_remove(ARRAY['a',NULL,'a','b'],NULL),array_remove(ARRAY['a','b','a'],'a'),array_remove((SELECT reloptions FROM pg_class WHERE relname='pg_class'),'a')").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Array(vec![Value::Text("giggles=arwdDxtm/giggles".into())]),
            Value::Array(vec![Value::Text("giggles=rwU/giggles".into())]),
            Value::Array(vec![
                Value::Text("a".into()),
                Value::Text("a".into()),
                Value::Text("b".into())
            ]),
            Value::Array(vec![Value::Text("b".into())]),
            Value::Null
        ]]
    );
}

#[test]
fn pg_dump_qualified_table_selection() {
    let result = run("SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace WHERE c.relkind OPERATOR(pg_catalog.=) ANY (array['r','S','v','m','f','p']) AND c.relname OPERATOR(pg_catalog.~) '^(pg_.*)$' COLLATE pg_catalog.default AND n.nspname OPERATOR(pg_catalog.~) '^(pg_catalog)$' COLLATE pg_catalog.default").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn full_relation_inventory() {
    let sql = r###"SELECT c.tableoid, c.oid, c.relname, c.relnamespace, c.relkind, c.reltype, c.relowner, c.relchecks, c.relhasindex, c.relhasrules, c.relpages, c.reltuples, c.relallvisible, 0 AS relallfrozen, c.relhastriggers, c.relpersistence, c.reloftype, c.relacl, acldefault(CASE WHEN c.relkind = 'S' THEN 's'::"char" ELSE 'r'::"char" END, c.relowner) AS acldefault, CASE WHEN c.relkind = 'f' THEN (SELECT ftserver FROM pg_catalog.pg_foreign_table WHERE ftrelid = c.oid) ELSE 0 END AS foreignserver, c.relfrozenxid, tc.relfrozenxid AS tfrozenxid, tc.oid AS toid, tc.relpages AS toastpages, tc.reloptions AS toast_reloptions, d.refobjid AS owning_tab, d.refobjsubid AS owning_col, tsp.spcname AS reltablespace, false AS relhasoids, c.relispopulated, c.relreplident, c.relrowsecurity, c.relforcerowsecurity, c.relminmxid, tc.relminmxid AS tminmxid, array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions, CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, am.amname, (d.deptype = 'i') IS TRUE AS is_identity_sequence, c.relispartition AS ispartition
FROM pg_class c
LEFT JOIN pg_depend d ON (c.relkind = 'S' AND d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.objsubid = 0 AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i'))
LEFT JOIN pg_tablespace tsp ON (tsp.oid = c.reltablespace)
LEFT JOIN pg_am am ON (c.relam = am.oid)
LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid AND tc.relkind = 't' AND c.relkind <> 'p')
WHERE c.relkind IN ('r', 'S', 'v', 'c', 'm', 'f', 'p')
ORDER BY c.oid
"###;
    let result = run(sql);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn pg_dump_routine_inventory() {
    let result = run(r###"SELECT p.tableoid, p.oid, p.proname, p.prolang, p.pronargs, p.proargtypes, p.prorettype, p.proacl, acldefault('f', p.proowner) AS acldefault, p.pronamespace, p.proowner FROM pg_proc p LEFT JOIN pg_init_privs pip ON (p.oid = pip.objoid AND pip.classoid = 'pg_proc'::regclass AND pip.objsubid = 0) WHERE p.prokind <> 'a'
  AND NOT EXISTS (SELECT 1 FROM pg_depend WHERE classid = 'pg_proc'::regclass AND objid = p.oid AND deptype = 'i')
  AND (
  pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')
  OR EXISTS (SELECT 1 FROM pg_cast
  WHERE pg_cast.oid > 16383
  AND p.oid = pg_cast.castfunc)
  OR EXISTS (SELECT 1 FROM pg_transform
  WHERE pg_transform.oid > 16383 AND
  (p.oid = pg_transform.trffromsql
  OR p.oid = pg_transform.trftosql))
  OR p.proacl IS DISTINCT FROM pip.initprivs)"###).unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn exists_and_distinct_preserve_nulls_and_correlations() {
    let r=run("SELECT EXISTS(SELECT 1/0), NOT EXISTS(SELECT 1 WHERE false), NULL IS DISTINCT FROM NULL, 1 IS DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM 1").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true)
        ]]
    );
    let r=run("SELECT n.nspname FROM pg_namespace n WHERE EXISTS (SELECT 1 FROM pg_namespace x WHERE x.oid=n.oid AND x.nspname='public')").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Text("public".into())]]);
}

#[test]
fn pg_dump_type_inventory() {
    let r = run(r###"SELECT tableoid, oid, typname, typnamespace, typacl, acldefault('T', typowner) AS acldefault, typowner, typelem, typrelid, typarray, CASE WHEN typrelid = 0 THEN ' '::"char" ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, typtype, typisdefined, typname[0] = '_' AND typelem != 0 AND (SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray FROM pg_type"###).unwrap();
    assert!(!r.rows.is_empty());
}

#[test]
fn name_subscripts_use_zero_based_padded_bytes() {
    let r=run("SELECT typname[0],typname[1],typname[63],typname[-1],typname[64] FROM pg_type WHERE typname='int4'").unwrap();
    assert!(r.fields.iter().all(|f| f.data_type == PgType::Char));
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Text("i".into()),
            Value::Text("n".into()),
            Value::Text("".into()),
            Value::Text("".into()),
            Value::Text("".into())
        ]]
    );
}

#[test]
fn pg_dump_language_inventory() {
    let r=run("SELECT tableoid,oid,lanname,lanpltrusted,lanplcallfoid,laninline,lanvalidator,lanacl,acldefault('l',lanowner) AS acldefault,lanowner FROM pg_language WHERE lanispl ORDER BY oid").unwrap();
    assert!(r.rows.is_empty());
}

#[test]
fn pg_dump_access_method_inventory_preserves_regproc() {
    let r = run(
        "SELECT tableoid,oid,amname,amtype,amhandler::pg_catalog.regproc AS amhandler FROM pg_am",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.fields[4].data_type, PgType::Regproc);
    assert_eq!(r.rows[0][4], Value::Int(3));
    assert_eq!(r.rows[1][4], Value::Int(330));
}

#[test]
fn pg_dump_text_search_inventory_is_typed_and_empty() {
    for sql in [
        "SELECT tableoid,oid,prsname,prsnamespace,prsstart::oid,prstoken::oid,prsend::oid,prsheadline::oid,prslextype::oid FROM pg_ts_parser",
        "SELECT tableoid,oid,dictname,dictnamespace,dictowner,dicttemplate,dictinitoption FROM pg_ts_dict",
        "SELECT tableoid,oid,tmplname,tmplnamespace,tmplinit::oid,tmpllexize::oid FROM pg_ts_template",
        "SELECT tableoid,oid,cfgname,cfgnamespace,cfgowner,cfgparser FROM pg_ts_config",
        "SELECT mapcfg,maptokentype,mapseqno,mapdict FROM pg_ts_config_map",
    ] {
        assert!(run(sql).unwrap().rows.is_empty(), "{sql}");
    }
}

#[test]
fn pg_dump_foreign_wrapper_inventory() {
    assert!(run(r###"SELECT tableoid, oid, fdwname, fdwowner, fdwhandler::pg_catalog.regproc, fdwvalidator::pg_catalog.regproc, fdwacl, acldefault('F', fdwowner) AS acldefault, array_to_string(ARRAY(SELECT quote_ident(option_name) || ' ' || quote_literal(option_value) FROM pg_options_to_table(fdwoptions) ORDER BY option_name), E',
    ') AS fdwoptions FROM pg_foreign_data_wrapper"###).unwrap().rows.is_empty());
}

#[test]
fn option_rows_and_sql_literal_quoting() {
    let r=run("SELECT option_name,quote_literal(option_value) FROM pg_options_to_table(ARRAY['abc=a=b','x=','bare']) ORDER BY option_name").unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Text("abc".into()), Value::Text("'a=b'".into())],
            vec![Value::Text("bare".into()), Value::Text("''".into())],
            vec![Value::Text("x".into()), Value::Text("''".into())]
        ]
    );
    let r = run("SELECT quote_literal('a''b')").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Text("'a''b'".into())]]);
}

#[test]
fn exists_ignores_multicolumn_targets_and_ordering() {
    assert_eq!(
        run("SELECT EXISTS (SELECT oid,nspname FROM pg_namespace ORDER BY 2)")
            .unwrap()
            .rows,
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn pg_dump_default_acl_inventory() {
    assert!(run(r###"SELECT oid, tableoid, defaclrole, defaclnamespace, defaclobjtype, defaclacl, CASE WHEN defaclnamespace = 0 THEN acldefault(CASE WHEN defaclobjtype = 'S' THEN 's'::"char" ELSE defaclobjtype END, defaclrole) ELSE '{}' END AS acldefault FROM pg_default_acl"###).unwrap().rows.is_empty());
}

#[test]
fn additional_default_acl_kinds_and_literal_escaping() {
    let r=run("SELECT acldefault('f',10),acldefault('T',10),acldefault('l',10),acldefault('F',10),acldefault('S',10)").unwrap();
    let acl =
        |values: &[&str]| Value::Array(values.iter().map(|s| Value::Text((*s).into())).collect());
    assert_eq!(
        r.rows,
        vec![vec![
            acl(&["=X/giggles", "giggles=X/giggles"]),
            acl(&["=U/giggles", "giggles=U/giggles"]),
            acl(&["=U/giggles", "giggles=U/giggles"]),
            acl(&["giggles=U/giggles"]),
            acl(&["giggles=U/giggles"])
        ]]
    );
    let r = run("SELECT quote_literal(chr(92)),quote_literal(NULL)").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Text(r"E'\\'".into()), Value::Null]]
    );
}

#[test]
fn pg_dump_conversion_inventory() {
    assert!(
        run("SELECT tableoid,oid,conname,connamespace,conowner FROM pg_conversion")
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn pg_dump_attribute_inventory() {
    let r=run(r###"SELECT
a.attrelid,
a.attnum,
a.attname,
a.attstattarget,
a.attstorage,
t.typstorage,
a.atthasdef,
a.attisdropped,
a.attlen,
a.attalign,
a.attislocal,
pg_catalog.format_type(t.oid, a.atttypmod) AS atttypname,
array_to_string(a.attoptions, ', ') AS attoptions,
CASE WHEN a.attcollation <> t.typcollation THEN a.attcollation ELSE 0 END AS attcollation,
pg_catalog.array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value) FROM pg_catalog.pg_options_to_table(attfdwoptions) ORDER BY option_name), E',
    ') AS attfdwoptions,
CASE WHEN a.attnotnull THEN '' ELSE NULL END AS notnull_name,
NULL AS notnull_comment,
NULL AS notnull_invalidoid,
false AS notnull_noinherit,
CASE WHEN a.attislocal THEN true
     WHEN a.attnotnull AND NOT a.attislocal THEN true
     ELSE false
END AS notnull_islocal,
a.attcompression AS attcompression,
a.attidentity,
CASE WHEN a.atthasmissing AND NOT a.attisdropped THEN a.attmissingval ELSE null END AS attmissingval,
a.attgenerated
FROM unnest('{65540,65560,65580}'::pg_catalog.oid[]) AS src(tbloid)
JOIN pg_catalog.pg_attribute a ON (src.tbloid = a.attrelid) LEFT JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
WHERE a.attnum > 0::pg_catalog.int2
ORDER BY a.attrelid, a.attnum"###).unwrap();
    assert!(r.rows.is_empty());
}

#[test]
fn pg_dump_schema_stats_partition_inventory() {
    assert!(run(r###"SELECT tableoid, oid, stxname, stxnamespace, stxowner, stxrelid, stxstattarget FROM pg_catalog.pg_statistic_ext"###).unwrap().rows.is_empty());
}

#[test]
fn pg_dump_data_stats_partition_inventory() {
    assert!(run(r###"SELECT partrelid FROM pg_partitioned_table WHERE
(SELECT c.oid FROM pg_opclass c JOIN pg_am a ON c.opcmethod = a.oid
WHERE opcname = 'enum_ops' AND opcnamespace = 'pg_catalog'::regnamespace AND amname = 'hash') = ANY(partclass)"###).unwrap().rows.is_empty());
}

#[test]
fn pg_dump_publication_inventory() {
    assert!(run(r###"SELECT p.tableoid, p.oid, p.pubname, p.pubowner, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot, 'n' AS pubgencols FROM pg_publication p"###).unwrap().rows.is_empty());
}

#[test]
fn pg_dump_subscription_inventory() {
    assert!(
        run(r###"SELECT s.tableoid, s.oid, s.subname,
 s.subowner,
 s.subconninfo, s.subslotname, s.subsynccommit,
 s.subpublications,
 s.subbinary,
 s.substream,
 'd' AS subtwophasestate,
 false AS subdisableonerr,
 't' AS subpasswordrequired,
 't' AS subrunasowner,
 'any' AS suborigin,
 NULL AS suboriginremotelsn,
 false AS subenabled,
 false AS subfailover
FROM pg_subscription s
WHERE s.subdbid = (SELECT oid FROM pg_database
                   WHERE datname = current_database())"###)
        .unwrap()
        .rows
        .is_empty()
    );
}

#[test]
fn pg_dump_schema_labels_recursive_inventory() {
    assert!(run(r###"SELECT label, provider, classoid, objoid, objsubid FROM pg_catalog.pg_seclabels ORDER BY classoid, objoid, objsubid"###).unwrap().rows.is_empty());
}

#[test]
fn pg_dump_data_labels_recursive_inventory() {
    assert!(run(r###"WITH RECURSIVE w AS ( SELECT d1.objid, d2.refobjid, c2.relkind AS refrelkind FROM pg_depend d1 JOIN pg_class c1 ON c1.oid = d1.objid AND c1.relkind = 'm' JOIN pg_rewrite r1 ON r1.ev_class = d1.objid JOIN pg_depend d2 ON d2.classid = 'pg_rewrite'::regclass AND d2.objid = r1.oid AND d2.refobjid <> d1.objid JOIN pg_class c2 ON c2.oid = d2.refobjid AND c2.relkind IN ('m','v') WHERE d1.classid = 'pg_class'::regclass UNION SELECT w.objid, d3.refobjid, c3.relkind FROM w JOIN pg_rewrite r3 ON r3.ev_class = w.refobjid JOIN pg_depend d3 ON d3.classid = 'pg_rewrite'::regclass AND d3.objid = r3.oid AND d3.refobjid <> w.refobjid JOIN pg_class c3 ON c3.oid = d3.refobjid AND c3.relkind IN ('m','v') ) SELECT 'pg_class'::regclass::oid AS classid, objid, refobjid FROM w WHERE refrelkind = 'm'"###).unwrap().rows.is_empty());
}

#[test]
fn productive_or_nullable_recursive_sources_stay_unsupported() {
    for sql in [
        "WITH RECURSIVE w AS (SELECT oid FROM pg_namespace UNION ALL SELECT oid FROM w) SELECT * FROM w",
        "WITH RECURSIVE w AS (SELECT oid FROM pg_rewrite UNION ALL SELECT n.oid FROM pg_namespace n LEFT JOIN w ON false) SELECT * FROM w",
    ] {
        assert!(matches!(plan(sql), Err(Error::Unsupported)), "{sql}");
    }
}

#[test]
fn exists_retains_parameter_types_without_evaluating_targets() {
    let p = plan("SELECT EXISTS (SELECT $1::int4 / 0)")
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Int4]);
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[Value::Int(1)])
            .unwrap()
            .rows,
        vec![vec![Value::Bool(true)]]
    );
}
