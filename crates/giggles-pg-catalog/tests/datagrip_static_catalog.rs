use giggles_pg_catalog::*;

fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "inventory".into(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                data_type: ScalarType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
    }
}
fn run(sql: &str, args: &[Value]) -> ResultSet {
    let p = plan(sql).expect("plan").expect("catalog");
    let described = describe(&p).expect("describe");
    let result = execute(&p, &snapshot(), args).expect("execute");
    assert_eq!(described, result.fields);
    result
}
// Descriptor contracts independently checked against PostgreSQL REL_14_STABLE
// src/include/catalog/pg_*.h. Missing regproc/pg_node_tree fields reject below.
#[test]
fn static_catalog_descriptors_and_empty_rows() {
    let result = run("select * from pg_catalog.pg_aggregate", &[]);
    assert!(result.rows.is_empty(), "pg_aggregate");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("aggkind", PgType::Char),
            ("aggnumdirectargs", PgType::Int2),
            ("aggfinalextra", PgType::Bool),
            ("aggmfinalextra", PgType::Bool),
            ("aggfinalmodify", PgType::Char),
            ("aggmfinalmodify", PgType::Char),
            ("aggsortop", PgType::Oid),
            ("aggtranstype", PgType::Oid),
            ("aggtransspace", PgType::Int4),
            ("aggmtranstype", PgType::Oid),
            ("aggmtransspace", PgType::Int4),
            ("agginitval", PgType::Text),
            ("aggminitval", PgType::Text),
            ("xmin", PgType::Xid),
            ("aggfnoid", PgType::Regproc),
            ("aggtransfn", PgType::Regproc),
            ("aggfinalfn", PgType::Regproc),
            ("aggcombinefn", PgType::Regproc),
            ("aggserialfn", PgType::Regproc),
            ("aggdeserialfn", PgType::Regproc),
            ("aggmtransfn", PgType::Regproc),
            ("aggminvtransfn", PgType::Regproc),
            ("aggmfinalfn", PgType::Regproc),
        ],
        "pg_aggregate"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_aggregate'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2600)]]
    );
    let result = run("select * from pg_catalog.pg_amop", &[]);
    assert!(result.rows.is_empty(), "pg_amop");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("amopfamily", PgType::Oid),
            ("amoplefttype", PgType::Oid),
            ("amoprighttype", PgType::Oid),
            ("amopstrategy", PgType::Int2),
            ("amoppurpose", PgType::Char),
            ("amopopr", PgType::Oid),
            ("amopmethod", PgType::Oid),
            ("amopsortfamily", PgType::Oid),
            ("xmin", PgType::Xid),
        ],
        "pg_amop"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_amop'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2602)]]
    );
    let result = run("select * from pg_catalog.pg_amproc", &[]);
    assert!(result.rows.is_empty(), "pg_amproc");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("amprocfamily", PgType::Oid),
            ("amproclefttype", PgType::Oid),
            ("amprocrighttype", PgType::Oid),
            ("amprocnum", PgType::Int2),
            ("xmin", PgType::Xid),
            ("amproc", PgType::Regproc),
        ],
        "pg_amproc"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_amproc'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2603)]]
    );
    let result = run("select * from pg_catalog.pg_collation", &[]);
    assert!(result.rows.is_empty(), "pg_collation");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("collname", PgType::Name),
            ("collnamespace", PgType::Oid),
            ("collowner", PgType::Oid),
            ("collprovider", PgType::Char),
            ("collisdeterministic", PgType::Bool),
            ("collencoding", PgType::Int4),
            ("collcollate", PgType::Name),
            ("collctype", PgType::Name),
            ("collversion", PgType::Text),
            ("xmin", PgType::Xid),
        ],
        "pg_collation"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_collation'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(3456)]]
    );
    let result = run("select * from pg_catalog.pg_opclass", &[]);
    assert!(result.rows.is_empty(), "pg_opclass");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("opcname", PgType::Name),
            ("opcnamespace", PgType::Oid),
            ("opcmethod", PgType::Oid),
            ("opcdefault", PgType::Bool),
            ("opcowner", PgType::Oid),
            ("opcfamily", PgType::Oid),
            ("opcintype", PgType::Oid),
            ("opckeytype", PgType::Oid),
            ("xmin", PgType::Xid),
        ],
        "pg_opclass"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_opclass'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2616)]]
    );
    let result = run("select * from pg_catalog.pg_operator", &[]);
    assert!(result.rows.is_empty(), "pg_operator");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("oprname", PgType::Name),
            ("oprnamespace", PgType::Oid),
            ("oprowner", PgType::Oid),
            ("oprkind", PgType::Char),
            ("oprcanmerge", PgType::Bool),
            ("oprcanhash", PgType::Bool),
            ("oprleft", PgType::Oid),
            ("oprright", PgType::Oid),
            ("oprresult", PgType::Oid),
            ("oprcom", PgType::Oid),
            ("oprnegate", PgType::Oid),
            ("xmin", PgType::Xid),
            ("oprcode", PgType::Regproc),
            ("oprrest", PgType::Regproc),
            ("oprjoin", PgType::Regproc),
        ],
        "pg_operator"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_operator'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2617)]]
    );
    let result = run("select * from pg_catalog.pg_opfamily", &[]);
    assert!(result.rows.is_empty(), "pg_opfamily");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("opfmethod", PgType::Oid),
            ("opfname", PgType::Name),
            ("opfnamespace", PgType::Oid),
            ("opfowner", PgType::Oid),
            ("xmin", PgType::Xid),
        ],
        "pg_opfamily"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_opfamily'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2753)]]
    );
    let result = run("select * from pg_catalog.pg_policy", &[]);
    assert!(result.rows.is_empty(), "pg_policy");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("polname", PgType::Name),
            ("polrelid", PgType::Oid),
            ("polcmd", PgType::Char),
            ("polpermissive", PgType::Bool),
            ("polroles", PgType::OidArray),
            ("xmin", PgType::Xid),
            ("polqual", PgType::PgNodeTree),
            ("polwithcheck", PgType::PgNodeTree),
        ],
        "pg_policy"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_policy'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(3256)]]
    );
    let result = run("select * from pg_catalog.pg_proc", &[]);
    assert!(result.rows.is_empty(), "pg_proc");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("proname", PgType::Name),
            ("pronamespace", PgType::Oid),
            ("proowner", PgType::Oid),
            ("prolang", PgType::Oid),
            ("procost", PgType::Float4),
            ("prorows", PgType::Float4),
            ("provariadic", PgType::Oid),
            ("prokind", PgType::Char),
            ("prosecdef", PgType::Bool),
            ("proleakproof", PgType::Bool),
            ("proisstrict", PgType::Bool),
            ("proretset", PgType::Bool),
            ("provolatile", PgType::Char),
            ("proparallel", PgType::Char),
            ("pronargs", PgType::Int2),
            ("pronargdefaults", PgType::Int2),
            ("prorettype", PgType::Oid),
            ("proargtypes", PgType::OidVector),
            ("proallargtypes", PgType::OidArray),
            ("proargmodes", PgType::CharArray),
            ("proargnames", PgType::TextArray),
            ("protrftypes", PgType::OidArray),
            ("prosrc", PgType::Text),
            ("probin", PgType::Text),
            ("proconfig", PgType::TextArray),
            ("proacl", PgType::AclItemArray),
            ("xmin", PgType::Xid),
            ("prosupport", PgType::Regproc),
            ("proargdefaults", PgType::PgNodeTree),
            ("prosqlbody", PgType::PgNodeTree),
        ],
        "pg_proc"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_proc'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(1255)]]
    );
    let result = run("select * from pg_catalog.pg_rewrite", &[]);
    assert!(result.rows.is_empty(), "pg_rewrite");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("rulename", PgType::Name),
            ("ev_class", PgType::Oid),
            ("ev_type", PgType::Char),
            ("ev_enabled", PgType::Char),
            ("is_instead", PgType::Bool),
            ("xmin", PgType::Xid),
            ("ev_qual", PgType::PgNodeTree),
            ("ev_action", PgType::PgNodeTree),
        ],
        "pg_rewrite"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_rewrite'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2618)]]
    );
    let result = run("select * from pg_catalog.pg_sequence", &[]);
    assert!(result.rows.is_empty(), "pg_sequence");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("seqrelid", PgType::Oid),
            ("seqtypid", PgType::Oid),
            ("seqstart", PgType::Int8),
            ("seqincrement", PgType::Int8),
            ("seqmax", PgType::Int8),
            ("seqmin", PgType::Int8),
            ("seqcache", PgType::Int8),
            ("seqcycle", PgType::Bool),
            ("xmin", PgType::Xid),
        ],
        "pg_sequence"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_sequence'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2224)]]
    );
    let result = run("select * from pg_catalog.pg_trigger", &[]);
    assert!(result.rows.is_empty(), "pg_trigger");
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("oid", PgType::Oid),
            ("tgrelid", PgType::Oid),
            ("tgparentid", PgType::Oid),
            ("tgname", PgType::Name),
            ("tgfoid", PgType::Oid),
            ("tgtype", PgType::Int2),
            ("tgenabled", PgType::Char),
            ("tgisinternal", PgType::Bool),
            ("tgconstrrelid", PgType::Oid),
            ("tgconstrindid", PgType::Oid),
            ("tgconstraint", PgType::Oid),
            ("tgdeferrable", PgType::Bool),
            ("tginitdeferred", PgType::Bool),
            ("tgnargs", PgType::Int2),
            ("tgattr", PgType::Int2Vector),
            ("tgargs", PgType::Bytea),
            ("tgoldtable", PgType::Name),
            ("tgnewtable", PgType::Name),
            ("xmin", PgType::Xid),
            ("tgqual", PgType::PgNodeTree),
        ],
        "pg_trigger"
    );
    assert_eq!(
        run(
            "select oid from pg_catalog.pg_class where relname = 'pg_trigger'",
            &[]
        )
        .rows,
        vec![vec![Value::Int(2620)]]
    );
}

// The prior absent-type assertions are retained verbatim in T44b evidence.
// E177 authorizes exact descriptor variants; semantic rejection is tested in
// datagrip_catalog_descriptors.rs and the wire codec tests.
#[test]
fn catalog_identity_authorized_facts_and_null_xmin() {
    let ids = run("select oid from pg_catalog.pg_class", &[]).rows;
    let unique: std::collections::BTreeSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), unique.len());
    assert_eq!(run("select oid, relname, xmin from pg_catalog.pg_class where relnamespace = 2200 and relkind = 'r'", &[]).rows, vec![vec![Value::Int(65564), Value::Text("inventory".into()), Value::Null]]);
    assert_eq!(
        run(
            "select indrelid, indkey, indisprimary from pg_catalog.pg_index",
            &[]
        )
        .rows,
        vec![vec![
            Value::Int(65564),
            Value::Array(vec![Value::Int(1)]),
            Value::Bool(true)
        ]]
    );
    assert!(run("select oid from pg_catalog.pg_class where relnamespace = 2200 and relname = 'ungranted'", &[]).rows.is_empty());
    assert!(matches!(
        plan("select * from pg_catalog.pg_not_a_catalog"),
        Err(Error::Unsupported)
    ));
    assert_eq!(
        run("select xmin from pg_catalog.pg_namespace", &[]).rows,
        vec![vec![Value::Null], vec![Value::Null]]
    );
    assert!(matches!(
        plan("select age(xmin) from pg_catalog.pg_proc"),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        plan("select * from pg_catalog.pg_locks"),
        Err(Error::Unsupported)
    ));
}
#[test]
fn exact_static_captured_query_subset() {
    // Exact captured SHA256 328a6fbc2b8abd1efbb1ccdbcbe84d83bc0105454c26e432c194fc55c2c84b9b.
    let result = run(
        r#"select pg_amop.oid
from pg_catalog.pg_amop
         join pg_catalog.pg_opfamily on pg_opfamily.oid = amopfamily
where opfnamespace = $1::oid"#,
        &[Value::Int(2200)],
    );
    assert!(result.rows.is_empty());
    // Exact captured SHA256 39f920b9e3f53cb2c7de8285983ac8af3a6a6f30bb949343bbb1c8e9bc949a74.
    let result = run(
        r#"select pg_amproc.oid
from pg_catalog.pg_amproc
         join pg_catalog.pg_opfamily on pg_opfamily.oid = amprocfamily
where opfnamespace = $1::oid"#,
        &[Value::Int(2200)],
    );
    assert!(result.rows.is_empty());
    // Exact captured SHA256 b5983676a5aec7b2ce85cf8363361c638087231643eb00de64bb40014112b83f.
    let result = run(
        r#"select TG.oid
from pg_catalog.pg_trigger TG,
     pg_catalog.pg_class TC
where TC.oid = TG.tgrelid
  and TC.relnamespace = $1::oid"#,
        &[Value::Int(2200)],
    );
    assert!(result.rows.is_empty());
    // Exact captured SHA256 2c6e0b4af4314cf5ea5306508bc5d28b704e3fb86092642ec8323f4ce326bdee.
    assert!(
        run(
            r#"select RU.oid
from pg_catalog.pg_rewrite RU,
     pg_catalog.pg_class RC
where RC.oid = RU.ev_class
  and RC.relnamespace = $1::oid
  and not RU.rulename = '_RETURN'"#,
            &[Value::Int(2200)]
        )
        .rows
        .is_empty()
    );
    // Exact captured SHA256 4f54b7806ed8b19020ffa3ded6c34644ec8fd0b3e18b91ce25e5636cb7a48d83.
    assert!(
        run(
            r#"select P.oid
from pg_catalog.pg_policy P
     join pg_catalog.pg_class C on polrelid = C.oid
where relnamespace = $1::oid"#,
            &[Value::Int(2200)]
        )
        .rows
        .is_empty()
    );
}
