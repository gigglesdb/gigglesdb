use giggles_core::{
    Admin, Limits,
    relational::{DataType, with_sql_parser_forbidden},
};
use giggles_language::*;
use std::{
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

const V7: LanguageVersion = LanguageVersion::Embedded07;
const EARLIER: [LanguageVersion; 6] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
    LanguageVersion::Embedded05,
    LanguageVersion::Embedded06,
];
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-catalog-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn database() -> (PathBuf, Admin, Engine) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    (path, admin.clone(), Engine::with_version(admin, V7))
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(path()).unwrap()
}
fn column(name: &str, data_type: CatalogType, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        data_type,
        nullable,
    }
}
fn create(table: &str) -> CatalogStatement {
    CatalogStatement::CreateTable {
        table: table.into(),
        columns: vec![column("id", CatalogType::Int64, false)],
        primary_key: vec!["id".into()],
    }
}
fn invoke(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(args)
        .output()
        .unwrap()
}
fn json(out: &Output) -> serde_json::Value {
    assert!(out.status.success(), "{out:?}");
    assert!(out.stderr.is_empty());
    serde_json::from_slice(&out.stdout).unwrap()
}
fn error(out: &Output) -> Error {
    assert!(!out.status.success(), "{out:?}");
    assert!(out.stdout.is_empty());
    let v: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    serde_json::from_value(v["error"].clone()).unwrap()
}

#[test]
fn catalog_version_gates_and_required_defaults_are_explicit() {
    let (database, admin, engine) = database();
    let source = "CREATE TABLE Mixed (ID int64,Flag bool,N int32,Tag nullable text,Raw nullable bytes,PRIMARY KEY(ID))";
    let tree = CatalogStatement::CreateTable {
        table: "mixed".into(),
        columns: vec![
            column("id", CatalogType::Int64, false),
            column("flag", CatalogType::Bool, false),
            column("n", CatalogType::Int32, false),
            column("tag", CatalogType::Text, true),
            column("raw", CatalogType::Bytes, true),
        ],
        primary_key: vec!["id".into()],
    };
    assert_eq!(parse_catalog_with_version(source, V7).unwrap(), tree);
    assert_eq!(
        parse_input_with_version(source, V7).unwrap(),
        ParsedInput::Catalog(tree.clone())
    );
    assert!(parse_with_version(source, V7).is_err());
    let decoded: CatalogStatement =
        serde_json::from_value(serde_json::to_value(&tree).unwrap()).unwrap();
    assert_eq!(decoded, tree);
    let before = digest(&admin);
    let prepared = engine.prepare_catalog_bound(&tree).unwrap();
    assert_eq!(prepared.language_version(), "0.7");
    assert_eq!(prepared.profile(), "embedded-trusted-native-0.7");
    assert_eq!(digest(&admin), before);
    for version in EARLIER {
        let older = Engine::with_version(admin.clone(), version);
        assert_eq!(
            parse_catalog_with_version(source, version),
            Err(Error::Unsupported)
        );
        assert_eq!(older.execute_catalog(&decoded), Err(Error::Unsupported));
        assert_eq!(
            older.prepare_catalog_bound(&decoded).err(),
            Some(Error::Unsupported)
        );
        assert_eq!(older.execute_catalog_text(source), Err(Error::Unsupported));
        assert_eq!(
            older.prepare_catalog_bound_text(source).err(),
            Some(Error::Unsupported)
        );
        assert_eq!(
            older.execute_catalog_bound(&prepared),
            Err(Error::Unsupported)
        );
        assert!(parse_input_with_version(source, version).is_err());
    }
    assert_eq!(digest(&admin), before);
    let older = Engine::with_version(admin.clone(), LanguageVersion::Embedded06);
    let old_block = older
        .prepare_transaction_bound_text("transaction read only { require true }", &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_transaction_bound(&old_block),
        Err(Error::Unsupported)
    );
    let created = with_sql_parser_forbidden(|| engine.execute_catalog_bound(&prepared)).unwrap();
    assert_eq!(
        created
            .table
            .columns
            .iter()
            .map(|c| c.not_null)
            .collect::<Vec<_>>(),
        [true, true, true, false, false]
    );
    assert_eq!(
        created
            .table
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        [
            DataType::BigInt,
            DataType::Boolean,
            DataType::Integer,
            DataType::Text,
            DataType::Bytes
        ]
    );
    for source in [
        "create table small(id smallint,primary key(id))",
        "create table small(id int16,primary key(id))",
        "create table sql_type(id bigint,primary key(id))",
        "create table floats(id float64,primary key(id))",
    ] {
        assert!(
            engine.prepare_catalog_bound_text(source).is_err(),
            "{source}"
        );
    }
    let mut noncanonical = create("candidate");
    let CatalogStatement::CreateTable { columns, .. } = &mut noncanonical else { unreachable!() };
    columns[0].name = "ID".into();
    assert!(engine.prepare_catalog_bound(&noncanonical).is_err());
    assert!(engine.execute_catalog(&noncanonical).is_err());
    let names = (0..64).map(|i| format!("c{i}")).collect::<Vec<_>>();
    let wide_source = format!(
        "create table wide({},primary key({}))",
        names
            .iter()
            .map(|name| format!("{name} int64"))
            .collect::<Vec<_>>()
            .join(","),
        names.join(",")
    );
    let wide_tree = parse_catalog_with_version(&wide_source, V7).unwrap();
    let wide = engine.execute_catalog(&wide_tree).unwrap();
    assert_eq!(wide.table.columns.len(), 64);
    assert_eq!(wide.table.primary_key, (0..64).collect::<Vec<_>>());
    let mut over = wide_tree.clone();
    let CatalogStatement::CreateTable { table, columns, .. } = &mut over else { unreachable!() };
    *table = "too_wide".into();
    columns.push(column("extra", CatalogType::Int64, false));
    assert_eq!(
        engine.prepare_catalog_bound(&over).err(),
        Some(Error::Limit)
    );
    let mut over = wide_tree;
    let CatalogStatement::CreateTable {
        table, primary_key, ..
    } = &mut over else { unreachable!() };
    *table = "too_many_keys".into();
    primary_key.push("extra".into());
    assert_eq!(
        engine.prepare_catalog_bound(&over).err(),
        Some(Error::Limit)
    );
    for name in ["nullable", "primary", "key", "bool"] {
        admin
            .sql_session()
            .execute(
                &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY,value BIGINT)"),
                &[],
                None,
            )
            .unwrap();
        admin
            .sql_session()
            .execute(&format!("INSERT INTO {name} VALUES(1,7)"), &[], None)
            .unwrap();
        let source = format!("{name} where id=1 select value");
        for version in EARLIER.into_iter().chain([V7]) {
            assert_eq!(
                parse_with_version(&source, version).unwrap(),
                parse(&source).unwrap()
            );
            assert_eq!(
                Engine::with_version(admin.clone(), version)
                    .execute_text(&source, &Bindings::new())
                    .unwrap()
                    .rows[0]
                    .values,
                [Value::Int(7)]
            );
        }
    }
    assert_eq!(serde_json::to_value(V7).unwrap(), "0.7");
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    assert_eq!(profile_for(V7).name, "embedded-trusted-native-0.7");
    let before = digest(&admin);
    drop(engine);
    drop(older);
    drop(admin);
    for version in EARLIER {
        error(&invoke(&[
            "run",
            "--language-version",
            version.as_str(),
            database.to_str().unwrap(),
            "create table forbidden(id int64,primary key(id))",
        ]));
    }
    error(&invoke(&[
        "run",
        database.to_str().unwrap(),
        "create table forbidden(id int64,primary key(id))",
    ]));
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
}

#[test]
fn catalog_text_tree_and_bound_execution_match_without_sql() {
    let source = "create table items (id int64,tenant text,n int32,note nullable text,raw nullable bytes,primary key(tenant,id))";
    let tree = CatalogStatement::CreateTable {
        table: "items".into(),
        columns: vec![
            column("id", CatalogType::Int64, false),
            column("tenant", CatalogType::Text, false),
            column("n", CatalogType::Int32, false),
            column("note", CatalogType::Text, true),
            column("raw", CatalogType::Bytes, true),
        ],
        primary_key: vec!["tenant".into(), "id".into()],
    };
    assert_eq!(parse_catalog_with_version(source, V7).unwrap(), tree);
    let mut baseline = None;
    for mode in 0..4 {
        let (path, admin, engine) = database();
        let result = with_sql_parser_forbidden(|| match mode {
            0 => engine.execute_catalog_text(source),
            1 => engine.execute_catalog(&tree),
            _ => {
                let mut supplied = tree.clone();
                let before = digest(&admin);
                let bound = if mode == 2 {
                    engine.prepare_catalog_bound_text(source)
                } else {
                    engine.prepare_catalog_bound(&supplied)
                }
                .unwrap();
                assert_eq!(digest(&admin), before);
                let CatalogStatement::CreateTable { table, columns, .. } = &mut supplied else { unreachable!() };
                *table = "different".into();
                columns[0].data_type = CatalogType::Text;
                engine.execute_catalog_bound(&bound)
            }
        })
        .unwrap();
        assert_eq!(result.catalog_epoch, 1);
        assert_eq!(result.table.id, 1);
        assert_eq!(result.table.primary_key, [1, 0]);
        assert_eq!(
            result
                .table
                .columns
                .iter()
                .map(|c| c.id)
                .collect::<Vec<_>>(),
            [2, 3, 4, 5, 6]
        );
        assert_eq!(
            admin.catalog().unwrap(),
            (result.catalog_epoch, vec![result.table.clone()])
        );
        if let Some(baseline) = &baseline {
            assert_eq!(&result, baseline);
        } else {
            baseline = Some(result.clone());
        }
        let mut sql = admin.sql_session();
        sql.execute(
            "CREATE TABLE legacy (id BIGINT PRIMARY KEY,s SMALLINT,optional TEXT)",
            &[],
            None,
        )
        .unwrap();
        let legacy = engine
            .prepare_bound_text("legacy", &Bindings::new())
            .unwrap();
        assert_eq!(
            legacy.description().columns[1].data_type,
            DataType::SmallInt
        );
        assert!(!legacy.description().columns[1].not_null);
        sql.execute(
            "INSERT INTO items(id,tenant,n) VALUES(1,'b',2147483647)",
            &[],
            None,
        )
        .unwrap();
        let bindings = Bindings::from([
            ("raw".into(), Value::Bytes(vec![0, 255])),
            ("note".into(), Value::Text("x\0y".into())),
        ]);
        with_sql_parser_forbidden(|| {
            engine.execute_text(
                "insert into items(id,tenant,n,note,raw) values(2,\"a\",-2147483648,$note,$raw)",
                &bindings,
            )
        })
        .unwrap();
        let native =
            with_sql_parser_forbidden(|| engine.execute_text("items", &Bindings::new())).unwrap();
        assert_eq!(
            native.rows,
            sql.execute("SELECT * FROM items LIMIT 1024", &[], None)
                .unwrap()
                .rows
        );
        assert_eq!(
            native.rows[0].values,
            [
                Value::Int(2),
                Value::Text("a".into()),
                Value::Int(i32::MIN as i64),
                Value::Text("x\0y".into()),
                Value::Bytes(vec![0, 255])
            ]
        );
        assert_eq!(native.rows[1].values[3..], [Value::Null, Value::Null]);
        for text in [
            "insert into items(id,tenant,n) values(3,\"a\",2147483648)",
            "insert into items(id,tenant) values(3,\"a\")",
            "insert into items(id,tenant,n) values(3,null,1)",
        ] {
            let before = digest(&admin);
            assert!(engine.execute_text(text, &Bindings::new()).is_err());
            assert_eq!(digest(&admin), before);
        }
        let metadata = admin.catalog().unwrap();
        drop(sql);
        drop(engine);
        drop(admin);
        let reopened = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(reopened.catalog().unwrap(), metadata);
        let exported = path_for_export();
        reopened.export(&exported).unwrap();
        let restored = Admin::restore(exported, path_for_export(), Limits::default()).unwrap();
        assert_eq!(restored.catalog().unwrap(), metadata);
    }
}
fn path_for_export() -> PathBuf {
    path()
}

#[test]
fn catalog_rejections_and_populated_policy_are_atomic() {
    let (_, admin, engine) = database();
    let before = digest(&admin);
    for source in [
        "create table invalid(id nullable int64,primary key(id))",
        "create table invalid(id int64)",
        "create table invalid(id int64,id int32,primary key(id))",
        "create table invalid(id int64,primary key(absent))",
        "create table invalid(id int64,primary key(id,id))",
        "create table invalid(id int64 default 1,primary key(id))",
        "create table invalid(id int64 primary key)",
        "create table invalid(id int64,primary key(id),constraint positive check(id>0))",
        "create table invalid(id int64,primary key(id)) partition by hash(id)",
        "create table public.invalid(id int64,primary key(id))",
        "create table if not exists invalid(id int64,primary key(id))",
        "create table invalid(id int64,primary key(id)) return *",
        "create table deliveries(id int64,primary key(id))",
        "create table invalid(id int64,deliveries text,primary key(id))",
        "create table invalid(id decimal(5,2),primary key(id))",
        "create schema billing",
        "drop table invalid",
        "alter table invalid add column n int64",
        "create index ix on invalid(id)",
    ] {
        with_sql_parser_forbidden(|| {
            assert!(
                engine.prepare_catalog_bound_text(source).is_err(),
                "{source}"
            );
            assert!(engine.execute_catalog_text(source).is_err(), "{source}");
            if let Ok(tree) = parse_catalog_with_version(source, V7) {
                assert!(engine.prepare_catalog_bound(&tree).is_err());
                assert!(engine.execute_catalog(&tree).is_err());
            }
        });
        assert_eq!(digest(&admin), before, "{source}");
    }
    engine
        .execute_catalog_text("create table items(id int64,state text,primary key(id))")
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        engine.execute_catalog(&create("items")),
        Err(Error::Sql { sqlstate: "42P07".into() })
    );
    assert_eq!(digest(&admin), before);
    for source in [
        "transaction { create table other(id int64,primary key(id)) }",
        "transaction read only { create table other(id int64,primary key(id)) }",
        "transaction { let schema=(create table other(id int64,primary key(id))); emit @schema.status }",
        "transaction { insert items set id=1,state=\"ready\"; create table other(id int64,primary key(id)) }",
    ] {
        assert!(
            engine
                .prepare_transaction_bound_text(source, &Bindings::new())
                .is_err()
        );
        assert!(
            engine
                .execute_transaction_text(source, &Bindings::new())
                .is_err()
        );
        assert_eq!(digest(&admin), before);
    }
    let bound = engine
        .prepare_catalog_bound_text("create table later(id int64,primary key(id))")
        .unwrap();
    engine
        .execute_text("insert items set id=1,state=\"ready\"", &Bindings::new())
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        engine.execute_catalog_bound(&bound),
        Err(Error::PlanRequired)
    );
    assert_eq!(
        engine.execute_catalog_text("create table later(id int64,primary key(id))"),
        Err(Error::PlanRequired)
    );
    assert_eq!(digest(&admin), before);
    let while_populated = engine
        .prepare_catalog_bound_text("create table later(id int64,primary key(id))")
        .unwrap();
    assert_eq!(digest(&admin), before);
    engine
        .execute_text("delete items where id=1", &Bindings::new())
        .unwrap();
    let created =
        with_sql_parser_forbidden(|| engine.execute_catalog_bound(&while_populated)).unwrap();
    assert_eq!(created.table.name, "later");
    assert_eq!(created.catalog_epoch, 2);
    assert_eq!(
        engine.execute_catalog_bound(&bound),
        Err(Error::Sql { sqlstate: "0A000".into() })
    );
    let read = engine
        .prepare_bound_text("items", &Bindings::new())
        .unwrap();
    let catalog = engine.prepare_catalog_bound(&create("future")).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE sql_new (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_bound(&read),
        Err(Error::Sql { sqlstate: "0A000".into() })
    );
    assert_eq!(
        engine.execute_catalog_bound(&catalog),
        Err(Error::Sql { sqlstate: "0A000".into() })
    );
}

#[test]
fn catalog_cli_empty_init_and_selected_manifest_match_version_seven() {
    let database = path();
    json(&invoke(&[
        "init",
        "--engine",
        "legacy",
        database.to_str().unwrap(),
    ]));
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(admin.catalog().unwrap(), (0, vec![]));
    drop(admin);
    let created: CatalogOutcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.7",
        database.to_str().unwrap(),
        "create table items(id int64,state text,n nullable int32,primary key(id))",
    ])))
    .unwrap();
    assert_eq!(created.catalog_epoch, 1);
    assert_eq!(created.table.name, "items");
    assert_eq!(created.table.columns[2].data_type, DataType::Integer);
    assert!(!created.table.columns[2].not_null);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    let before = digest(&admin);
    drop(admin);
    error(&invoke(&[
        "run",
        "--language-version",
        "0.7",
        database.to_str().unwrap(),
        "create table forbidden(id int64,primary key(id))",
        r#"{"unused":{"Int":1}}"#,
    ]));
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
    let engine = Engine::with_version(admin.clone(), V7);
    let manifest = capability_manifest_for(V7);
    let metadata = serde_json::to_value(&manifest).unwrap();
    assert_eq!(metadata["language_version"], "0.7");
    assert_eq!(metadata["limits"]["catalog_primary_key_columns"], 64);
    for version in EARLIER {
        assert!(
            serde_json::to_value(capability_manifest_for(version)).unwrap()["limits"]
                .get("catalog_primary_key_columns")
                .is_none()
        );
    }
    let mut count = 0;
    let mut catalog_examples = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            count += 1;
            with_sql_parser_forbidden(|| match parse_input_with_version(source, V7) {
                Ok(ParsedInput::Statement(tree)) => {
                    assert!(
                        engine.execute_text(source, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine.execute(&tree, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    if capability.feature == "duplicate_insert_many_targets" {
                        let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
                        assert_eq!(engine.execute_bound(&bound), Err(Error::DuplicateTarget));
                    } else {
                        assert!(
                            engine.prepare_bound_text(source, &Bindings::new()).is_err(),
                            "{}: {source}",
                            capability.feature
                        );
                    }
                }
                Ok(ParsedInput::Transaction(tree)) => {
                    assert!(
                        engine
                            .execute_transaction_text(source, &Bindings::new())
                            .is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine
                            .prepare_transaction_bound_text(source, &Bindings::new())
                            .is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(engine.execute_transaction(&tree, &Bindings::new()).is_err());
                }
                Ok(ParsedInput::Catalog(tree)) => {
                    catalog_examples += 1;
                    assert!(
                        engine.execute_catalog_text(source).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine.prepare_catalog_bound_text(source).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(engine.execute_catalog(&tree).is_err());
                    assert!(engine.prepare_catalog_bound(&tree).is_err());
                }
                Ok(
                    ParsedInput::Once(_)
                    | ParsedInput::Outcome(_)
                    | ParsedInput::Inspection(_)
                    | ParsedInput::Explain(_),
                ) => {
                    panic!("earlier profile admitted a retry envelope: {source}")
                }
                Err(_) => {}
            });
        }
    }
    assert!(count > 0 && catalog_examples > 0);
    assert_eq!(digest(&admin), before);
    drop(engine);
    drop(admin);
    let mut count = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::Cli)
    {
        for source in &capability.rejection_examples {
            count += 1;
            error(&invoke(&[
                "run",
                "--language-version",
                "0.7",
                database.to_str().unwrap(),
                source,
            ]));
        }
    }
    assert!(count > 0);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
    drop(admin);
    assert_eq!(
        json(&invoke(&["capabilities", "--language-version", "0.7"]))["name"],
        "embedded-trusted-native-0.7"
    );
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.7"
        ])),
        metadata
    );
    json(&invoke(&[
        "run",
        "--language-version",
        "0.7",
        database.to_str().unwrap(),
        "create table another(id int64,primary key(id))",
        "{}",
    ]));
    json(&invoke(&[
        "run",
        database.to_str().unwrap(),
        "insert items set id=1,state=\"ready\"",
    ]));
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.7",
            database.to_str().unwrap(),
            "create table populated(id int64,primary key(id))"
        ])),
        Error::PlanRequired
    );
    let legacy = path();
    json(&invoke(&[
        "init",
        "--engine",
        "legacy",
        legacy.to_str().unwrap(),
        "CREATE TABLE legacy(id BIGINT PRIMARY KEY,s SMALLINT)",
    ]));
    let admin = Admin::open(legacy, Limits::default()).unwrap();
    assert_eq!(
        admin.catalog().unwrap().1[0].columns[1].data_type,
        DataType::SmallInt
    );
    let absent = database.with_extension("absent");
    assert!(!absent.exists());
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "9.9",
            absent.to_str().unwrap(),
            "create table invalid(id int64,primary key(id))"
        ])),
        Error::Unsupported
    );
    assert!(!absent.exists());
}
