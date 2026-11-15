mod common;

use giggles_core::{
    Admin, Cancellation, CoreError, CrashPoint, Limits,
    operations::{
        Bindings, CatalogRequest, ColumnSpec, ENGINE_CONTRACT_VERSION, Engine, Error,
        InspectionOutcome, InspectionRequest, InspectionResult, MAX_INSPECTION_METADATA_BYTES,
        Request, TableSummary,
    },
    relational::{DataType, MAX_COLUMNS, MAX_ROWS, MAX_TABLES, MAX_TRANSACTIONS, Value},
};
use std::path::PathBuf;

fn database() -> (PathBuf, Admin, Engine) {
    let path = common::fresh("inspection");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    (path, admin.clone(), Engine::new(admin))
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("inspection-export")).unwrap()
}
fn describe(table: &str) -> InspectionRequest {
    InspectionRequest::Table {
        table: table.into(),
    }
}
fn summaries(outcome: InspectionOutcome) -> Vec<TableSummary> {
    assert!(outcome.complete);
    let InspectionResult::Tables(tables) = outcome.result else {
        panic!("table summaries expected")
    };
    tables
}
fn trap<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")]
    {
        giggles_core::relational::with_sql_parser_forbidden(work)
    }
    #[cfg(not(feature = "test-sql-parser-trap"))]
    {
        work()
    }
}

#[test]
fn inspection_reports_shared_metadata_without_sql_or_generation_changes() {
    let (_, admin, engine) = database();
    let empty_before = digest(&admin);
    let empty = trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap();
    assert_eq!(empty.contract_version, ENGINE_CONTRACT_VERSION);
    assert_eq!(empty.catalog_epoch, 0);
    assert_eq!(empty.database_domain, admin.snapshot().unwrap().domain);
    assert!(summaries(empty).is_empty());
    assert_eq!(digest(&admin), empty_before);

    admin.sql_session().execute(
        "CREATE TABLE zeta (flag BOOLEAN NOT NULL, small SMALLINT, n INTEGER, id BIGINT NOT NULL, title TEXT, raw BYTEA NOT NULL, PRIMARY KEY(raw,id))",
        &[], None,
    ).unwrap();
    let created = engine
        .execute_catalog(&CatalogRequest::CreateTable {
            table: "alpha".into(),
            columns: vec![ColumnSpec {
                name: "id".into(),
                data_type: DataType::Integer,
                not_null: true,
                default: None, generated: None,
            }],
            primary_key: vec!["id".into()],
        })
        .unwrap();
    let (epoch, tables) = admin.catalog().unwrap();
    let before = digest(&admin);
    let output = trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap();
    assert_eq!(output.catalog_epoch, epoch);
    assert_eq!(output.database_domain, admin.snapshot().unwrap().domain);
    assert_eq!(
        summaries(output),
        vec![
            TableSummary {
                id: created.table.id,
                name: "alpha".into(),
                column_count: 1,
                primary_key_columns: 1
            },
            TableSummary {
                id: tables[1].id,
                name: "zeta".into(),
                column_count: 6,
                primary_key_columns: 2
            },
        ]
    );
    for table in &tables {
        let described = trap(|| engine.inspect(&describe(&table.name))).unwrap();
        assert_eq!(described.catalog_epoch, epoch);
        assert!(described.complete);
        assert_eq!(described.result, InspectionResult::Table(table.clone()));
        let wire = serde_json::to_value(&described).unwrap();
        assert_eq!(
            wire["database_domain"],
            described.database_domain.to_string()
        );
        assert_eq!(
            serde_json::from_value::<InspectionOutcome>(wire).unwrap(),
            described
        );
    }
    let zeta = &tables[1];
    assert_eq!(zeta.primary_key, [5, 3]);
    assert_eq!(
        zeta.columns
            .iter()
            .map(|column| column.data_type.clone())
            .collect::<Vec<_>>(),
        [
            DataType::Boolean,
            DataType::SmallInt,
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Bytes,
        ]
    );
    assert_eq!(
        zeta.columns
            .iter()
            .map(|column| column.not_null)
            .collect::<Vec<_>>(),
        [true, false, false, true, false, true]
    );
    assert_eq!(digest(&admin), before);
}

#[test]
fn inspection_reads_only_committed_metadata_and_ignores_business_row_bounds() {
    let (_, admin, engine) = database();
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE items (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    sql.execute("BEGIN", &[], None).unwrap();
    sql.execute("CREATE TABLE staged (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        trap(|| engine.inspect(&describe("staged"))),
        Err(Error::Bind("table".into()))
    );
    assert_eq!(
        summaries(trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap()).len(),
        1
    );
    assert_eq!(digest(&admin), before);
    sql.rollback();
    sql.execute("BEGIN", &[], None).unwrap();
    let insert = sql.prepare("INSERT INTO items VALUES($1)").unwrap();
    for value in 0..=MAX_ROWS {
        sql.execute_prepared(&insert, &[Value::Int(value as i64)], None)
            .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    let before = digest(&admin);
    let output = trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap();
    let names = summaries(output);
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].column_count, 1);
    assert_eq!(names[0].primary_key_columns, 1);
    let encoded = serde_json::to_string(&names).unwrap();
    assert!(!encoded.contains("row_count"));
    trap(|| engine.inspect(&describe("items"))).unwrap();
    // A real row operation cannot certify this oversized relation. Inspection
    // neither inherits that row ceiling nor counts/materializes its rows.
    assert_eq!(
        engine.execute(
            &Request::Read {
                table: "items".into(),
                filter: None,
                order: vec![],
                limit: None,
                projection: vec![],
            },
            &Bindings::new()
        ),
        Err(Error::Limit)
    );
    assert_eq!(digest(&admin), before);

    sql.execute("CREATE TABLE published (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let after = trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap();
    assert_eq!(after.catalog_epoch, 2);
    assert_eq!(
        summaries(after)
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>(),
        ["items", "published"]
    );
}

#[test]
fn inspection_metadata_bounds_and_name_admission_are_explicit() {
    let (_, admin, engine) = database();
    let wide_name = "w".repeat(63);
    let columns: Vec<_> = (0..MAX_COLUMNS)
        .map(|index| format!("c{index:02}{}", "x".repeat(60)))
        .collect();
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for index in 0..MAX_TABLES - 1 {
        sql.execute(
            &format!("CREATE TABLE t{index:03} (id BIGINT PRIMARY KEY)"),
            &[],
            None,
        )
        .unwrap();
    }
    let definitions = columns
        .iter()
        .map(|name| format!("{name} BIGINT NOT NULL"))
        .collect::<Vec<_>>()
        .join(",");
    sql.execute(
        &format!(
            "CREATE TABLE {wide_name} ({definitions}, PRIMARY KEY({}))",
            columns.join(",")
        ),
        &[],
        None,
    )
    .unwrap();
    sql.execute("COMMIT", &[], None).unwrap();
    let before = digest(&admin);
    let table_list = trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap();
    assert_eq!(summaries(table_list).len(), MAX_TABLES);
    let wide = trap(|| engine.inspect(&describe(&wide_name))).unwrap();
    let InspectionResult::Table(table) = wide.result else {
        panic!("descriptor expected")
    };
    assert_eq!(table.columns.len(), MAX_COLUMNS);
    assert_eq!(table.primary_key.len(), MAX_COLUMNS);
    assert_eq!(table.primary_key, (0..MAX_COLUMNS).collect::<Vec<_>>());
    for invalid in [
        "",
        "Upper",
        "_leading",
        "space name",
        "schema.table",
        "é",
        "0first",
        "a\0b",
    ] {
        assert_eq!(
            engine.inspect(&describe(invalid)),
            Err(Error::Bind("identifier".into())),
            "{invalid}"
        );
    }
    for length in [64, MAX_INSPECTION_METADATA_BYTES + 1] {
        assert_eq!(
            engine.inspect(&describe(&"a".repeat(length))),
            Err(Error::Limit)
        );
    }
    assert_eq!(
        engine.inspect(&describe("missing")),
        Err(Error::Bind("table".into()))
    );
    // A valid but absent reserved name has the same missing-table diagnostic;
    // this surface grants no access to built-in repair or policy state.
    assert_eq!(
        engine.inspect(&describe("deliveries")),
        Err(Error::Bind("table".into()))
    );
    assert_eq!(digest(&admin), before);
}

#[test]
fn inspection_errors_and_cancellation_preserve_durable_state() {
    let (_, admin, engine) = database();
    admin
        .sql_session()
        .execute("CREATE TABLE items (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let before = digest(&admin);
    let cancelled = Cancellation::default();
    assert!(cancelled.cancel());
    for request in [
        InspectionRequest::Tables,
        describe("items"),
        describe("missing"),
    ] {
        assert_eq!(
            cancelled.scope(|| engine.inspect(&request)),
            Err(Error::Core(CoreError::Cancelled.to_string()))
        );
    }
    // Structural refusal precedes storage admission, including cancellation.
    assert_eq!(
        cancelled.scope(|| engine.inspect(&describe(&"a".repeat(64)))),
        Err(Error::Limit)
    );
    let scope = Cancellation::default();
    scope.scope(|| {
        trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap();
        assert!(
            scope.cancel(),
            "inspection must never mark a durable decision"
        );
    });
    admin
        .set_crash_point(CrashPoint::ErrorBeforeCommit)
        .unwrap();
    trap(|| engine.inspect(&describe("items"))).unwrap();
    admin.set_crash_point(CrashPoint::None).unwrap();
    assert_eq!(digest(&admin), before);
}

#[test]
fn inspection_uses_no_sql_transaction_slot() {
    let (_, admin, engine) = database();
    let mut sessions = Vec::new();
    for _ in 0..MAX_TRANSACTIONS {
        let mut sql = admin.sql_session();
        sql.execute("BEGIN", &[], None).unwrap();
        sessions.push(sql);
    }
    assert_eq!(
        admin.sql_session().execute("BEGIN", &[], None),
        Err(CoreError::Busy)
    );
    let before = digest(&admin);
    assert!(summaries(trap(|| engine.inspect(&InspectionRequest::Tables)).unwrap()).is_empty());
    assert_eq!(
        engine.inspect(&describe("missing")),
        Err(Error::Bind("table".into()))
    );
    assert_eq!(digest(&admin), before);
    drop(sessions);
}

#[test]
fn inspection_reopen_and_restore_report_current_domain() {
    let (path, admin, engine) = database();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, note TEXT)",
            &[],
            None,
        )
        .unwrap();
    let original = trap(|| engine.inspect(&describe("items"))).unwrap();
    let exported = common::fresh("inspection-domain-export");
    admin.export(&exported).unwrap();
    drop(engine);
    drop(admin);
    let reopened = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(
        Engine::new(reopened.clone())
            .inspect(&describe("items"))
            .unwrap(),
        original
    );
    let restored = Admin::restore(
        exported,
        common::fresh("inspection-domain-restored"),
        Limits::default(),
    )
    .unwrap();
    let before = digest(&restored);
    let current = trap(|| Engine::new(restored.clone()).inspect(&describe("items"))).unwrap();
    assert_ne!(current.database_domain, original.database_domain);
    assert_eq!(current.catalog_epoch, original.catalog_epoch);
    assert_eq!(current.result, original.result);
    assert_eq!(digest(&restored), before);
}
