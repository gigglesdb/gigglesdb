mod common;
use common::*;
use giggles_core::{
    relational::{DataType, HIDDEN_ROWID, MAX_COLUMNS, Numeric, Scan, SqlError as E, Value as V},
    *,
};

struct TestPaths(Vec<std::path::PathBuf>);

impl TestPaths {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn fresh(&mut self, name: &str) -> std::path::PathBuf {
        let path = fresh(name);
        self.0.push(path.clone());
        path
    }
}

impl Drop for TestPaths {
    fn drop(&mut self) {
        for path in &self.0 {
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

fn export_version(a: &Admin, path: &std::path::Path) -> u32 {
    a.export(path).unwrap();
    assert_eq!(Admin::verify_export(path).unwrap(), a.snapshot().unwrap());
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(&bytes[..8], b"GDEXP001");
    u32::from_be_bytes(bytes[8..12].try_into().unwrap())
}

fn assert_heap_view(a: &Admin, expected: &[Vec<V>]) {
    let mut s = a.sql_session();
    assert_eq!(run(&mut s, "SELECT * FROM h"), expected);
    let prepared = s.prepare("SELECT * FROM h").unwrap();
    let description = s.describe(&prepared).unwrap();
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(a.catalog().unwrap().1[0].hidden_rowid(), Some(2));
    assert_eq!(
        s.execute("SELECT giggles_rowid FROM h", &[], None),
        Err(CoreError::Sql(E::Reserved))
    );
}

fn db() -> (Admin, SqlSession) {
    let a = Admin::create(fresh("relational-heap"), Limits::default()).unwrap();
    let s = a.sql_session();
    (a, s)
}

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .expect(sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

#[test]
fn heap_catalog_insert_visibility_and_descriptions() {
    let (a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    let (_, tables) = a.catalog().unwrap();
    let table = &tables[0];
    assert_eq!(table.columns.len(), 3);
    assert_eq!(table.hidden_rowid(), Some(2));
    assert_eq!(table.primary_key, [2]);
    assert_eq!(table.columns[2].name, HIDDEN_ROWID);
    assert_eq!(table.columns[2].data_type, DataType::BigInt);
    assert!(table.columns[2].not_null);
    assert!(table.columns.iter().all(|column| column.id != 0));
    assert_eq!(
        table
            .logical_columns()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );

    run(&mut s, "INSERT INTO h VALUES (1, 'x')");
    run(&mut s, "INSERT INTO h (b, a) VALUES ('y', 2)");
    run(&mut s, "INSERT INTO h VALUES (1, 'x')");
    let expected = vec![
        vec![V::Int(1), V::Text("x".into())],
        vec![V::Int(2), V::Text("y".into())],
        vec![V::Int(1), V::Text("x".into())],
    ];
    assert_eq!(run(&mut s, "SELECT * FROM h"), expected);
    assert_eq!(run(&mut s, "SELECT a, b FROM h"), expected);
    let sorted = s.prepare("SELECT * FROM h ORDER BY a").unwrap();
    assert_eq!(
        s.describe(&sorted)
            .unwrap()
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(
        run(&mut s, "SELECT * FROM h ORDER BY a"),
        vec![
            expected[0].clone(),
            expected[2].clone(),
            expected[1].clone()
        ]
    );
    assert_eq!(
        s.execute("INSERT INTO h VALUES (1, 'x', 9)", &[], None),
        Err(CoreError::Sql(E::Syntax))
    );
    let select = s.prepare("SELECT * FROM h").unwrap();
    let description = s.describe(&select).unwrap();
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    let insert = s.prepare("INSERT INTO h VALUES ($1, $2)").unwrap();
    let description = s.describe(&insert).unwrap();
    assert_eq!(description.parameters, [DataType::BigInt, DataType::Text]);
    assert!(description.columns.is_empty());
    s.execute_prepared(&insert, &[V::Int(3), V::Text("p".into())], None)
        .unwrap();
    for sql in [
        "SELECT giggles_rowid FROM h",
        "INSERT INTO h (a, giggles_rowid) VALUES (1, 1)",
        "CREATE TABLE reserved (giggles_rowid BIGINT)",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::Reserved)),
            "{sql}"
        );
    }
}

#[test]
fn heap_expressions_filters_sort_limits_and_aggregates() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    // Arithmetic expressions are supported in INSERT values.
    assert_eq!(
        s.execute("INSERT INTO h VALUES (1 + 1, lower('Z'))", &[], None)
            .unwrap()
            .affected,
        1
    );
    run(&mut s, "INSERT INTO h VALUES (abs(-2)::bigint, lower('Z'))");
    run(
        &mut s,
        "INSERT INTO h (b, a) VALUES (lower('X'), abs(-1)::bigint)",
    );
    run(&mut s, "INSERT INTO h VALUES (3, 'y')");
    assert_eq!(
        run(
            &mut s,
            "SELECT * FROM h WHERE a >= 2 ORDER BY a DESC LIMIT 1"
        ),
        vec![vec![V::Int(3), V::Text("y".into())]]
    );
    assert_eq!(
        run(&mut s, "SELECT COUNT(*), SUM(a), MIN(a), MAX(a) FROM h"),
        vec![vec![
            V::Int(4),
            V::Numeric(Numeric::parse("8").unwrap()),
            V::Int(1),
            V::Int(3)
        ]]
    );
    assert_eq!(
        run(&mut s, "SELECT b FROM h WHERE b='z'"),
        vec![vec![V::Text("z".into())], vec![V::Text("z".into())]]
    );
}

#[test]
fn heap_star_hides_rowid_and_preserves_outer_join_padding() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    run(
        &mut s,
        "CREATE TABLE j (c BIGINT PRIMARY KEY, d TEXT NOT NULL)",
    );
    run(&mut s, "INSERT INTO h VALUES (1, 'x')");
    run(&mut s, "INSERT INTO j VALUES (2, 'y')");
    let sql = "SELECT * FROM h LEFT JOIN j ON h.a=j.c";
    let prepared = s.prepare(sql).unwrap();
    let description = s.describe(&prepared).unwrap();
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert!(!description.columns[2].not_null);
    assert!(!description.columns[3].not_null);
    assert_eq!(
        run(&mut s, sql),
        vec![vec![V::Int(1), V::Text("x".into()), V::Null, V::Null]]
    );
    run(&mut s, "INSERT INTO j VALUES (1, 'z')");
    assert_eq!(
        run(&mut s, sql),
        vec![vec![
            V::Int(1),
            V::Text("x".into()),
            V::Int(1),
            V::Text("z".into())
        ]]
    );
}

#[test]
fn heap_width_reserves_one_physical_column() {
    let (a, mut s) = db();
    let columns = (0..MAX_COLUMNS)
        .map(|index| format!("c{index} BIGINT"))
        .collect::<Vec<_>>();
    assert_eq!(
        s.execute(
            &format!("CREATE TABLE wide ({})", columns.join(",")),
            &[],
            None
        ),
        Err(CoreError::Sql(E::Limit))
    );
    run(
        &mut s,
        &format!(
            "CREATE TABLE fits ({})",
            columns[..MAX_COLUMNS - 1].join(",")
        ),
    );
    let (_, tables) = a.catalog().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].columns.len(), MAX_COLUMNS);
    assert_eq!(tables[0].logical_columns().count(), MAX_COLUMNS - 1);
    // GD-0042 boundary: index paths cap the PHYSICAL width (logical + hidden rowid)
    // at MAX_COLUMNS, so a full-width heap table must still index, enforce and look up.
    let last = MAX_COLUMNS - 2;
    run(&mut s, &format!("CREATE UNIQUE INDEX fits_last ON fits (c{last})"));
    let values = |v: i64| {
        (0..MAX_COLUMNS - 1)
            .map(|i| if i == last { v.to_string() } else { i.to_string() })
            .collect::<Vec<_>>()
            .join(",")
    };
    run(&mut s, &format!("INSERT INTO fits VALUES ({}), ({})", values(1), values(2)));
    assert_eq!(
        s.execute(&format!("INSERT INTO fits VALUES ({})", values(1)), &[], None),
        Err(CoreError::Sql(E::Duplicate))
    );
    assert_eq!(
        run(&mut s, &format!("SELECT c0 FROM fits WHERE c{last} = 2")),
        vec![vec![V::Int(0)]]
    );
    assert_eq!(run(&mut s, "SELECT COUNT(*) FROM fits"), vec![vec![V::Int(2)]]);
}

#[test]
fn heap_reopens_with_rows_and_hidden_logical_view() {
    let mut paths = TestPaths::new();
    let path = paths.fresh("relational-heap-reopen");
    {
        let a = Admin::create(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
        run(&mut s, "INSERT INTO h VALUES (1, 'x')");
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    assert_eq!(
        run(&mut s, "SELECT * FROM h"),
        vec![vec![V::Int(1), V::Text("x".into())]]
    );
    let prepared = s.prepare("SELECT * FROM h").unwrap();
    assert_eq!(s.describe(&prepared).unwrap().columns.len(), 2);
    assert_eq!(a.catalog().unwrap().1[0].hidden_rowid(), Some(2));
}

#[test]
fn heap_empty_and_populated_reopen_export_restore_use_heap_envelope() {
    let mut paths = TestPaths::new();
    let path = paths.fresh("heap-format");
    let empty_export = paths.fresh("heap-format-empty-export");
    let empty_restore = paths.fresh("heap-format-empty-restore");
    let populated_export = paths.fresh("heap-format-populated-export");
    let populated_restore = paths.fresh("heap-format-populated-restore");
    {
        let a = Admin::create(&path, Limits::default()).unwrap();
        run(&mut a.sql_session(), "CREATE TABLE h (a BIGINT, b TEXT)");
    }
    {
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert_heap_view(&a, &[]);
        assert_eq!(export_version(&a, &empty_export), 29);
        let restored = Admin::restore(&empty_export, &empty_restore, Limits::default()).unwrap();
        assert_heap_view(&restored, &[]);
        run(
            &mut a.sql_session(),
            "INSERT INTO h VALUES (1, 'x'), (1, 'x'), (2, NULL)",
        );
    }
    let expected = vec![
        vec![V::Int(1), V::Text("x".into())],
        vec![V::Int(1), V::Text("x".into())],
        vec![V::Int(2), V::Null],
    ];
    let a = Admin::open(&path, Limits::default()).unwrap();
    assert_heap_view(&a, &expected);
    assert_eq!(export_version(&a, &populated_export), 29);
    let restored = Admin::restore(&populated_export, &populated_restore, Limits::default()).unwrap();
    assert_heap_view(&restored, &expected);
    run(&mut restored.sql_session(), "INSERT INTO h VALUES (3, 'restored')");
    let mut after_insert = expected;
    after_insert.push(vec![V::Int(3), V::Text("restored".into())]);
    assert_heap_view(&restored, &after_insert);
}

#[test]
fn heap_date_descriptor_and_rows_roundtrip_under_temporal_heap_envelope() {
    let mut paths = TestPaths::new();
    let path = paths.fresh("heap-date-format");
    let empty_export = paths.fresh("heap-date-empty-export");
    let export = paths.fresh("heap-date-export");
    let destination = paths.fresh("heap-date-restore");
    let expected;
    {
        let a = Admin::create(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE TABLE h (a BIGINT, b DATE)");
        assert_eq!(export_version(&a, &empty_export), 41);
        run(&mut s, "INSERT INTO h VALUES (1, '2000-01-01'), (2, NULL)");
        expected = run(&mut s, "SELECT * FROM h");
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    assert_heap_view(&a, &expected);
    assert_eq!(a.catalog().unwrap().1[0].columns[1].data_type, DataType::Date);
    assert_eq!(export_version(&a, &export), 41);
    let restored = Admin::restore(&export, &destination, Limits::default()).unwrap();
    assert_heap_view(&restored, &expected);
    assert_eq!(
        restored.catalog().unwrap().1[0].columns[1].data_type,
        DataType::Date
    );
}

#[test]
fn heap_secondary_and_unique_indexes_survive_reopen_export_restore() {
    let mut paths = TestPaths::new();
    let path = paths.fresh("heap-index-format");
    let export = paths.fresh("heap-index-export");
    let destination = paths.fresh("heap-index-restore");
    {
        let a = Admin::create(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
        run(&mut s, "INSERT INTO h VALUES (1, 'x'), (2, 'y')");
        run(&mut s, "CREATE UNIQUE INDEX unique_a ON h (a)");
        run(&mut s, "CREATE INDEX by_b ON h (b)");
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    let version = export_version(&a, &export);
    assert!(
        (33..=36).contains(&version),
        "indexed heap version: {version}"
    );
    let restored = Admin::restore(&export, &destination, Limits::default()).unwrap();
    for database in [&a, &restored] {
        assert_heap_view(
            database,
            &[
                vec![V::Int(1), V::Text("x".into())],
                vec![V::Int(2), V::Text("y".into())],
            ],
        );
        let mut s = database.sql_session();
        assert_eq!(
            s.execute("INSERT INTO h VALUES (1, 'duplicate')", &[], None),
            Err(CoreError::Sql(E::Duplicate))
        );
        assert_eq!(
            run(&mut s, "SELECT b FROM h WHERE a=2"),
            vec![vec![V::Text("y".into())]]
        );
        assert_eq!(
            run(&mut s, "SELECT a FROM h WHERE b='x'"),
            vec![vec![V::Int(1)]]
        );
        assert_eq!(
            run(&mut s, "SELECT COUNT(*) FROM h"),
            vec![vec![V::Int(2)]]
        );
    }
}

#[test]
fn keyed_tables_keep_canonical_base_export_versions() {
    let mut paths = TestPaths::new();
    for (name, column_type, version) in [("plain", "TEXT", 3), ("date", "DATE", 15)] {
        let path = paths.fresh(&format!("keyed-{name}-format"));
        let export = paths.fresh(&format!("keyed-{name}-export"));
        let a = Admin::create(&path, Limits::default()).unwrap();
        run(
            &mut a.sql_session(),
            &format!("CREATE TABLE h (a BIGINT PRIMARY KEY, b {column_type})"),
        );
        assert_eq!(a.catalog().unwrap().1[0].hidden_rowid(), None);
        assert_eq!(export_version(&a, &export), version);
    }
}

#[test]
fn heap_searched_sql_mutations_work_and_native_scan_remains_unavailable() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    run(&mut s, "INSERT INTO h VALUES (1, 'x')");
    run(&mut s, "BEGIN");
    for sql in ["UPDATE h SET b='z' WHERE a=1", "DELETE FROM h WHERE a=1"] {
        assert_eq!(s.execute(sql, &[], None).unwrap().affected, 1, "{sql}");
    }
    run(&mut s, "ROLLBACK");
    assert_eq!(
        s.scan(Scan {
            table: "h".into(),
            projection: vec![],
            prefix: vec![],
            lower: None,
            upper: None,
            reverse: false,
            limit: 10,
        }),
        Err(CoreError::Sql(E::Unsupported))
    );
    assert_eq!(
        run(&mut s, "SELECT * FROM h"),
        vec![vec![V::Int(1), V::Text("x".into())]]
    );
}

#[test]
fn heap_multi_row_insert_maps_logical_columns_and_preserves_nulls() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    run(&mut s, "INSERT INTO h VALUES (3, 'c'), (NULL, 'n'), (1, NULL)");
    run(&mut s, "INSERT INTO h (b, a) VALUES ('b', 2), (NULL, NULL)");
    assert_eq!(
        run(&mut s, "SELECT * FROM h"),
        vec![
            vec![V::Int(3), V::Text("c".into())],
            vec![V::Null, V::Text("n".into())],
            vec![V::Int(1), V::Null],
            vec![V::Int(2), V::Text("b".into())],
            vec![V::Null, V::Null],
        ]
    );
    assert_eq!(run(&mut s, "SELECT COUNT(*) FROM h"), vec![vec![V::Int(5)]]);
    let insert = s.prepare("INSERT INTO h VALUES ($1, $2), ($3, $4)").unwrap();
    assert_eq!(
        s.describe(&insert).unwrap().parameters,
        [DataType::BigInt, DataType::Text, DataType::BigInt, DataType::Text]
    );
    s.execute_prepared(&insert, &[V::Int(4), V::Null, V::Null, V::Text("p".into())], None)
        .unwrap();
    assert_eq!(run(&mut s, "SELECT COUNT(*) FROM h"), vec![vec![V::Int(7)]]);
}

#[test]
fn heap_unique_indexes_build_on_populated_tables_and_reject_duplicates() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE duplicates (a BIGINT, b TEXT)");
    run(&mut s, "INSERT INTO duplicates VALUES (1, 'x'), (1, 'y')");
    assert_eq!(
        s.execute("CREATE UNIQUE INDEX duplicate_a ON duplicates (a)", &[], None),
        Err(CoreError::Sql(E::Duplicate))
    );
    assert_eq!(run(&mut s, "SELECT COUNT(*) FROM duplicates"), vec![vec![V::Int(2)]]);
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    run(&mut s, "INSERT INTO h VALUES (1, 'x'), (2, 'y')");
    run(&mut s, "CREATE UNIQUE INDEX unique_a ON h (a)");
    run(&mut s, "CREATE INDEX by_b ON h (b)");
    assert_eq!(
        s.execute("INSERT INTO h VALUES (1, 'z')", &[], None),
        Err(CoreError::Sql(E::Duplicate))
    );
    assert_eq!(run(&mut s, "SELECT b FROM h WHERE a=2"), vec![vec![V::Text("y".into())]]);
    assert_eq!(
        s.execute("CREATE INDEX hidden ON h (giggles_rowid)", &[], None),
        Err(CoreError::Sql(E::Reserved))
    );
}

#[test]
fn heap_on_conflict_explicit_unique_target_skips_and_updates() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    run(&mut s, "CREATE UNIQUE INDEX unique_a ON h (a)");
    run(&mut s, "INSERT INTO h VALUES (1, 'old')");
    assert_eq!(
        s.execute(
            "INSERT INTO h VALUES (1, 'skip'), (2, NULL) ON CONFLICT (a) DO NOTHING",
            &[],
            None
        )
        .unwrap()
        .affected,
        1
    );
    assert_eq!(
        s.execute(
            "INSERT INTO h (b, a) VALUES ('new', 1), (NULL, 3) ON CONFLICT (a) DO UPDATE SET b=EXCLUDED.b",
            &[],
            None
        )
        .unwrap()
        .affected,
        2
    );
    assert_eq!(
        run(&mut s, "SELECT * FROM h ORDER BY a"),
        vec![
            vec![V::Int(1), V::Text("new".into())],
            vec![V::Int(2), V::Null],
            vec![V::Int(3), V::Null],
        ]
    );
    assert_eq!(
        s.execute("INSERT INTO h VALUES (4, 'x') ON CONFLICT ON CONSTRAINT h_pkey DO NOTHING", &[], None),
        Err(CoreError::Sql(E::Unsupported))
    );
}

#[test]
fn heap_targetless_conflicts_use_only_unique_indexes() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT, b TEXT)");
    assert_eq!(
        s.execute(
            "INSERT INTO h VALUES (1, NULL), (1, NULL), (2, 'x') ON CONFLICT DO NOTHING",
            &[],
            None
        )
        .unwrap()
        .affected,
        3
    );
    assert_eq!(run(&mut s, "SELECT COUNT(*) FROM h"), vec![vec![V::Int(3)]]);
    run(&mut s, "CREATE TABLE indexed (a BIGINT, b TEXT)");
    run(&mut s, "CREATE UNIQUE INDEX indexed_a ON indexed (a)");
    run(&mut s, "INSERT INTO indexed VALUES (1, 'old')");
    assert_eq!(
        s.execute(
            "INSERT INTO indexed VALUES (1, 'skip'), (2, NULL), (2, 'skip'), (3, 'new') ON CONFLICT DO NOTHING",
            &[],
            None
        )
        .unwrap()
        .affected,
        2
    );
    assert_eq!(
        run(&mut s, "SELECT * FROM indexed ORDER BY a"),
        vec![
            vec![V::Int(1), V::Text("old".into())],
            vec![V::Int(2), V::Null],
            vec![V::Int(3), V::Text("new".into())],
        ]
    );
}

#[test]
fn heap_logical_arity_matches_keyed_errors_and_not_null_is_enforced() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE h (a BIGINT NOT NULL, b TEXT)");
    run(&mut s, "CREATE TABLE keyed (a BIGINT PRIMARY KEY, b TEXT)");
    for suffix in ["", " ON CONFLICT DO NOTHING"] {
        for table in ["h", "keyed"] {
            assert_eq!(s.execute(&format!("INSERT INTO {table} VALUES (1), (2){suffix}"), &[], None).unwrap().affected, 2);
            assert_eq!(run(&mut s, &format!("SELECT * FROM {table} ORDER BY a")), vec![vec![V::Int(1), V::Null], vec![V::Int(2), V::Null]]);
            run(&mut s, &format!("DELETE FROM {table}"));
        }
    }
    for values in ["(1, 'x', 9), (2, 'y', 8)", "(1, 'x', 'extra')"] {
        for suffix in ["", " ON CONFLICT DO NOTHING"] {
            let heap_error = s
                .execute(&format!("INSERT INTO h VALUES {values}{suffix}"), &[], None)
                .unwrap_err();
            let keyed_error = s
                .execute(&format!("INSERT INTO keyed VALUES {values}{suffix}"), &[], None)
                .unwrap_err();
            assert_eq!(heap_error, CoreError::Sql(E::Syntax));
            assert_eq!(heap_error, keyed_error);
        }
    }
    for sql in [
        "INSERT INTO h VALUES (1, 'ok'), (NULL, 'bad')",
        "INSERT INTO h (b) VALUES ('bad'), ('also bad')",
        "INSERT INTO h VALUES (1, 'ok'), (NULL, 'bad') ON CONFLICT DO NOTHING",
    ] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(E::Null)), "{sql}");
        assert_eq!(run(&mut s, "SELECT COUNT(*) FROM h"), vec![vec![V::Int(0)]]);
    }
}

#[test]
fn heap_maximum_logical_width_accepts_positional_multi_row_and_conflict_input() {
    let (_a, mut s) = db();
    let columns = (0..MAX_COLUMNS - 1)
        .map(|index| format!("c{index} BIGINT"))
        .collect::<Vec<_>>()
        .join(",");
    run(&mut s, &format!("CREATE TABLE h ({columns})"));
    let values = (0..MAX_COLUMNS - 1)
        .map(|_| "NULL")
        .collect::<Vec<_>>()
        .join(",");
    for suffix in ["", " ON CONFLICT DO NOTHING"] {
        run(
            &mut s,
            &format!("INSERT INTO h VALUES ({values}), ({values}){suffix}"),
        );
    }
    assert_eq!(
        run(&mut s, "SELECT COUNT(*) FROM h"),
        vec![vec![V::Int(4)]]
    );
    assert!(run(&mut s, "SELECT * FROM h")
        .iter()
        .all(|row| row == &vec![V::Null; MAX_COLUMNS - 1]));
}
