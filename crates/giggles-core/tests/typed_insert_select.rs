mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::*};
use ReadExpr::{Column as C, Literal as L};
fn read() -> SingleSourceRead {
    SingleSourceRead { table: "t".into(), projection: vec![ReadProjection {
        expression: C("id".into()), alias: None }], filter: None }
}
fn insert(source: TypedRead) -> TypedInsertSelect {
    TypedInsertSelect { table: "dest".into(), columns: vec!["id".into()], source }
}
fn trap<T>(f: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")] { with_sql_parser_forbidden(f) }
    #[cfg(not(feature = "test-sql-parser-trap"))] { f() }
}
#[test]
fn inserts_share_defaults_counts_constraints_and_transactions_with_sql() {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")] engines.push(Engine::V2);
    for engine in engines {
        let path = common::fresh("typed-insert-select");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in ["CREATE TABLE t(id INT PRIMARY KEY,n INT)", "CREATE TABLE u(n INT)",
            "INSERT INTO t VALUES(1,1),(2,2),(3,NULL)", "INSERT INTO u VALUES(2),(NULL)",
            "CREATE TABLE dest(id INT PRIMARY KEY,n INT NOT NULL DEFAULT 9)"] {
            s.execute(sql, &[], None).unwrap();
        }
        let q = |source: &str, column: &str| ReadExpr::QualifiedColumn { source: source.into(), column: column.into() };
        let joined = TwoSourceRead {
            left: ReadSource { table: "t".into(), alias: "a".into() },
            right: ReadSource { table: "u".into(), alias: "b".into() }, kind: ReadJoinKind::Inner,
            on: ReadPredicate::Compare(q("a", "n"), ReadCompare::Eq, q("b", "n")),
            group_by: vec![], projection: vec![ReadProjection { expression: q("a", "id"), alias: None }], filter: None,
        };
        let mut filtered = read();
        filtered.filter = Some(ReadPredicate::In { expr: C("n".into()), subquery: Box::new(SingleSourceRead {
            table: "u".into(), projection: vec![ReadProjection { expression: C("n".into()), alias: None }], filter: None }) });
        for (source, select, count) in [(TypedRead::Single(Box::new(read())), "SELECT id FROM t", 3),
            (TypedRead::Join(Box::new(joined)), "SELECT a.id FROM t a JOIN u b ON a.n=b.n", 1),
            (TypedRead::Single(Box::new(filtered)), "SELECT id FROM t WHERE n IN(SELECT n FROM u)", 1)] {
            s.execute("BEGIN", &[], None).unwrap();
            let sql = format!("INSERT INTO dest(id) {select}");
            let expected = s.execute(&sql, &[], None).unwrap();
            let rows = s.execute("SELECT * FROM dest ORDER BY id", &[], None).unwrap();
            s.execute("ROLLBACK", &[], None).unwrap();
            s.execute("BEGIN", &[], None).unwrap();
            let actual = trap(|| s.execute_typed_insert_select(insert(source.clone()), None)).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual.affected, count);
            let actual_rows = s.execute("SELECT * FROM dest ORDER BY id", &[], None).unwrap();
            assert_eq!(actual_rows.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
                rows.rows.iter().map(|r| &r.values).collect::<Vec<_>>());
            assert!(rows.rows.iter().all(|r| r.values[1] == Value::Int(9)));
            s.execute("ROLLBACK", &[], None).unwrap();
            let p = trap(|| Prepared::from_typed_insert_select(0, 0, insert(source))).unwrap();
            assert_eq!(p.table(), Some("dest"));
        }
        // CoreError identity preserves the server SQLSTATE: Duplicate=23505, Null=23502.
        s.execute("INSERT INTO dest(id) SELECT id FROM t", &[], None).unwrap();
        for (select, source, columns, error, state) in [
            ("SELECT id FROM t", read(), vec!["id".into()], SqlError::Duplicate, "23505"),
            ("SELECT 4,NULL FROM t", SingleSourceRead { projection: vec![
                ReadProjection { expression: L(Value::Int(4)), alias: None },
                ReadProjection { expression: L(Value::Null), alias: None }], ..read() }, vec!["id".into(), "n".into()], SqlError::Null, "23502"),
        ] {
            let sql = format!("INSERT INTO dest({}) {select}", columns.join(","));
            let expected = s.execute(&sql, &[], None);
            assert_eq!(expected, Err(CoreError::Sql(error)));
            assert_eq!(giggles_core::sqlstate(expected.as_ref().unwrap_err()), state);
            let typed = TypedInsertSelect { columns, ..insert(TypedRead::Single(Box::new(source))) };
            assert_eq!(trap(|| s.execute_typed_insert_select(typed, None)), expected);
        }
        assert_eq!(s.execute("SELECT * FROM dest", &[], None).unwrap().rows.len(), 3);
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn insert_requires_explicit_bounded_columns_and_exact_target_names() {
    for columns in [vec![], vec!["id".into(); MAX_TABLE_COLUMNS + 1]] {
        let input = TypedInsertSelect { columns, ..insert(TypedRead::Single(Box::new(read()))) };
        assert!(matches!(Prepared::from_typed_insert_select(0, 0, input), Err(SqlError::Limit)));
    }
    for target in ["dest.secret.x", ".dest", "app.", "App.dest", "app.Dest", "app.de st", "app.1dest"] {
        let input = TypedInsertSelect { table: target.into(), ..insert(TypedRead::Single(Box::new(read()))) };
        assert!(matches!(Prepared::from_typed_insert_select(0, 0, input), Err(SqlError::Syntax)));
    }
    let input = TypedInsertSelect { columns: vec!["dest.id".into()], ..insert(TypedRead::Single(Box::new(read()))) };
    assert!(matches!(Prepared::from_typed_insert_select(0, 0, input), Err(SqlError::Syntax)));
}

#[test]
fn qualified_insert_targets_match_sql_and_search_path() {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")] engines.push(Engine::V2);
    for engine in engines {
        for (target, path, destination) in [("app.dest", "public", "app.dest"),
            ("dest", "app, public", "app.dest"), ("dest", "public", "public.dest"),
            ("public.dest", "app, public", "public.dest")] {
            let paths = [common::fresh("typed-qualified-insert"), common::fresh("sql-qualified-insert")];
            let admins = paths.iter().map(|p| Admin::create_engine(p, Limits::default(), engine).unwrap()).collect::<Vec<_>>();
            let mut typed = admins[0].sql_session();
            let mut sql = admins[1].sql_session();
            for s in [&mut typed, &mut sql] {
                for statement in ["CREATE SCHEMA app", "CREATE TABLE app.dest(id INT PRIMARY KEY)",
                    "CREATE TABLE public.dest(id INT PRIMARY KEY)", "CREATE TABLE src(id INT)",
                    "INSERT INTO src VALUES(1),(2)"] { s.execute(statement, &[], None).unwrap(); }
                s.set_search_path(path, "postgres");
            }
            let input = || TypedInsertSelect { table: target.into(), columns: vec!["id".into()],
                source: TypedRead::Single(Box::new(SingleSourceRead { table: "src".into(), ..read() })) };
            let actual = trap(|| typed.execute_typed_insert_select(input(), None)).unwrap();
            let expected = sql.execute(&format!("INSERT INTO {target}(id) SELECT id FROM src"), &[], None).unwrap();
            assert_eq!(actual.affected, expected.affected);
            assert_eq!(actual.affected, 2);
            for table in ["app.dest", "public.dest"] {
                let query = format!("SELECT id FROM {table} ORDER BY id");
                let actual = typed.execute(&query, &[], None).unwrap();
                let expected = sql.execute(&query, &[], None).unwrap();
                assert_eq!(actual.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
                    expected.rows.iter().map(|r| &r.values).collect::<Vec<_>>());
                assert_eq!(actual.rows.len(), if table == destination { 2 } else { 0 });
            }
            for target in ["nope.dest", "app.missing"] {
                let request = TypedInsertSelect { table: target.into(), ..input() };
                let actual = trap(|| typed.execute_typed_insert_select(request, None)).unwrap_err();
                let expected = sql.execute(&format!("INSERT INTO {target}(id) SELECT id FROM src"), &[], None).unwrap_err();
                assert_eq!(giggles_core::sqlstate(&actual), "42P01");
                assert_eq!(giggles_core::sqlstate(&actual), giggles_core::sqlstate(&expected));
            }
            for s in [&mut typed, &mut sql] {
                assert_eq!(s.execute(&format!("SELECT id FROM {destination}"), &[], None).unwrap().rows.len(), 2);
            }
            drop(typed); drop(sql); drop(admins);
            for p in paths { std::fs::remove_dir_all(p).unwrap(); }
        }
    }
}
