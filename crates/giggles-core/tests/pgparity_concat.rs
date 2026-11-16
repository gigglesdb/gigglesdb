mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn row(values: &[&str]) -> Vec<Value> {
    values
        .iter()
        .map(|s| {
            if *s == "<null>" {
                Value::Null
            } else {
                Value::Text((*s).into())
            }
        })
        .collect()
}
#[test]
fn array_overloads() {
    let path = common::fresh("parity-array-concat");
    let a = Admin::create(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE TABLE a(id int PRIMARY KEY, v text[], i int[])",
    );
    run(
        &mut s,
        "INSERT INTO a VALUES (1,'{a,b}','{1,2}'),(2,NULL,NULL),(3,'{}','{}')",
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT v || '{c,d}', v || 'c'::text, 'z'::text || v, v || v, v || NULL, v || NULL::text, NULL::text || v, i || 3, 0 || i, i || '{3}' FROM a ORDER BY id"
        ),
        vec![
            row(&[
                "{a,b,c,d}",
                "{a,b,c}",
                "{z,a,b}",
                "{a,b,a,b}",
                "{a,b}",
                "{a,b,NULL}",
                "{NULL,a,b}",
                "{1,2,3}",
                "{0,1,2}",
                "{1,2,3}"
            ]),
            row(&[
                "{c,d}", "{c}", "{z}", "<null>", "<null>", "{NULL}", "{NULL}", "{3}", "{0}", "{3}"
            ]),
            row(&[
                "{c,d}", "{c}", "{z}", "{}", "{}", "{NULL}", "{NULL}", "{3}", "{0}", "{3}"
            ])
        ]
    );
    for (sql, error) in [
        (
            "SELECT v || 'c' FROM a",
            SqlError::InvalidTextRepresentation,
        ),
        (
            "SELECT i || 'x' FROM a",
            SqlError::InvalidTextRepresentation,
        ),
        ("SELECT v || 1 FROM a", SqlError::UndefinedFunction),
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(error)),
            "{sql}"
        );
    }
    assert_eq!(
        run(&mut s, "SELECT v || '{c}'::text[] FROM a ORDER BY id"),
        vec![row(&["{a,b,c}"]), row(&["{c}"]), row(&["{c}"])]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT '{a}'::text[] || '{b}', 'z'::text || '{a}'::text[], 'x'::text || 1"
        ),
        vec![row(&["{a,b}", "{z,a}", "x1"])]
    );
    drop(s);
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn jsonb_overloads() {
    let path = common::fresh("parity-jsonb-concat");
    let a = Admin::create(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(&mut s, "CREATE TABLE a(id int PRIMARY KEY, j jsonb)");
    run(
        &mut s,
        r#"INSERT INTO a VALUES (1,'{"a":1}'),(2,NULL),(3,'[1]')"#,
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT j || '{"b":2}', j || '[2]', j || '3', j || j, j || NULL, '"s"'::jsonb || j FROM a ORDER BY id"#
        ),
        vec![
            row(&[
                r#"{"a": 1, "b": 2}"#,
                r#"[{"a": 1}, 2]"#,
                r#"[{"a": 1}, 3]"#,
                r#"{"a": 1}"#,
                "<null>",
                r#"["s", {"a": 1}]"#
            ]),
            vec![Value::Null; 6],
            row(&[
                r#"[1, {"b": 2}]"#,
                "[1, 2]",
                "[1, 3]",
                "[1, 1]",
                "<null>",
                r#"["s", 1]"#
            ])
        ]
    );
    assert_eq!(
        s.execute("SELECT j || 'notjson' FROM a", &[], None),
        Err(CoreError::Sql(SqlError::InvalidTextRepresentation))
    );
    assert_eq!(
        run(&mut s, r#"SELECT '{"a":1}'::jsonb || '{"a":2}'::jsonb"#),
        vec![row(&[r#"{"a": 2}"#])]
    );
    drop(s);
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn grouped_enum_order() {
    let path = common::fresh("parity-enum-group");
    let a = Admin::create(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(&mut s, "CREATE TYPE e AS ENUM ('z','a','m')");
    run(&mut s, "CREATE TABLE t(id int PRIMARY KEY, v e)");
    run(
        &mut s,
        "INSERT INTO t VALUES (1,'z'),(2,'a'),(3,'m'),(4,NULL)",
    );
    for (order, expected) in [
        ("v", vec!["z", "a", "m", "<null>"]),
        ("v DESC", vec!["<null>", "m", "a", "z"]),
    ] {
        let actual = run(
            &mut s,
            &format!("SELECT v, count(*) FROM t GROUP BY v ORDER BY {order}"),
        );
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|v| vec![row(&[v])[0].clone(), Value::Int(1)])
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(
        run(&mut s, "SELECT DISTINCT v FROM t ORDER BY 1"),
        vec![row(&["z"]), row(&["a"]), row(&["m"]), row(&["<null>"])]
    );
    drop(s);
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
