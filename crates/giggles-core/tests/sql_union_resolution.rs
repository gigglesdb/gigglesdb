mod common;

use giggles_core::{
    Admin, Limits,
    relational::{DataType, Value, take_sql_error_message},
};

#[test]
fn set_operations_resolve_unknown_projections_with_subqueries() {
    use Value::{Int, Null, Text};

    let path = common::fresh("union-resolution");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();

    // PostgreSQL 18.6 resolves each pair at its set-operation boundary.
    let cases = [
        (
            "SELECT 1 UNION ALL SELECT NULL WHERE EXISTS (SELECT 1) ORDER BY 1",
            vec![Int(1), Null],
        ),
        (
            "SELECT '2' WHERE EXISTS (SELECT 1) UNION ALL SELECT 1 ORDER BY 1",
            vec![Int(1), Int(2)],
        ),
        (
            "SELECT 1 INTERSECT SELECT '1' WHERE EXISTS (SELECT 1)",
            vec![Int(1)],
        ),
        ("SELECT 1 EXCEPT SELECT '1' WHERE EXISTS (SELECT 1)", vec![]),
        (
            "SELECT 'a' WHERE EXISTS (SELECT 1) UNION ALL SELECT NULL ORDER BY 1",
            vec![Text("a".into()), Null],
        ),
    ];
    for (sql, expected) in cases {
        let result = session
            .execute(sql, &[], None)
            .unwrap_or_else(|error| panic!("{sql}: {error:?} {:?}", take_sql_error_message()));
        let values: Vec<_> = result
            .rows
            .into_iter()
            .map(|mut row| row.values.remove(0))
            .collect();
        assert_eq!(values, expected, "{sql}");
    }

    for sql in [
        "SELECT 1 UNION ALL SELECT NULL::text WHERE EXISTS (SELECT 1)",
        "SELECT NULL WHERE EXISTS (SELECT 1) UNION ALL SELECT NULL UNION ALL SELECT 1",
        "SELECT 1 UNION ALL SELECT DISTINCT NULL WHERE EXISTS (SELECT 1)",
        "SELECT 1 UNION ALL (SELECT NULL WHERE EXISTS (SELECT 1) ORDER BY 1)",
        "SELECT 1 UNION ALL SELECT CASE WHEN true THEN NULL END WHERE EXISTS (SELECT 1)",
    ] {
        let error = session.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "42804", "{sql}: {error:?}");
    }
    let invalid = session
        .execute(
            "SELECT 1 UNION ALL SELECT 'bad' WHERE EXISTS (SELECT 1)",
            &[],
            None,
        )
        .unwrap_err();
    assert_eq!(giggles_core::sqlstate(&invalid), "22P02");

    let prepared = session
        .prepare("SELECT 1 UNION ALL SELECT $1 WHERE EXISTS (SELECT 1) ORDER BY 1")
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        [DataType::Integer],
    );
    let rows = session
        .execute_prepared(&prepared, &[Int(2)], None)
        .unwrap()
        .rows;
    assert_eq!(
        rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
        [vec![Int(1)], vec![Int(2)]],
    );

    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
