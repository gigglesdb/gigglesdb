mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, SqlResult, Value as V},
};

fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("relational-scan-budget");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    test(&mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(s: &mut SqlSession, sql: &str, params: &[V]) -> SqlResult {
    s.execute(sql, params, None).expect(sql)
}

fn values(result: SqlResult) -> Vec<Vec<V>> {
    result.rows.into_iter().map(|r| r.values).collect()
}

#[test]
fn scans_get_fresh_evaluation_budgets_but_keep_statement_output_limits() {
    database(|s| {
        run(s, "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)", &[]);
        let name = "A".repeat(1000);
        let lower = name.to_ascii_lowercase();
        assert!(2200 * name.len() > 2 * 1024 * 1024);
        let insert = s.prepare("INSERT INTO t VALUES ($1,$2)").unwrap();
        run(s, "BEGIN", &[]);
        for id in 0..2200 {
            s.execute_prepared(&insert, &[V::Int(id), V::Text(name.clone())], None)
                .unwrap();
        }
        run(s, "COMMIT", &[]);

        for (sql, params, count) in [
            (
                "SELECT id FROM t WHERE name=$1",
                vec![V::Text(name.clone())],
                2200,
            ),
            (
                "SELECT id FROM t WHERE name=$1",
                vec![V::Text("absent".into())],
                0,
            ),
            (
                "SELECT id FROM t WHERE lower(name)=$1",
                vec![V::Text(lower.clone())],
                2200,
            ),
        ] {
            let actual = values(run(s, sql, &params));
            let expected = (0..count).map(|id| vec![V::Int(id)]).collect::<Vec<_>>();
            assert_eq!(actual, expected, "{sql}");
        }
        for sql in [
            "SELECT count(*) FROM t WHERE name=$1",
            "SELECT count(*) FROM t WHERE lower(name)=lower($1)",
        ] {
            let params = if sql.contains('$') {
                vec![V::Text(name.clone())]
            } else {
                vec![]
            };
            assert_eq!(values(run(s, sql, &params)), [vec![V::Int(2200)]], "{sql}");
        }
        assert_eq!(
            values(run(s, "SELECT length(lower(name)) FROM t", &[])),
            vec![vec![V::Int(1000)]; 2200],
        );
        assert_eq!(
            values(run(
                s,
                "SELECT id,lower(name) FROM t WHERE name=$1 ORDER BY id LIMIT 3",
                &[V::Text(name.clone())]
            )),
            (0..3)
                .map(|id| vec![V::Int(id), V::Text(lower.clone())])
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            values(run(
                s,
                "SELECT id FROM t ORDER BY lower(name),id LIMIT 3",
                &[]
            )),
            [vec![V::Int(0)], vec![V::Int(1)], vec![V::Int(2)]],
        );
        assert_eq!(
            values(run(
                s,
                "SELECT count(*) FROM t a JOIN t b ON a.id=b.id WHERE lower(b.name)=$1",
                &[V::Text(lower.clone())]
            )),
            [vec![V::Int(2200)]],
        );
        assert_eq!(
            s.execute("SELECT name FROM t", &[], None),
            Err(CoreError::Sql(SqlError::Limit))
        );

        for sql in [
            "SELECT id FROM t WHERE name='x'",
            "SELECT id FROM t WHERE lower(name)='x'",
        ] {
            assert!(run(s, sql, &[]).rows.is_empty(), "{sql}");
        }
        assert_eq!(
            values(run(s, "SELECT count(*) FROM t WHERE name<>'x'", &[])),
            [vec![V::Int(2200)]]
        );
        assert_eq!(
            values(run(
                s,
                "SELECT id FROM t WHERE lower(name)=$1 ORDER BY id LIMIT 5",
                &[V::Text(lower)]
            )),
            (0..5).map(|id| vec![V::Int(id)]).collect::<Vec<_>>()
        );
        // These no-match mutations must visit and evaluate every text row.
        for sql in [
            "UPDATE t SET name='y' WHERE lower(name)='x'",
            "DELETE FROM t WHERE lower(name)='x'",
        ] {
            assert_eq!(run(s, sql, &[]).affected, 0);
        }
        assert_eq!(
            run(
                s,
                "UPDATE t SET name='y' WHERE id<10 AND name=$1",
                &[V::Text(name)]
            )
            .affected,
            10
        );
        assert_eq!(
            run(s, "DELETE FROM t WHERE id<5 AND lower(name)='y'", &[]).affected,
            5
        );
        assert_eq!(
            values(run(s, "SELECT id FROM t WHERE name='y' ORDER BY id", &[])),
            (5..10).map(|id| vec![V::Int(id)]).collect::<Vec<_>>()
        );
        assert_eq!(
            values(run(s, "SELECT count(*) FROM t", &[])),
            [vec![V::Int(2195)]]
        );
        // Aggregate inputs still share one statement-wide evaluation budget
        // (DD-0068 aggregate_scalar_work_budget_is_shared_over_full_input).
        // PostgreSQL has no such limit; changing it is a separate DD-0068 item.
        assert_eq!(
            s.execute("SELECT count(lower(name)) FROM t", &[], None),
            Err(CoreError::Sql(SqlError::Limit))
        );
    });
}

#[test]
fn a_single_row_still_cannot_exceed_the_evaluation_budget() {
    database(|s| {
        run(s, "CREATE TABLE wide (name TEXT)", &[]);
        let text = "A".repeat(60 * 1024);
        run(s, "INSERT INTO wide VALUES ($1)", &[V::Text(text)]);
        assert_eq!(
            values(run(s, "SELECT length(name) FROM wide", &[])),
            [vec![V::Int(61440)]]
        );
        // Each true clause reads and lowercases the same 60 KiB value. The
        // conjunction must share one row budget, rather than reset per clause.
        let clause = "length(lower(name))=61440";
        let predicate = [clause; 10].join(" AND ");
        assert_eq!(
            s.execute(&format!("SELECT 1 FROM wide WHERE {predicate}"), &[], None),
            Err(CoreError::Sql(SqlError::Limit)),
        );
    });
}
