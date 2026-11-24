//! PostgreSQL 18.6 witnesses from the AM-8 b_srf and AM-2d g1/g1t probes.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{DataType, SqlError, SqlResult, Value, take_sql_error_message},
};

fn with_session(test: impl FnOnce(&mut SqlSession)) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "insert-select-sources-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    ));
    std::fs::create_dir(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    test(&mut admin.sql_session());
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(db: &mut SqlSession, sql: &str) -> SqlResult {
    db.execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn rows(result: SqlResult) -> Vec<Vec<Value>> {
    result.rows.into_iter().map(|row| row.values).collect()
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn pairs(values: &[(i64, &str)]) -> Vec<Vec<Value>> {
    values
        .iter()
        .map(|&(id, value)| vec![Value::Int(id), text(value)])
        .collect()
}

fn source(db: &mut SqlSession) {
    run(
        db,
        "CREATE TABLE src (id int PRIMARY KEY, tags text[], note text)",
    );
    run(
        db,
        "INSERT INTO src VALUES (1,'{a,b}','x1'), (2,'{c}','x2'), (3,'{}','x3')",
    );
}

#[test]
fn insert_materializes_standalone_select_shapes() {
    with_session(|db| {
        source(db);
        assert_eq!(
            rows(run(db, "SELECT id,unnest(tags) FROM src")),
            pairs(&[(1, "a"), (1, "b"), (2, "c")]),
        );
        run(db, "CREATE TABLE dst (id int, tag text, n bigint)");
        for (sql, affected) in [
            (
                "INSERT INTO dst (id, tag) SELECT s.id, t FROM src s CROSS JOIN LATERAL unnest(s.tags) AS t",
                3,
            ),
            (
                "INSERT INTO dst (id, tag) SELECT 100, u FROM unnest(ARRAY['p','q']) u",
                2,
            ),
            (
                "INSERT INTO dst (id, tag) SELECT g, 'g' FROM generate_series(1,3) g",
                3,
            ),
            (
                "INSERT INTO dst (id, tag) SELECT DISTINCT s.id,t.x FROM src s, unnest(s.tags) WITH ORDINALITY AS t(x,o) WHERE o=1",
                2,
            ),
            (
                "INSERT INTO dst (id, tag) SELECT id,note FROM src WHERE id=1 UNION ALL SELECT id,note FROM src WHERE id=2",
                2,
            ),
            (
                "INSERT INTO dst (id, tag) SELECT id,note FROM src UNION SELECT id,note FROM src ORDER BY 1 LIMIT 2",
                2,
            ),
            (
                "INSERT INTO dst (id, tag,n) SELECT id,note,row_number() OVER (ORDER BY id DESC) FROM src",
                3,
            ),
            (
                "INSERT INTO dst (id, tag,n) SELECT id,note,count(*) OVER () FROM src WHERE id>1",
                2,
            ),
            (
                "INSERT INTO dst (id, tag) SELECT id,tag FROM (SELECT id,unnest(tags) AS tag FROM src) z",
                3,
            ),
            (
                "INSERT INTO dst (id, tag) SELECT id,unnest(tags) FROM src",
                3,
            ),
            (
                "WITH w AS (SELECT id,rank() OVER (ORDER BY id) r FROM src) INSERT INTO dst (id,n) SELECT id,r FROM w",
                3,
            ),
        ] {
            assert_eq!(run(db, sql).affected, affected, "{sql}");
        }
        assert_eq!(
            rows(run(
                db,
                "INSERT INTO dst (id,tag) VALUES (900,'v') RETURNING *"
            )),
            vec![vec![Value::Int(900), text("v"), Value::Null]]
        );
        let returning = run(
            db,
            "INSERT INTO dst (id,tag) SELECT s.id,t FROM src s CROSS JOIN LATERAL unnest(s.tags) AS t RETURNING id,tag",
        );
        assert_eq!(returning.affected, 3);
        assert_eq!(rows(returning), pairs(&[(1, "a"), (1, "b"), (2, "c")]));

        let expected = [
            (1, Some("a"), None),
            (1, Some("a"), None),
            (1, Some("a"), None),
            (1, Some("a"), None),
            (1, Some("a"), None),
            (1, Some("b"), None),
            (1, Some("b"), None),
            (1, Some("b"), None),
            (1, Some("b"), None),
            (1, Some("g"), None),
            (1, Some("x1"), Some(3)),
            (1, Some("x1"), None),
            (1, Some("x1"), None),
            (1, None, Some(1)),
            (2, Some("c"), None),
            (2, Some("c"), None),
            (2, Some("c"), None),
            (2, Some("c"), None),
            (2, Some("c"), None),
            (2, Some("g"), None),
            (2, Some("x2"), Some(2)),
            (2, Some("x2"), Some(2)),
            (2, Some("x2"), None),
            (2, Some("x2"), None),
            (2, None, Some(2)),
            (3, Some("g"), None),
            (3, Some("x3"), Some(1)),
            (3, Some("x3"), Some(2)),
            (3, None, Some(3)),
            (100, Some("p"), None),
            (100, Some("q"), None),
            (900, Some("v"), None),
        ];
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(id, tag, n)| {
                vec![
                    Value::Int(id),
                    tag.map_or(Value::Null, text),
                    n.map_or(Value::Null, Value::Int),
                ]
            })
            .collect();
        assert_eq!(
            rows(run(db, "SELECT * FROM dst ORDER BY id,tag,n")),
            expected
        );

        run(db, "CREATE TABLE set_ops (id int)");
        for sql in [
            "INSERT INTO set_ops SELECT id FROM src INTERSECT SELECT 2",
            "INSERT INTO set_ops SELECT id FROM src EXCEPT SELECT 2 ORDER BY 1 DESC LIMIT 1",
            "INSERT INTO set_ops WITH w AS (SELECT count(*) OVER () AS n FROM src) SELECT n FROM w LIMIT 1",
        ] {
            assert_eq!(run(db, sql).affected, 1, "{sql}");
        }
        assert_eq!(
            rows(run(db, "SELECT id FROM set_ops ORDER BY id")),
            vec![
                vec![Value::Int(2)],
                vec![Value::Int(3)],
                vec![Value::Int(3)]
            ]
        );
    });
}

#[test]
fn select_list_srf_refuses_distinct_on_and_unsupported_shapes() {
    with_session(|db| {
        source(db);
        run(db, "CREATE TABLE dst (id int, tag text)");
        let select = "SELECT DISTINCT ON (id) id, unnest(tags) FROM src ORDER BY id";
        for sql in [
            select.to_owned(),
            format!("INSERT INTO dst {select}"),
            format!("CREATE TABLE ctas AS {select}"),
            "SELECT ARRAY(SELECT unnest(ARRAY[3,1,2]) ORDER BY 1)".into(),
            "SELECT ARRAY(SELECT unnest(ARRAY[3,1,2]))".into(),
            "SELECT generate_series('2026-01-01'::date,'2026-01-02','1 day')".into(),
        ] {
            assert_eq!(
                db.execute(&sql, &[], None),
                Err(CoreError::Sql(SqlError::Unsupported)),
                "{sql}"
            );
        }
        assert_eq!(
            db.execute("SELECT unnest(tags) FROM src HAVING true", &[], None),
            Err(CoreError::Sql(SqlError::GroupingError))
        );
        assert_eq!(
            rows(run(db, "SELECT DISTINCT id, unnest(tags) FROM src ORDER BY 1, 2")),
            pairs(&[(1, "a"), (1, "b"), (2, "c")])
        );
        assert!(rows(run(db, "SELECT * FROM dst")).is_empty());
        assert_eq!(
            db.execute("SELECT * FROM ctas", &[], None),
            Err(CoreError::Sql(SqlError::UnknownTable))
        );
    });
}

#[test]
fn ctas_keeps_select_names_types_and_no_data() {
    with_session(|db| {
        source(db);
        for (create, affected, select, labels, values, types) in [
            (
                "CREATE TABLE c1 AS SELECT s.id,t FROM src s CROSS JOIN LATERAL unnest(s.tags) AS t",
                3,
                "SELECT * FROM c1 ORDER BY 1,2",
                vec!["id", "t"],
                pairs(&[(1, "a"), (1, "b"), (2, "c")]),
                vec![DataType::Integer, DataType::Text],
            ),
            (
                "CREATE TABLE c2 AS SELECT id,note FROM src WHERE id=1 UNION ALL SELECT id,note FROM src WHERE id=3",
                2,
                "SELECT * FROM c2 ORDER BY 1",
                vec!["id", "note"],
                pairs(&[(1, "x1"), (3, "x3")]),
                vec![DataType::Integer, DataType::Text],
            ),
            (
                "CREATE TABLE c3 AS SELECT id,row_number() OVER (ORDER BY id DESC) AS rn FROM src",
                3,
                "SELECT * FROM c3 ORDER BY 1",
                vec!["id", "rn"],
                vec![
                    vec![Value::Int(1), Value::Int(3)],
                    vec![Value::Int(2), Value::Int(2)],
                    vec![Value::Int(3), Value::Int(1)],
                ],
                vec![DataType::Integer, DataType::BigInt],
            ),
            (
                "CREATE TABLE c4 AS SELECT g AS n FROM generate_series(1,2) g WITH NO DATA",
                0,
                "SELECT * FROM c4",
                vec!["n"],
                vec![],
                vec![DataType::Integer],
            ),
            (
                "CREATE TABLE c5 (a,b) AS SELECT id,unnest(tags) FROM src",
                3,
                "SELECT * FROM c5 ORDER BY 1,2",
                vec!["a", "b"],
                pairs(&[(1, "a"), (1, "b"), (2, "c")]),
                vec![DataType::Integer, DataType::Text],
            ),
        ] {
            assert_eq!(run(db, create).affected, affected, "{create}");
            let prepared = db.prepare(select).unwrap();
            let description = db.describe(&prepared).unwrap();
            assert_eq!(
                description
                    .columns
                    .iter()
                    .map(|c| c.data_type.clone())
                    .collect::<Vec<_>>(),
                types
            );
            let result = run(db, select);
            assert_eq!(result.columns, labels);
            assert_eq!(rows(result), values);
        }
        run(
            db,
            "CREATE TABLE unevaluated AS SELECT g FROM generate_series(1,2,0) g WITH NO DATA",
        );
        assert!(rows(run(db, "SELECT * FROM unevaluated")).is_empty());
        assert_eq!(
            run(db, "CREATE TABLE IF NOT EXISTS c2 AS SELECT 1/0 AS bad").affected,
            0
        );
        assert_eq!(
            rows(run(db, "SELECT count(*) FROM c2")),
            vec![vec![Value::Int(2)]]
        );
    });
}

#[test]
fn enum_and_array_assignment_in_set_and_window_sources() {
    with_session(|db| {
        for sql in [
            "CREATE TYPE mood AS ENUM ('z','a','m')",
            "CREATE TABLE enum_source (id int PRIMARY KEY, feeling mood, tags mood[])",
            "CREATE TABLE enum_overlap (id int PRIMARY KEY, feeling mood, tags mood[])",
            "INSERT INTO enum_source VALUES (1,'a','{z,a}'),(2,'m','{m}'),(3,'z','{}')",
            "INSERT INTO enum_overlap(id,feeling,tags) SELECT s.id+10,s.feeling,s.tags FROM enum_source s JOIN (VALUES (1)) v(x) ON s.id=v.x",
            "INSERT INTO enum_overlap(id,feeling) SELECT id+20,feeling FROM enum_source WHERE id=1 UNION ALL SELECT id+30,feeling FROM enum_source WHERE id=2",
            "WITH c AS (SELECT id,feeling FROM enum_source WHERE id=3) INSERT INTO enum_overlap(id,feeling) SELECT id+40,feeling FROM c",
            "INSERT INTO enum_overlap(id,feeling) SELECT id+50,first_value(feeling) OVER (ORDER BY feeling) FROM enum_source",
            "INSERT INTO enum_overlap(id,feeling) SELECT s.id+60,s.feeling FROM enum_source s,LATERAL (VALUES (s.id)) v(x)",
            "UPDATE enum_overlap o SET feeling=s.feeling FROM enum_source s WHERE o.id=s.id+10",
            "INSERT INTO enum_overlap(id,feeling) SELECT id+70,COALESCE(feeling,'a') FROM enum_source WHERE id=1",
            "INSERT INTO enum_overlap(id,feeling) SELECT id+80,CASE WHEN id=1 THEN feeling ELSE 'm' END FROM enum_source",
        ] {
            run(db, sql);
        }
        assert_eq!(
            rows(run(db, "SELECT id,feeling FROM enum_overlap ORDER BY id")),
            pairs(&[
                (11, "a"),
                (21, "a"),
                (32, "m"),
                (43, "z"),
                (51, "z"),
                (52, "z"),
                (53, "z"),
                (61, "a"),
                (62, "m"),
                (63, "z"),
                (71, "a"),
                (81, "a"),
                (82, "m"),
                (83, "m"),
            ])
        );
        assert_eq!(
            rows(run(db, "SELECT tags FROM enum_overlap WHERE id=11")),
            vec![vec![text("{z,a}")]]
        );
        assert_eq!(
            rows(run(
                db,
                "SELECT count(*) FROM enum_overlap WHERE tags IS NULL"
            )),
            vec![vec![Value::Int(13)]]
        );

        run(
            db,
            "INSERT INTO enum_overlap(id,tags) SELECT id+100,tags FROM enum_source WHERE id=1 UNION ALL SELECT id+100,tags FROM enum_source WHERE id=2",
        );
        run(
            db,
            "INSERT INTO enum_overlap(id,tags) SELECT id+110,first_value(tags) OVER (ORDER BY id) FROM enum_source",
        );
        assert_eq!(
            rows(run(
                db,
                "SELECT id,tags FROM enum_overlap WHERE id>=100 ORDER BY id"
            )),
            pairs(&[
                (101, "{z,a}"),
                (102, "{m}"),
                (111, "{z,a}"),
                (112, "{z,a}"),
                (113, "{z,a}")
            ])
        );

        for sql in [
            "CREATE TABLE ts (id int PRIMARY KEY, feeling text)",
            "CREATE TABLE tov (id int PRIMARY KEY, feeling text)",
            "INSERT INTO ts VALUES (1,'a'),(2,'m'),(3,'z')",
            "INSERT INTO tov(id,feeling) SELECT id+20,feeling FROM ts WHERE id=1 UNION ALL SELECT id+30,feeling FROM ts WHERE id=2",
            "INSERT INTO tov(id,feeling) SELECT id+50,first_value(feeling) OVER (ORDER BY feeling) FROM ts",
        ] {
            run(db, sql);
        }
        assert_eq!(
            rows(run(db, "SELECT id,feeling FROM tov ORDER BY id")),
            pairs(&[(21, "a"), (32, "m"), (51, "a"), (52, "a"), (53, "a")])
        );
    });
}

#[test]
fn coercion_conflicts_and_late_constraint_errors_use_insert_semantics() {
    with_session(|db| {
        run(db, "CREATE TYPE mood AS ENUM ('z','a','m')");
        run(
            db,
            "CREATE TABLE dst (id int, uid uuid, feeling mood DEFAULT 'm', PRIMARY KEY (id,uid))",
        );
        let uuid = "b0000000-0000-0000-0000-000000000001";
        let insert = format!(
            "INSERT INTO dst (id,uid) SELECT DISTINCT s.id,m.x::uuid
            FROM (VALUES (1,'{{{uuid},{uuid}}}'),(2,'{{{uuid}}}')) s(id,tags)
            CROSS JOIN LATERAL unnest(s.tags::text[]) AS m(x)
            ON CONFLICT (id,uid) DO NOTHING RETURNING id,feeling"
        );
        assert_eq!(rows(run(db, &insert)), pairs(&[(1, "m"), (2, "m")]));
        assert_eq!(run(db, &insert).affected, 0);
        let insert = format!(
            "INSERT INTO dst SELECT g,'{uuid}'::uuid,'a' FROM generate_series(3,3) g RETURNING feeling"
        );
        assert_eq!(rows(run(db, &insert)), vec![vec![text("a")]]);

        run(db, "CREATE TABLE nn (id int, ids uuid[] NOT NULL)");
        let error = db.execute("INSERT INTO nn SELECT 1,ARRAY['b0000000-0000-0000-0000-000000000001']::uuid[] UNION ALL SELECT 2,NULL", &[], None).unwrap_err();
        assert_eq!(error, CoreError::Sql(SqlError::Null));
        assert_eq!(
            take_sql_error_message().as_deref(),
            Some("null value in column \"ids\" of relation \"nn\" violates not-null constraint")
        );
        assert!(rows(run(db, "SELECT * FROM nn")).is_empty());
    });
}

#[test]
fn regex_sets_materialize_in_insert_and_ctas() {
    with_session(|db| {
        let a = "b0000000-0000-0000-0000-000000000001";
        let b = "b0000000-0000-0000-0000-000000000002";
        run(db, "CREATE TABLE src (id int PRIMARY KEY, note text)");
        run(db, &format!("INSERT INTO src VALUES (1,'hi ({a}) and ({b})'), (2,'none')"));
        run(db, "CREATE TABLE dst (id int, mention uuid, PRIMARY KEY (id, mention))");
        assert_eq!(
            run(
                db,
                r"INSERT INTO dst SELECT DISTINCT id, (regexp_matches(note, '\(([0-9a-f-]{36})\)', 'g'))[1]::uuid FROM src ON CONFLICT DO NOTHING",
            ).affected,
            2,
        );
        assert_eq!(
            run(
                db,
                r"INSERT INTO dst SELECT DISTINCT id, m[1]::uuid FROM src CROSS JOIN LATERAL regexp_matches(note, '\(([0-9a-f-]{36})\)', 'g') AS m ON CONFLICT DO NOTHING",
            ).affected,
            0,
        );
        assert_eq!(rows(run(db, "SELECT id, mention::text FROM dst ORDER BY mention")), pairs(&[(1, a), (1, b)]));
        assert_eq!(
            run(db, "CREATE TABLE pieces AS SELECT src.id, piece FROM src CROSS JOIN LATERAL regexp_split_to_table(note, ' ') AS piece WHERE src.id = 2").affected,
            1,
        );
        assert_eq!(rows(run(db, "SELECT id, piece FROM pieces")), pairs(&[(2, "none")]));
    });
}
