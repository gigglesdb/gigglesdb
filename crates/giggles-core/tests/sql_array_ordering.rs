mod common;

use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value, sqlstate};

fn run(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn arrays<const N: usize>(values: [&str; N]) -> Vec<Vec<Value>> {
    values.into_iter().map(|s| vec![text(s)]).collect()
}

fn boolean_row(session: &mut SqlSession, sql: &str, expected: &[bool]) {
    let expected: Vec<_> = expected.iter().copied().map(Value::Bool).collect();
    assert_eq!(run(session, sql), vec![expected], "{sql}");
}

fn refused(session: &mut SqlSession, sql: &str, expected: &str) {
    let error = session.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), expected, "{sql}: {error:?}");
}

#[test]
fn comparisons_use_element_order() {
    let path = common::fresh("array-comparisons");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    boolean_row(
        &mut s,
        "SELECT ARRAY[10] < ARRAY[2], ARRAY[10]::int[] < ARRAY[2]::int[], \
         '{10}'::int[] < '{2}'::int[], '{10}'::int[] > '{9,1}'::int[], '{10}'::int[] = '{10}'::int[], \
         ARRAY[10] <= ARRAY[2], ARRAY[10] >= ARRAY[2], ARRAY[1,2] < ARRAY[1,2,0], ARRAY[1,2,3] > ARRAY[1,3]",
        &[false, false, false, true, true, false, true, true, false],
    );
    boolean_row(
        &mut s,
        "SELECT ARRAY[1,NULL] < ARRAY[1,2], ARRAY[1,NULL] > ARRAY[1,2], \
         ARRAY[NULL]::int[] = ARRAY[NULL]::int[], ARRAY[NULL,1] < ARRAY[NULL,2], \
         '{}'::int[] < ARRAY[1], '{}'::int[] = '{}'::int[]",
        &[false, true, true, true, true, true],
    );
    assert_eq!(
        run(&mut s, "SELECT ARRAY[1] < NULL::int[]"),
        vec![vec![Value::Null]]
    );
    boolean_row(
        &mut s,
        "SELECT ARRAY[-1] < ARRAY[1], ARRAY[-10] < ARRAY[-2], ARRAY[2147483647] > ARRAY[-2147483648], \
         '{1,2}'::int[] = '{1, 2}'::int[], '{1,2}'::int[] <> '{1, 2}'::int[], '{1,2}'::int[] <> '{1,3}'::int[]",
        &[true, true, true, true, false, true],
    );
    boolean_row(
        &mut s,
        "SELECT ARRAY[10]::bigint[] < ARRAY[2]::bigint[], ARRAY[10]::bigint[] > ARRAY[9,1]::bigint[], \
         ARRAY[9000000000]::bigint[] > ARRAY[10]::bigint[]",
        &[false, true, true],
    );
    boolean_row(
        &mut s,
        r#"SELECT ARRAY['b']::text[] < ARRAY['a','z']::text[], ARRAY['a']::text[] < ARRAY['a b']::text[],
            ARRAY['a b']::text[] < ARRAY['a']::text[], ARRAY['a',NULL]::text[] > ARRAY['a','z']::text[],
            ARRAY['B']::text[] < ARRAY['a']::text[], ARRAY['"x']::text[] < ARRAY['a']::text[],
            ARRAY['']::text[] < ARRAY['a']::text[], 'B' < 'a'::text, '"x' < 'a'::text"#,
        &[false, true, false, true, true, true, true, true, true],
    );
    boolean_row(
        &mut s,
        r#"SELECT ARRAY['B']::text[] COLLATE "C" < ARRAY['a']::text[]"#,
        &[true],
    );
    boolean_row(
        &mut s,
        r#"SELECT v < ARRAY['a'] FROM (
               SELECT max(ARRAY['B'] COLLATE "und-x-icu") AS v FROM (VALUES (1)) q(id)
           ) q"#,
        &[false],
    );
    refused(
        &mut s,
        "SELECT ARRAY[10]::int[] < ARRAY[2]::bigint[]",
        "42883",
    );
    refused(
        &mut s,
        "SELECT ARRAY[1.5]::numeric[] < ARRAY[10]::numeric[]",
        "0A000",
    );
    refused(&mut s, "SELECT '{{1,2},{3,4}}'::int[]", "22P02");
    run(&mut s, "CREATE TABLE trc(id int PRIMARY KEY, v text[])");
    run(&mut s, "INSERT INTO trc VALUES (1,'{B}'),(2,'{a}')");
    assert_eq!(
        run(
            &mut s,
            r#"SELECT id FROM trc GROUP BY id ORDER BY max(v COLLATE "und-x-icu")"#
        ),
        vec![vec![Value::Int(2)], vec![Value::Int(1)]]
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT id FROM trc GROUP BY id HAVING max(v COLLATE "und-x-icu") > ARRAY['a']"#
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        run(
            &mut s,
            r#"SELECT v < ARRAY['a'] FROM (
                SELECT first_value(v COLLATE "und-x-icu") OVER (ORDER BY id) AS v FROM trc
            ) q"#,
        ),
        vec![vec![Value::Bool(false)]; 2]
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn int_queries(s: &mut SqlSession) {
    for (sql, ids) in [
        (
            "SELECT id,v FROM ar ORDER BY v,id",
            [5, 7, 2, 8, 6, 3, 1, 4],
        ),
        (
            "SELECT id,v FROM ar ORDER BY v DESC,id",
            [4, 1, 3, 6, 2, 8, 7, 5],
        ),
        (
            "SELECT id,v FROM ar ORDER BY v NULLS FIRST,id",
            [4, 5, 7, 2, 8, 6, 3, 1],
        ),
    ] {
        let expected: Vec<_> = ids
            .into_iter()
            .map(|id| {
                let value = [
                    Some("{10}"),
                    Some("{2}"),
                    Some("{9,1}"),
                    None,
                    Some("{}"),
                    Some("{2,NULL}"),
                    Some("{-5}"),
                    Some("{2}"),
                ];
                vec![
                    Value::Int(id),
                    value[id as usize - 1].map_or(Value::Null, text),
                ]
            })
            .collect();
        assert_eq!(run(s, sql), expected, "{sql}");
    }
    assert_eq!(
        run(s, "SELECT max(v),min(v) FROM ar"),
        vec![vec![text("{10}"), text("{}")]]
    );
    assert_eq!(
        run(s, "SELECT max(v ORDER BY v),min(v ORDER BY v DESC) FROM ar"),
        vec![vec![text("{10}"), text("{}")]]
    );
    let ordered: Vec<_> = ["{}", "{-5}", "{2}", "{2,NULL}", "{9,1}", "{10}"]
        .map(text)
        .into_iter()
        .chain([Value::Null])
        .collect();
    assert_eq!(
        run(s, "SELECT DISTINCT v FROM ar ORDER BY v"),
        ordered.iter().cloned().map(|v| vec![v]).collect::<Vec<_>>()
    );
    assert_eq!(
        run(s, "SELECT v,count(*) FROM ar GROUP BY v ORDER BY v"),
        ordered
            .into_iter()
            .zip([1, 1, 2, 1, 1, 1, 1])
            .map(|(v, n)| vec![v, Value::Int(n)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        run(s, "SELECT v FROM ar WHERE v > '{2}' ORDER BY v"),
        arrays(["{2,NULL}", "{9,1}", "{10}"])
    );
    assert_eq!(
        run(
            s,
            "SELECT v FROM ar WHERE v BETWEEN '{2}' AND '{9,9}' ORDER BY v"
        ),
        arrays(["{2}", "{2}", "{2,NULL}", "{9,1}"])
    );
    assert_eq!(
        run(
            s,
            "SELECT greatest('{10}'::int[], '{2}'::int[]), least('{10}'::int[], '{2}'::int[])"
        ),
        vec![vec![text("{10}"), text("{2}")]]
    );
    let ranks = [7, 3, 6, 8, 1, 5, 2, 3];
    let values = [
        Some("{10}"),
        Some("{2}"),
        Some("{9,1}"),
        None,
        Some("{}"),
        Some("{2,NULL}"),
        Some("{-5}"),
        Some("{2}"),
    ];
    assert_eq!(
        run(
            s,
            "SELECT id,v,rank() OVER (ORDER BY v) FROM ar ORDER BY id"
        ),
        (1..=8)
            .zip(values)
            .zip(ranks)
            .map(|((id, value), rank)| vec![
                Value::Int(id),
                value.map_or(Value::Null, text),
                Value::Int(rank)
            ])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        run(s, "SELECT v FROM ar WHERE v >= '{9}' ORDER BY v"),
        arrays(["{9,1}", "{10}"])
    );
}

fn stored_queries(engine: Engine) {
    let path = common::fresh("stored-array-ordering");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for ddl in [
        "CREATE TABLE ar(id int PRIMARY KEY, v int[])",
        "INSERT INTO ar VALUES (1,'{10}'),(2,'{2}'),(3,'{9,1}'),(4,NULL),(5,'{}'),(6,'{2,NULL}'),(7,'{-5}'),(8,'{ 2 }')",
        "CREATE TABLE br(id int PRIMARY KEY, v bigint[])",
        "INSERT INTO br VALUES (1,'{10}'),(2,'{2}'),(3,'{9,1}')",
        "CREATE TABLE tr(id int PRIMARY KEY, v text[])",
        r#"INSERT INTO tr VALUES (1,'{"a b"}'),(2,'{a}'),(3,'{b}'),(4,'{a,NULL}'),(5,'{""}')"#,
        "CREATE TABLE pr(v int[] PRIMARY KEY)",
        "INSERT INTO pr VALUES ('{10}'),('{2}'),('{9,1}')",
        "CREATE TABLE gd92_src(id int PRIMARY KEY, v int[])",
        "CREATE TABLE gd92_dst(id int PRIMARY KEY, v int[])",
        "INSERT INTO gd92_src VALUES (1,'{10}'),(2,'{2}')",
    ] {
        run(&mut s, ddl);
    }
    refused(&mut s, "CREATE INDEX ar_v ON ar(v)", "0A000");
    let inserted = s
        .execute(
            "INSERT INTO gd92_dst SELECT DISTINCT * FROM gd92_src ORDER BY 2",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(inserted.affected, 2);
    let ordered = vec![
        vec![Value::Int(2), text("{2}")],
        vec![Value::Int(1), text("{10}")],
    ];

    drop(s);
    let mut admin = admin;
    for reopen in [false, true] {
        if reopen {
            drop(admin);
            admin = Admin::open(&path, Limits::default()).unwrap();
        }
        let mut s = admin.sql_session();
        int_queries(&mut s);
        for sql in [
            "SELECT * FROM gd92_dst ORDER BY 2",
            "WITH d AS (SELECT DISTINCT * FROM gd92_src ORDER BY 2) SELECT * FROM d",
            "SELECT * FROM (SELECT DISTINCT * FROM gd92_src ORDER BY 2) d",
        ] {
            assert_eq!(run(&mut s, sql), ordered, "{sql}");
        }
        for (name, ordered, extremes) in [
            (
                "br",
                arrays(["{2}", "{9,1}", "{10}"]),
                vec![text("{10}"), text("{2}")],
            ),
            (
                "tr",
                arrays([r#"{""}"#, "{a}", "{a,NULL}", r#"{"a b"}"#, "{b}"]),
                vec![text("{b}"), text(r#"{""}"#)],
            ),
        ] {
            assert_eq!(
                run(&mut s, &format!("SELECT v FROM {name} ORDER BY v")),
                ordered
            );
            assert_eq!(
                run(&mut s, &format!("SELECT max(v),min(v) FROM {name}")),
                vec![extremes]
            );
        }
        assert_eq!(
            run(&mut s, "SELECT v FROM pr ORDER BY v"),
            arrays(["{2}", "{9,1}", "{10}"])
        );
        assert_eq!(
            run(&mut s, "SELECT DISTINCT * FROM pr ORDER BY v"),
            arrays(["{2}", "{9,1}", "{10}"])
        );
        let prepared = s.prepare("SELECT v FROM pr ORDER BY v").unwrap();
        let mut cursor = s.open_cursor(&prepared, &[]).unwrap();
        let mut paged = Vec::new();
        while !cursor.is_exhausted() {
            paged.extend(cursor.fetch(1, 4096).unwrap().into_iter().map(|r| r.values));
        }
        assert_eq!(paged, arrays(["{2}", "{9,1}", "{10}"]));
        drop(cursor);

        let rows = run(&mut s, "SELECT v FROM pr WHERE v > '{3}'");
        assert_eq!(rows.len(), 2);
        assert!(
            arrays(["{10}", "{9,1}"])
                .iter()
                .all(|row| rows.contains(row))
        );
        assert_eq!(
            run(&mut s, "SELECT max(v),min(v) FROM pr WHERE v > '{3}'"),
            vec![vec![text("{10}"), text("{9,1}")]]
        );
        assert_eq!(
            run(
                &mut s,
                "SELECT v,count(*) FROM pr WHERE v > '{3}' GROUP BY v ORDER BY v"
            ),
            vec![
                vec![text("{9,1}"), Value::Int(1)],
                vec![text("{10}"), Value::Int(1)]
            ]
        );
        assert_eq!(
            run(&mut s, "SELECT v FROM pr WHERE v = '{ 2 }'"),
            arrays(["{2}"])
        );
    }
    let mut s = admin.sql_session();
    assert_eq!(
        run(
            &mut s,
            "UPDATE gd92_dst SET v = d.v FROM (SELECT DISTINCT * FROM gd92_src ORDER BY 2 LIMIT 1) d WHERE gd92_dst.id = 1 RETURNING gd92_dst.id, gd92_dst.v"
        ),
        vec![vec![Value::Int(1), text("{2}")]]
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn stored_arrays_legacy() {
    stored_queries(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn stored_arrays_v2() {
    stored_queries(Engine::V2);
}
