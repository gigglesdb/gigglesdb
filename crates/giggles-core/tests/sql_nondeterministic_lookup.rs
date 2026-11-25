mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{SqlResult, Value, take_sql_error_detail, take_sql_error_message},
    sqlstate,
};

fn each_engine(test: impl Fn(&mut SqlSession)) {
    let run_on = |engine| {
        let admin =
            Admin::create_engine(common::fresh("nondeterministic"), Limits::default(), engine)
                .unwrap();
        let mut session = admin.sql_session();
        run(
            &mut session,
            "CREATE COLLATION ci (provider=icu, locale='und-u-ks-level2', deterministic=false)",
            &[],
        );
        test(&mut session);
    };
    run_on(Engine::Legacy);
    #[cfg(feature = "engine-rocksdb")]
    run_on(Engine::V2);
}

fn run(session: &mut SqlSession, sql: &str, params: &[Value]) -> SqlResult {
    session
        .execute(sql, params, None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}, {:?}", take_sql_error_message()))
}

fn rows(session: &mut SqlSession, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    run(session, sql, params)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn txt(text: &str) -> Value {
    Value::Text(text.to_owned())
}

fn fails(session: &mut SqlSession, sql: &str, state: &str, message: &str, detail: Option<&str>) {
    let error = session.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), state, "{sql}");
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
    assert_eq!(take_sql_error_detail().as_deref(), detail, "{sql}");
}

#[test]
fn predicates_and_conflicts_use_collation_equality() {
    each_engine(|s| {
        run(
            s,
            "CREATE TABLE t (id text COLLATE ci PRIMARY KEY, v int)",
            &[],
        );
        run(s, "INSERT INTO t VALUES ('x',1),('y',2),('Zed',3)", &[]);
        assert_eq!(
            run(s, "UPDATE t SET v=10 WHERE id=$1", &[txt("X")]).affected,
            1
        );
        assert_eq!(
            rows(s, "SELECT v FROM t WHERE id=$1", &[txt("X")]),
            [[Value::Int(10)]]
        );
        let version = run(s, "SELECT v FROM t WHERE id='X'", &[]).rows[0].version;
        assert_eq!(
            s.execute("UPDATE t SET v=10 WHERE id='X'", &[], Some(version))
                .unwrap()
                .affected,
            1,
        );
        let stale = s
            .execute("DELETE FROM t WHERE id='X'", &[], Some(version))
            .unwrap_err();
        assert_eq!(sqlstate(&stale), "40001");
        assert_eq!(
            rows(
                s,
                "SELECT id, count(*) FROM t WHERE id=$1 GROUP BY id",
                &[txt("X")]
            ),
            [[txt("x"), Value::Int(1)]],
        );
        assert_eq!(
            rows(s, "SELECT id FROM t WHERE id >= 'y' ORDER BY id", &[]),
            [[txt("y")], [txt("Zed")]]
        );
        assert_eq!(
            run(s, "UPDATE t SET v=v+1 WHERE id IN ('X','zed')", &[]).affected,
            2
        );
        assert_eq!(
            rows(s, "DELETE FROM t WHERE id=$1 RETURNING id", &[txt("Y")]),
            [[txt("y")]]
        );
        fails(
            s,
            "INSERT INTO t VALUES ('X',2)",
            "23505",
            "duplicate key value violates unique constraint \"t_pkey\"",
            Some("Key (id)=(X) already exists."),
        );
        fails(
            s,
            "INSERT INTO t VALUES ('q',1),('Q',2)",
            "23505",
            "duplicate key value violates unique constraint \"t_pkey\"",
            Some("Key (id)=(Q) already exists."),
        );
        fails(
            s,
            "UPDATE t SET id='zED' WHERE id='X'",
            "23505",
            "duplicate key value violates unique constraint \"t_pkey\"",
            Some("Key (id)=(zED) already exists."),
        );
        assert_eq!(
            run(
                s,
                "INSERT INTO t VALUES ('X',7) ON CONFLICT (id) DO UPDATE SET v=excluded.v",
                &[]
            )
            .affected,
            1
        );

        run(
            s,
            "CREATE TABLE src (id int PRIMARY KEY, k text COLLATE ci UNIQUE)",
            &[],
        );
        run(s, "INSERT INTO src VALUES (1,'X'),(2,'zed')", &[]);
        assert_eq!(
            rows(
                s,
                "SELECT t.id FROM src JOIN t ON src.k=t.id ORDER BY src.id",
                &[]
            ),
            [[txt("x")], [txt("Zed")]]
        );
        assert_eq!(
            run(
                s,
                "MERGE INTO t USING src ON t.id=src.k WHEN MATCHED THEN UPDATE SET v=src.id",
                &[]
            )
            .affected,
            2
        );
        assert_eq!(
            rows(s, "SELECT id FROM src WHERE k='ZeD'", &[]),
            [[Value::Int(2)]]
        );
        assert_eq!(run(s, "DELETE FROM src WHERE k='ZED'", &[]).affected, 1);
    });
}

#[test]
fn foreign_key_actions_and_parent_diagnostics_use_collation() {
    each_engine(|s| {
        run(s, "CREATE TABLE p (id text COLLATE ci PRIMARY KEY)", &[]);
        run(
            s,
            "CREATE TABLE c (cid int PRIMARY KEY, pid text COLLATE ci REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
            &[],
        );
        run(s, "INSERT INTO p VALUES ('abc'),('def')", &[]);
        run(s, "INSERT INTO c VALUES (1,'ABC'),(2,'DEF')", &[]);
        assert_eq!(run(s, "DELETE FROM p WHERE id='ABC'", &[]).affected, 1);
        assert_eq!(
            run(s, "UPDATE p SET id='ghi' WHERE id='DEF'", &[]).affected,
            1
        );
        assert_eq!(
            rows(s, "SELECT * FROM c", &[]),
            [[Value::Int(2), txt("ghi")]]
        );

        run(s, "CREATE TABLE p2 (id text COLLATE ci PRIMARY KEY)", &[]);
        run(
            s,
            "CREATE TABLE c2 (cid int PRIMARY KEY, pid text COLLATE ci REFERENCES p2(id))",
            &[],
        );
        run(s, "INSERT INTO p2 VALUES ('abc')", &[]);
        run(s, "INSERT INTO c2 VALUES (1,'ABC')", &[]);
        for sql in [
            "DELETE FROM p2 WHERE id='abc'",
            "UPDATE p2 SET id='zzz' WHERE id='ABC'",
        ] {
            fails(
                s,
                sql,
                "23503",
                "update or delete on table \"p2\" violates foreign key constraint \"c2_pid_fkey\" on table \"c2\"",
                Some("Key (id)=(abc) is still referenced from table \"c2\"."),
            );
        }
        assert_eq!(
            rows(s, "SELECT id FROM p2 WHERE id='ABC'", &[]),
            [[txt("abc")]]
        );

        run(
            s,
            "CREATE TABLE c3 (cid int PRIMARY KEY, pid text COLLATE ci REFERENCES p2(id) DEFERRABLE INITIALLY DEFERRED)",
            &[],
        );
        run(s, "DELETE FROM c2", &[]);
        run(s, "INSERT INTO c3 VALUES (1,'ABC')", &[]);
        run(s, "BEGIN", &[]);
        assert_eq!(run(s, "DELETE FROM p2 WHERE id='ABC'", &[]).affected, 1);
        fails(
            s,
            "COMMIT",
            "23503",
            "update or delete on table \"p2\" violates foreign key constraint \"c3_pid_fkey\" on table \"c3\"",
            Some("Key (id)=(abc) is still referenced from table \"c3\"."),
        );
    });
}

#[test]
fn like_and_substring_functions_use_the_resolved_collation() {
    each_engine(|s| {
        run(
            s,
            "CREATE COLLATION ai (provider=icu,locale='und-u-ks-level1',deterministic=false)",
            &[],
        );
        run(
            s,
            "CREATE TABLE t (id text COLLATE ci PRIMARY KEY, s text COLLATE ci, d text)",
            &[],
        );
        run(
            s,
            "INSERT INTO t VALUES ('alice','Hello World','Hello')",
            &[],
        );
        assert!(rows(s, r#"SELECT id FROM t WHERE id LIKE 'A%' COLLATE "C""#, &[]).is_empty());
        assert_eq!(
            rows(s, "SELECT id FROM t WHERE d LIKE 'hel%' COLLATE ci", &[]),
            [[txt("alice")]]
        );
        assert_eq!(
            rows(
                s,
                r"SELECT 'a%b' LIKE 'A\%B' COLLATE ci, 'a_b' LIKE 'A#_B' ESCAPE '#' COLLATE ci,
            'ß' LIKE 'ss' COLLATE ai, 'ss' LIKE '_' COLLATE ai, 'abc' NOT LIKE 'A_C' COLLATE ci,
            'abc' ~~ 'A%' COLLATE ci, 'abc' !~~ 'A%' COLLATE ci",
                &[]
            ),
            [[true, true, true, false, false, true, false].map(Value::Bool)]
        );
        for sql in [
            "SELECT 'abc' ILIKE 'A%' COLLATE ci",
            "SELECT 'abc' NOT ILIKE 'A%' COLLATE ci",
        ] {
            fails(
                s,
                sql,
                "0A000",
                "nondeterministic collations are not supported for ILIKE",
                None,
            );
        }
        assert_eq!(
            rows(
                s,
                "SELECT strpos(s,'WORLD'), position('LL' IN s), replace(s,'L','x'), split_part(s,' W',2) FROM t",
                &[]
            ),
            [[
                Value::Int(7),
                Value::Int(3),
                txt("Hexxo Worxd"),
                txt("orld")
            ]]
        );
        assert_eq!(rows(s, "SELECT strpos('hello' COLLATE ci,'L'), split_part('aXbxc' COLLATE ci,'x',2),
            split_part('aXbxc' COLLATE ci,'x',-2), replace('aXbxc' COLLATE ci,'x','-'), replace('aßb' COLLATE ai,'ss','-')", &[]),
            [[Value::Int(3), txt("b"), txt("b"), txt("a-b-c"), txt("a-b")]]);
        fails(
            s,
            "SELECT split_part('a' COLLATE ci, ',', 0)",
            "22023",
            "field position must not be zero",
            None,
        );
    });
}
