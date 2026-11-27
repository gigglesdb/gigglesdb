use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{SqlResult, StatementClock, Value, take_sql_error_detail, take_sql_error_message},
};
use giggles_pg_types::temporal::SessionZone;

mod common;

const UTC: StatementClock = StatementClock {
    unavailable: None,
    transaction_us: 757_382_400_000_000,
    statement_us: 757_382_400_000_000,
    zone: Some(SessionZone::Fixed(0)),
    wall: None,
};

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute_at(sql, &[], None, &UTC)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn fails(session: &mut SqlSession, sql: &str, code: &str) {
    let error = session.execute_at(sql, &[], None, &UTC).expect_err(sql);
    assert_eq!(giggles_core::sqlstate(&error), code, "{sql}: {error:?}");
}

fn fails_with_detail(session: &mut SqlSession, sql: &str, code: &str, detail: &str) {
    fails(session, sql, code);
    assert_eq!(take_sql_error_detail().as_deref(), Some(detail), "{sql}");
}

fn in_database(engine: Engine, test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("fk-type-pairs");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    test(&mut admin.sql_session());
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

struct Pair {
    parent: &'static str,
    child: &'static str,
    old: &'static str,
    new: &'static str,
    missing: &'static str,
    equal: Option<Image>,
}

#[derive(Clone, Copy)]
struct Image {
    sql: &'static str,
    different: bool,
}

const PAIRS: &[Pair] = &[
    Pair {
        parent: "text",
        child: "char(3)",
        old: "'a'",
        new: "'b'",
        missing: "'z'",
        equal: Some(Image {
            sql: "'a'",
            different: false,
        }),
    },
    Pair {
        parent: "varchar(5)",
        child: "char(3)",
        old: "'a'",
        new: "'b'",
        missing: "'z'",
        equal: None,
    },
    Pair {
        parent: "date",
        child: "timestamp",
        old: "'2020-01-01'",
        new: "'2021-01-01'",
        missing: "'2020-01-01 12:00'",
        equal: None,
    },
    Pair {
        parent: "timestamp",
        child: "date",
        old: "'2020-01-01'",
        new: "'2021-01-01'",
        missing: "'1999-01-01'",
        equal: None,
    },
    Pair {
        parent: "date",
        child: "date",
        old: "'2020-01-01'",
        new: "'2021-01-01'",
        missing: "'1999-01-01'",
        equal: None,
    },
    Pair {
        parent: "timestamp",
        child: "timestamp",
        old: "'2020-01-01'",
        new: "'2021-01-01'",
        missing: "'1999-01-01'",
        equal: None,
    },
    Pair {
        parent: "timestamptz",
        child: "timestamptz",
        old: "'2020-01-01 00:00+00'",
        new: "'2021-01-01 00:00+00'",
        missing: "'1999-01-01 00:00+00'",
        equal: Some(Image {
            sql: "'2020-01-01 01:00+01'",
            different: false,
        }),
    },
    Pair {
        parent: "time",
        child: "time",
        old: "'01:00'",
        new: "'02:00'",
        missing: "'03:00'",
        equal: Some(Image {
            sql: "'01:00:00.000'",
            different: false,
        }),
    },
    Pair {
        parent: "timetz",
        child: "timetz",
        old: "'01:00+00'",
        new: "'02:00+00'",
        missing: "'02:00+01'",
        equal: None,
    },
    Pair {
        parent: "numeric",
        child: "numeric",
        old: "1",
        new: "2",
        missing: "9",
        equal: Some(Image {
            sql: "1.000",
            different: true,
        }),
    },
    Pair {
        parent: "float8",
        child: "float8",
        old: "0",
        new: "2",
        missing: "9",
        equal: Some(Image {
            sql: "'-0'::float8",
            different: true,
        }),
    },
];

#[derive(Clone, Copy, Debug)]
enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
    Deferred,
}

impl ReferentialAction {
    fn clause(self) -> String {
        let action = match self {
            Self::NoAction => "NO ACTION",
            Self::Restrict => "RESTRICT",
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
            Self::SetDefault => "SET DEFAULT",
            Self::Deferred => return "DEFERRABLE INITIALLY DEFERRED".into(),
        };
        format!("ON UPDATE {action} ON DELETE {action}")
    }
}

fn scalar(session: &mut SqlSession, sql: &str) -> Value {
    run(session, sql)
        .rows
        .pop()
        .expect(sql)
        .values
        .pop()
        .expect(sql)
}

fn assert_no_orphans(session: &mut SqlSession, pair: &Pair) {
    let child = if pair.child == "char(3)" {
        "c.r::text"
    } else {
        "c.r"
    };
    assert_eq!(
        scalar(
            session,
            &format!(
                "SELECT count(*) FROM c WHERE r IS NOT NULL AND \
            NOT EXISTS (SELECT 1 FROM p WHERE p.k={child})"
            )
        ),
        Value::Int(0),
        "{} <- {}",
        pair.parent,
        pair.child,
    );
}

fn assert_child(session: &mut SqlSession, pair: &Pair, expected: &str) {
    assert_value_image(session, "SELECT r FROM c WHERE id=1", expected, pair.child);
}

fn assert_value_image(session: &mut SqlSession, sql: &str, expected: &str, ty: &str) {
    let actual = scalar(session, sql);
    let expected = scalar(session, &format!("SELECT CAST({expected} AS {ty})"));
    assert_eq!(actual, expected, "{sql}");
    match (&actual, &expected) {
        (Value::Numeric(actual), Value::Numeric(expected)) => {
            assert_eq!(actual.scale(), expected.scale())
        }
        (Value::Float8(actual), Value::Float8(expected)) => {
            assert_eq!(actual.to_bits(), expected.to_bits())
        }
        _ => {}
    }
}

fn child_dml(session: &mut SqlSession, pair: &Pair, action: ReferentialAction) {
    let Pair { old, missing, .. } = *pair;
    for sql in [
        format!("INSERT INTO c VALUES (2,{missing})"),
        format!("UPDATE c SET r={missing} WHERE id=1"),
        format!("INSERT INTO c VALUES (1,{old}) ON CONFLICT (id) DO UPDATE SET r={missing}"),
    ] {
        if matches!(action, ReferentialAction::Deferred) {
            run(session, "BEGIN");
            run(session, &sql);
            fails(session, "COMMIT", "23503");
        } else {
            fails(session, &sql, "23503");
        }
        assert_child(session, pair, old);
        assert_no_orphans(session, pair);
    }
}

fn parent_dml(
    session: &mut SqlSession,
    pair: &Pair,
    action: ReferentialAction,
    sql: &str,
    new: Option<&str>,
) {
    run(session, "BEGIN");
    match action {
        ReferentialAction::NoAction | ReferentialAction::SetDefault => fails(session, sql, "23503"),
        ReferentialAction::Restrict => fails(session, sql, "23001"),
        ReferentialAction::Deferred => {
            run(session, sql);
            fails(session, "COMMIT", "23503");
            assert_child(session, pair, pair.old);
            assert_no_orphans(session, pair);
            return;
        }
        ReferentialAction::Cascade | ReferentialAction::SetNull => {
            run(session, sql);
            match (action, new) {
                (ReferentialAction::Cascade, None) => {
                    assert_eq!(scalar(session, "SELECT count(*) FROM c"), Value::Int(0))
                }
                (ReferentialAction::Cascade, Some(new)) => assert_child(session, pair, new),
                (ReferentialAction::SetNull, _) => assert_child(session, pair, "NULL"),
                _ => unreachable!(),
            }
            assert_no_orphans(session, pair);
        }
    }
    run(session, "ROLLBACK");
    assert_child(session, pair, pair.old);
    assert_no_orphans(session, pair);
}

fn equal_update(session: &mut SqlSession, pair: &Pair, action: ReferentialAction, image: Image) {
    let sql = format!("UPDATE p SET k={} WHERE k={}", image.sql, pair.old);
    run(session, "BEGIN");
    if image.different && matches!(action, ReferentialAction::Restrict) {
        fails(session, &sql, "23001");
    } else {
        run(session, &sql);
        assert_value_image(session, "SELECT k FROM p", image.sql, pair.parent);
        let expected = match (image.different, action) {
            (true, ReferentialAction::Cascade) => image.sql,
            (true, ReferentialAction::SetNull) => "NULL",
            _ => pair.old,
        };
        assert_child(session, pair, expected);
        assert_no_orphans(session, pair);
    }
    run(session, "ROLLBACK");
}

fn actions(session: &mut SqlSession) {
    for pair in PAIRS {
        let Pair {
            parent,
            child,
            old,
            new,
            equal,
            ..
        } = *pair;
        let parent_key = match parent {
            "date" | "time" | "timetz" | "timestamp" | "timestamptz" => "UNIQUE",
            _ => "PRIMARY KEY",
        };
        for action in [
            ReferentialAction::NoAction,
            ReferentialAction::Restrict,
            ReferentialAction::Cascade,
            ReferentialAction::SetNull,
            ReferentialAction::SetDefault,
            ReferentialAction::Deferred,
        ] {
            run(session, &format!("CREATE TABLE p(k {parent} {parent_key})"));
            run(session, &format!("INSERT INTO p VALUES ({old})"));
            if parent == "timestamp" && child == "date" {
                run(session, "INSERT INTO p VALUES ('1999-01-01 12:00')");
            }
            let reference = format!("REFERENCES p(k) {}", action.clause());
            if matches!(
                action,
                ReferentialAction::Restrict
                    | ReferentialAction::SetDefault
                    | ReferentialAction::Deferred
            ) {
                run(
                    session,
                    &format!("CREATE TABLE c(id int PRIMARY KEY, r {child} DEFAULT {old})"),
                );
                run(session, &format!("INSERT INTO c VALUES (1,{old})"));
                run(
                    session,
                    &format!("ALTER TABLE c ADD FOREIGN KEY (r) {reference}"),
                );
            } else {
                run(
                    session,
                    &format!("CREATE TABLE c(id int PRIMARY KEY, r {child} {reference})"),
                );
                run(session, &format!("INSERT INTO c VALUES (1,{old})"));
            }
            child_dml(session, pair, action);
            if let Some(image) = equal {
                equal_update(session, pair, action, image);
            }
            for (sql, result) in [
                (format!("UPDATE p SET k={new} WHERE k={old}"), Some(new)),
                (
                    format!(
                        "MERGE INTO p USING (SELECT 1) s ON k={old} WHEN MATCHED THEN UPDATE SET k={new}"
                    ),
                    Some(new),
                ),
                (format!("DELETE FROM p WHERE k={old}"), None),
                (
                    format!("MERGE INTO p USING (SELECT 1) s ON k={old} WHEN MATCHED THEN DELETE"),
                    None,
                ),
            ] {
                parent_dml(session, pair, action, &sql, result);
            }
            if matches!(action, ReferentialAction::Deferred) {
                run(session, "BEGIN");
                run(session, &format!("UPDATE p SET k={new} WHERE k={old}"));
                run(session, &format!("UPDATE c SET r={new} WHERE id=1"));
                run(session, "COMMIT");
                assert_child(session, pair, new);
                assert_no_orphans(session, pair);
            }
            if matches!(action, ReferentialAction::SetDefault) {
                run(
                    session,
                    &format!("ALTER TABLE c ALTER COLUMN r SET DEFAULT {new}"),
                );
                run(session, &format!("INSERT INTO p VALUES ({new})"));
                run(session, &format!("DELETE FROM p WHERE k={old}"));
                assert_child(session, pair, new);
                assert_no_orphans(session, pair);
            }
            run(session, "DROP TABLE c");
            run(session, "DROP TABLE p");
        }
    }
}

fn insert_pages(session: &mut SqlSession, insert: &str, row_count: i32) {
    for first in (1..=row_count).step_by(128) {
        let last = (first + 127).min(row_count);
        run(
            session,
            &format!("{insert} FROM generate_series({first},{last}) g"),
        );
    }
}

fn paged_and_lossy(session: &mut SqlSession, row_count: i32) {
    run(session, "CREATE TABLE p(k text COLLATE \"C\" PRIMARY KEY)");
    run(
        session,
        "CREATE TABLE c(id int PRIMARY KEY, r char(3) REFERENCES p ON UPDATE CASCADE ON DELETE RESTRICT)",
    );
    run(session, "INSERT INTO p VALUES ('a'),('a  '),('b'),('q')");
    insert_pages(session, "INSERT INTO c SELECT g, 'q'", row_count);
    run(session, "INSERT INTO c VALUES (2000,'a'),(2001,'b')");
    fails(session, "DELETE FROM p WHERE k='a'", "23001");
    run(session, "DELETE FROM p WHERE k='a  '");
    fails(session, "UPDATE p SET k='c  ' WHERE k='b'", "23503");
    fails(session, "UPDATE p SET k='abcd' WHERE k='b'", "22001");
    run(session, "UPDATE p SET k='c' WHERE k='b'");
    assert_eq!(
        scalar(session, "SELECT count(*) FROM c WHERE r::text='c'"),
        Value::Int(1)
    );
    assert_no_orphans(session, &PAIRS[0]);

    run(session, "DROP TABLE c");
    run(session, "DROP TABLE p");
    run(
        session,
        "CREATE TABLE p(id int PRIMARY KEY, k timestamp UNIQUE)",
    );
    insert_pages(
        session,
        &format!("INSERT INTO p SELECT g, DATE '2022-10-22' + (g::int - {row_count})"),
        row_count,
    );
    run(
        session,
        "CREATE TABLE c(id int PRIMARY KEY, r date REFERENCES p(k) ON UPDATE CASCADE)",
    );
    run(session, "INSERT INTO c VALUES (1,'2022-10-22')");
    fails(
        session,
        &format!("UPDATE p SET k='2030-01-01 06:00' WHERE id={row_count}"),
        "23503",
    );
    assert_child(session, &PAIRS[3], "'2022-10-22'");
    run(
        session,
        &format!("UPDATE p SET k='2030-01-01' WHERE id={row_count}"),
    );
    assert_child(session, &PAIRS[3], "'2030-01-01'");
    assert_no_orphans(session, &PAIRS[3]);
    run(session, "INSERT INTO p VALUES (2000,'infinity')");
    fails(session, "INSERT INTO c VALUES (2,'294277-01-01')", "23503");
    run(session, "INSERT INTO c VALUES (2,'infinity')");
    assert_no_orphans(session, &PAIRS[3]);

    run(session, "CREATE TABLE pd(k date UNIQUE)");
    run(session, "CREATE TABLE pz(k timestamptz UNIQUE)");
    for (parent, child) in [
        ("p", "timestamptz"),
        ("pd", "timestamptz"),
        ("pz", "timestamp"),
        ("pz", "date"),
    ] {
        fails(
            session,
            &format!("CREATE TABLE refused(r {child} REFERENCES {parent}(k))"),
            "42804",
        );
    }
}

#[test]
fn actions_legacy() {
    in_database(Engine::Legacy, actions);
}

#[test]
fn paged_and_lossy_legacy() {
    // Legacy's bounded row materialization is limited to 1,024 rows.
    in_database(Engine::Legacy, |session| paged_and_lossy(session, 450));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn actions_v2() {
    in_database(Engine::V2, actions);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn paged_and_lossy_v2() {
    in_database(Engine::V2, |session| paged_and_lossy(session, 1025));
}

fn review_regressions(session: &mut SqlSession) {
    for sql in [
        "CREATE TABLE p6(k text PRIMARY KEY)",
        "INSERT INTO p6 VALUES ('a')",
        "CREATE TABLE c6(r bpchar REFERENCES p6 ON UPDATE CASCADE)",
        "INSERT INTO c6 VALUES ('a  ')",
        "CREATE TABLE p7(k text PRIMARY KEY)",
        "INSERT INTO p7 VALUES ('x')",
        "CREATE TABLE c7(r char(3) REFERENCES p7 ON UPDATE CASCADE)",
        "INSERT INTO c7 VALUES ('x')",
        "CREATE TABLE p8(k timetz UNIQUE)",
        "INSERT INTO p8 VALUES ('12:00+00')",
        "CREATE TABLE c8(r timetz REFERENCES p8(k) ON UPDATE RESTRICT)",
        "INSERT INTO c8 VALUES ('12:00+00')",
        "CREATE TABLE p9(k timestamp UNIQUE)",
        "INSERT INTO p9 VALUES ('2020-01-01 00:00')",
        "CREATE TABLE c9(r timestamp(0) REFERENCES p9(k) ON UPDATE CASCADE)",
        "INSERT INTO c9 VALUES ('2020-01-01 00:00')",
        "CREATE TABLE p10(a int, d date, UNIQUE (a, d))",
        "INSERT INTO p10 VALUES (1, '2020-01-01')",
        "CREATE TABLE c10(a int, t timestamp, FOREIGN KEY (a, t) REFERENCES p10(a, d) ON DELETE CASCADE)",
        "INSERT INTO c10 VALUES (1, '2020-01-01 00:00')",
    ] {
        run(session, sql);
    }
    for (sql, code, detail) in [
        (
            "UPDATE p6 SET k = 'b  '",
            "23503",
            r#"Key (r)=(b  ) is not present in table "p6"."#,
        ),
        (
            "UPDATE p7 SET k = 'abc  '",
            "23503",
            r#"Key (r)=(abc) is not present in table "p7"."#,
        ),
        (
            "UPDATE p8 SET k = '13:00+01'",
            "23001",
            r#"Key (k)=(12:00:00+00) is referenced from table "c8"."#,
        ),
        (
            "UPDATE p9 SET k = '2020-01-01 00:00:00.6'",
            "23503",
            r#"Key (r)=(2020-01-01 00:00:01) is not present in table "p9"."#,
        ),
        (
            "INSERT INTO c10 VALUES (1, '2020-01-01 01:00')",
            "23503",
            r#"Key (a, t)=(1, 2020-01-01 01:00:00) is not present in table "p10"."#,
        ),
    ] {
        fails_with_detail(session, sql, code, detail);
    }
    assert_eq!(
        scalar(session, "SELECT r::text FROM c6"),
        Value::Text("a".into())
    );
    assert_eq!(
        scalar(session, "SELECT r FROM c6"),
        Value::Text("a  ".into())
    );
    assert_eq!(
        scalar(session, "SELECT r::text FROM c7"),
        Value::Text("x".into())
    );
    assert_eq!(
        scalar(session, "SELECT r = '2020-01-01 00:00' FROM c9"),
        Value::Bool(true)
    );
    run(session, "DELETE FROM p10");
    assert_eq!(scalar(session, "SELECT count(*) FROM c10"), Value::Int(0));
}

fn alter_type_rejects_fk_rewrites(session: &mut SqlSession) {
    for sql in [
        "CREATE TABLE p(k text PRIMARY KEY)",
        "INSERT INTO p VALUES ('a ')",
        "CREATE TABLE c(r varchar(5) REFERENCES p)",
        "INSERT INTO c VALUES ('a ')",
        "CREATE TABLE p2(k text PRIMARY KEY)",
        "INSERT INTO p2 VALUES ('a')",
        "CREATE TABLE c2(r text REFERENCES p2)",
        "INSERT INTO c2 VALUES ('a')",
        "CREATE TABLE p4(k int PRIMARY KEY)",
        "CREATE TABLE c4(r int REFERENCES p4)",
        "CREATE TABLE p5(k timestamp UNIQUE)",
        "INSERT INTO p5 VALUES ('2020-01-01 12:00')",
        "CREATE TABLE c5(r timestamp REFERENCES p5(k))",
        "INSERT INTO c5 VALUES ('2020-01-01 12:00')",
    ] {
        run(session, sql);
    }
    for sql in [
        "ALTER TABLE c ALTER COLUMN r TYPE char(3)",
        "ALTER TABLE c2 ALTER COLUMN r TYPE char(3)",
        "ALTER TABLE c4 ALTER COLUMN r TYPE text",
        "ALTER TABLE c5 ALTER COLUMN r TYPE date",
        // Narrowing can drop trailing spaces and is refused (PostgreSQL
        // re-validates instead): varchar(5) -> varchar(3), text -> varchar(5).
        "ALTER TABLE c ALTER COLUMN r TYPE varchar(3)",
        "ALTER TABLE c2 ALTER COLUMN r TYPE varchar(5)",
    ] {
        fails(session, sql, "0A000");
        assert_eq!(
            take_sql_error_message().as_deref(),
            Some(
                r#"cannot alter type of column "r" because it is used by a foreign key constraint"#
            ),
            "{sql}"
        );
    }
    fails(
        session,
        "ALTER TABLE p2 ALTER COLUMN k TYPE char(3)",
        "0A000",
    );
    fails(session, "ALTER TABLE p2 ALTER COLUMN k TYPE date", "0A000");
    fails(
        session,
        "ALTER TABLE p5 ALTER COLUMN k TYPE char(3)",
        "0A000",
    );
    // Byte-preserving text/varchar retypes on FK columns stay allowed, as in
    // PostgreSQL 18.6, and the FK keeps being enforced afterwards.
    fails(session, "ALTER TABLE p2 ALTER COLUMN k TYPE varchar(5)", "0A000");
    run(session, "ALTER TABLE c ALTER COLUMN r TYPE varchar(8)");
    run(session, "ALTER TABLE c ALTER COLUMN r TYPE text");
    run(session, "ALTER TABLE c ALTER COLUMN r TYPE varchar");
    // Trailing-space truncation on a narrowing retype would orphan the child.
    run(session, "CREATE TABLE pn (k varchar(5) PRIMARY KEY)");
    run(session, "CREATE TABLE cn (k varchar(5) REFERENCES pn)");
    run(session, "INSERT INTO pn VALUES ('ab   ')");
    run(session, "INSERT INTO cn VALUES ('ab   ')");
    fails(session, "ALTER TABLE pn ALTER COLUMN k TYPE varchar(3)", "0A000");
    fails(session, "ALTER TABLE cn ALTER COLUMN k TYPE varchar(3)", "0A000");
    run(session, "ALTER TABLE pn ALTER COLUMN k TYPE varchar(100)");
    run(session, "ALTER TABLE cn ALTER COLUMN k TYPE varchar(255)");
    assert_eq!(scalar(session, "SELECT octet_length(k) FROM cn"), Value::Int(5));
    fails(session, "DELETE FROM pn", "23503");
    // text -> varchar(n) can truncate trailing spaces: refused; unbounded is fine.
    fails(session, "ALTER TABLE c2 ALTER COLUMN r TYPE varchar(100)", "0A000");
    run(session, "ALTER TABLE c2 ALTER COLUMN r TYPE varchar");
    fails(session, "INSERT INTO c2 VALUES ('zz')", "23503");
    fails(session, "UPDATE p2 SET k = 'b'", "23503");
    fails(session, "INSERT INTO c VALUES ('a')", "23503");
    fails(
        session,
        "ALTER TABLE c2 ADD COLUMN extra int, ALTER COLUMN r TYPE char(3)",
        "0A000",
    );
    fails(session, "SELECT extra FROM c2", "42703");
    run(session, "ALTER TABLE c2 ALTER COLUMN r TYPE text");
    assert_eq!(
        scalar(session, "SELECT r::text FROM c"),
        Value::Text("a ".into())
    );
    assert_eq!(scalar(session, "SELECT r FROM c2"), Value::Text("a".into()));
    assert_eq!(
        scalar(session, "SELECT r = '2020-01-01 12:00' FROM c5"),
        Value::Bool(true)
    );
    fails(session, "INSERT INTO c4 VALUES (1)", "23503");
    fails(session, "DELETE FROM p2", "23503");
}

fn review_cases(session: &mut SqlSession) {
    review_regressions(session);
    alter_type_rejects_fk_rewrites(session);
}

#[test]
fn review_cases_legacy() {
    in_database(Engine::Legacy, review_cases);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn review_cases_v2() {
    in_database(Engine::V2, review_cases);
}
