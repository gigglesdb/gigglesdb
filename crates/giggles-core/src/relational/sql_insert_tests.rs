use super::*;
use crate::{Admin, CoreError, Limits, SqlSession};
use std::path::PathBuf;

fn fixture() -> (Admin, SqlSession, PathBuf) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "gd42-sql-insert-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    run(
        &mut session,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, email TEXT, code TEXT, n INTEGER NOT NULL)",
    );
    (admin, session, path)
}
fn run(s: &mut SqlSession, text: &str) -> SqlResult {
    s.execute(text, &[], None).unwrap()
}
fn rows(s: &mut SqlSession) -> Vec<SqlRow> {
    run(s, "SELECT * FROM items ORDER BY id LIMIT 1024").rows
}
fn fail(s: &mut SqlSession, text: &str, error: SqlError) {
    assert_eq!(
        s.execute(text, &[], None),
        Err(CoreError::Sql(error)),
        "{text}"
    );
}
fn indexes(s: &mut SqlSession) {
    run(s, "CREATE UNIQUE INDEX email_unique ON items (email)");
    run(s, "CREATE UNIQUE INDEX code_unique ON items (code)");
}
fn snapshot(s: &mut SqlSession) -> Relational {
    let own = !s.in_transaction();
    if own {
        run(s, "BEGIN");
    }
    let fence = s.batch_fence_engine().unwrap();
    let (state, _) = s
        .apply_batch_engine(fence, &mut || false, |src, apply| {
            apply.apply(&[], src.next_version_engine())?;
            Ok(src.clone())
        })
        .unwrap();
    if own {
        run(s, "ROLLBACK");
    }
    state
}
fn parsed(text: &str) -> Prepared {
    let p = Prepared::parse(text, 1, 0).unwrap();
    assert!(matches!(p.statement.get(), Statement::InsertMulti(_)), "{text}");
    assert_eq!(p.table(), Some("items"));
    assert_eq!(p.command(), "INSERT");
    assert!(!p.is_ddl());
    assert!(!p.is_read());
    p
}

#[test]
fn implicit_sequences_keep_distinct_names_in_first_use_order() {
    let mut state = Relational::default();
    for sql in [
        "CREATE SEQUENCE distinct_seq",
        "CREATE SEQUENCE shared_seq",
        "CREATE SEQUENCE conflict_seq",
        "CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            distinct_value BIGINT DEFAULT nextval('distinct_seq'),
            shared_left BIGINT DEFAULT nextval('shared_seq'),
            shared_right BIGINT DEFAULT nextval('shared_seq'),
            conflict_value BIGINT DEFAULT nextval('conflict_seq')
        )",
    ] {
        let statement = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&statement, &[], None).unwrap();
    }

    let multi = parsed(
        "INSERT INTO items VALUES
         (1, 10, DEFAULT, DEFAULT, 20),
         (2, DEFAULT, 30, DEFAULT, 40),
         (3, DEFAULT, DEFAULT, 50, 60)
         ON CONFLICT (id) DO UPDATE SET conflict_value = DEFAULT, shared_left = DEFAULT",
    );
    assert_eq!(
        multi.implicit_sequences(&state).unwrap(),
        [
            Some("shared_seq".into()),
            Some("distinct_seq".into()),
            Some("conflict_seq".into()),
        ],
    );

    let single = Prepared::parse(
        "INSERT INTO items (id, distinct_value, conflict_value) VALUES (1, 10, 20)",
        1,
        state.epoch,
    )
    .unwrap();
    assert!(matches!(single.statement.get(), Statement::Insert { .. }));
    assert_eq!(
        single.implicit_sequences(&state).unwrap(),
        [Some("shared_seq".into())],
    );
}

#[test]
fn implicit_sequences_reports_the_first_default_error_in_iteration_order() {
    let mut state = Relational::default();
    let create = Prepared::parse(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
        1,
        state.epoch,
    )
    .unwrap();
    state.execute(&create, &[], None).unwrap();
    let a = state.tables["items"].columns[1].id;
    let b = state.tables["items"].columns[2].id;
    state.set_column_default(a, Some("1 2".into())).unwrap();
    state
        .set_column_default(b, Some("'unterminated".into()))
        .unwrap();

    for (sql, expected) in [
        (
            "INSERT INTO items VALUES (1, 10, DEFAULT), (2, DEFAULT, 20)",
            Err(SqlError::Syntax),
        ),
        (
            "INSERT INTO items VALUES (1, DEFAULT, DEFAULT), (2, 10, 20)",
            Err(SqlError::Unsupported),
        ),
        (
            "INSERT INTO items VALUES (1, 10, 20), (2, 30, 40)
             ON CONFLICT (id) DO UPDATE SET b = DEFAULT, a = DEFAULT",
            Err(SqlError::Syntax),
        ),
        (
            "INSERT INTO items VALUES (1, 10, 20), (2, 30, 40)",
            Ok(vec![]),
        ),
    ] {
        assert_eq!(parsed(sql).implicit_sequences(&state), expected, "{sql}");
    }
}

#[test]
fn sql_insert_claim_rule_preserves_single_row_paths() {
    for text in [
        "INSERT INTO items VALUES (1,'a','x',10)",
        "INSERT INTO items (id,n) VALUES ($1,$2)",
    ] {
        assert!(!claims(&lex(text).unwrap()));
        assert!(matches!(
            Prepared::parse(text, 1, 0).unwrap().statement.get(),
            Statement::Insert { .. }
        ));
    }
    let text = "INSERT INTO values (id) VALUES (1)";
    assert!(!claims(&lex(text).unwrap()));
    assert!(matches!(
        Prepared::parse(text, 1, 0).unwrap().statement.get(),
        Statement::Insert { .. }
    ));
    let text = "INSERT INTO items VALUES (1) garbage (2)";
    assert!(!claims(&lex(text).unwrap()));
    assert_eq!(
        Prepared::parse(text, 1, 0).unwrap_err(),
        SqlError::Unsupported
    );
    let text = "INSERT INTO items (id,n) VALUES (1,abs(-10))";
    assert!(!claims(&lex(text).unwrap()));
    assert!(matches!(
        Prepared::parse(text, 1, 0).unwrap().statement.get(),
        Statement::InsertExpressions(_)
    ));
    for text in [
        "INSERT INTO items VALUES (1,'a','x',10),(2,'b','y',20)",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT DO NOTHING",
        "INSERT INTO items VALUES (1,'on conflict','x',10),(2,'b','y',20)",
    ] {
        assert!(claims(&lex(text).unwrap()));
        parsed(text);
    }
    assert!(!claims(
        &lex("INSERT INTO items VALUES (1,'on conflict','x',10)").unwrap()
    ));
}

#[test]
fn sql_insert_single_row_trailing_garbage_error_unchanged() {
    let text = "INSERT INTO items (id,n) VALUES (1,10) garbage";
    assert!(!claims(&lex(text).unwrap()));
    assert_eq!(
        Prepared::parse(text, 1, 0).unwrap_err(),
        SqlError::Unsupported
    );
}

#[test]
fn sql_insert_grammar_accept_matrix() {
    for text in [
        "INSERT INTO items VALUES (1,'a','x',10),(2,'b','y',20) RETURNING id",
        "INSERT INTO items VALUES (1,'a',NULL,10),(2,'b','z',-20);",
        "insert into ITEMS (ID,n) values ($1,$2),($3,$4)",
        "INSERT INTO \"items\" (\"id\",\"n\") VALUES (1,10),(2,20)",
        "INSERT INTO items VALUES (1,1.25,TRUE,FALSE),(2,-1.25,NULL,$1)",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT (id) DO NOTHING",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT ON CONSTRAINT items_pkey DO NOTHING",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n,email=items.email,code=items.code WHERE NOT (items.n < $1 OR EXCLUDED.email IS NULL) AND items.code IS NOT NULL",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT (id) DO UPDATE SET n=$1 WHERE items.n=10 OR items.n<>20 AND items.n<=30 AND items.n>=0 AND items.n>0",
    ] {
        parsed(text);
    }
}

#[test]
fn sql_insert_default_and_overriding_grammar_accept_matrix() {
    for text in [
        "INSERT INTO items VALUES (DEFAULT,'a','x',10),(2,'b','y',20)",
        "INSERT INTO items DEFAULT VALUES ON CONFLICT DO NOTHING",
        "INSERT INTO items OVERRIDING SYSTEM VALUE VALUES (1,'a','x',10),(2,'b','y',20)",
    ] {
        parsed(text);
    }
}

#[test]
fn sql_insert_quoted_names_preserve_case() {
    let p = Prepared::parse(r#"INSERT INTO "Items" ("Id",n) VALUES (1,10),(2,20)"#, 1, 0).unwrap();
    assert_eq!(p.table(), Some("Items"));
    assert!(matches!(p.statement.get(), Statement::InsertMulti(_)));
}
#[test]
fn sql_insert_select_on_conflict_do_nothing() {
    let text = "INSERT INTO items SELECT 1,'a','x',10 ON CONFLICT DO NOTHING";
    parsed(text);
    let (_, mut s, _) = fixture();
    assert_eq!(run(&mut s, text).affected, 1);
    let got = rows(&mut s);
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].values,
        vec![
            Value::Int(1),
            Value::Text("a".into()),
            Value::Text("x".into()),
            Value::Int(10),
        ]
    );
    assert_eq!(run(&mut s, text).affected, 0);
    assert_eq!(rows(&mut s), got);
}

#[test]
fn sql_insert_grammar_unsupported_matrix() {
    // PG18.6 accepts the INSERT alias and expressions over target/EXCLUDED columns in DO UPDATE
    // SET/WHERE (e1-sql/evidence/oracle-pg186-upsert-expressions.txt), so these now parse.
    for text in [
        "INSERT INTO items VALUES (abs(1),'a','x',10),(2,'b','y',20)",
        "INSERT INTO items VALUES (1+1,'a','x',10),(2,'b','y',20)",
        "INSERT INTO items AS t VALUES (1,'a','x',10),(2,'b','y',20)",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT (id) DO UPDATE SET n=abs(items.n)",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT (id) DO UPDATE SET n=items.n+1",
        "INSERT INTO items VALUES (1,'a','x',10) ON CONFLICT (id) DO UPDATE SET n=10 WHERE items.n != 0",
    ] {
        assert!(Prepared::parse(text, 1, 0).is_ok(), "{text}");
    }
}

#[test]
fn sql_insert_grammar_syntax_matrix() {
    for text in [
        "INSERT INTO items VALUES (1,10),(2,20",
        "INSERT INTO items VALUES (1,10),(2,20),",
        "INSERT INTO items VALUES (1,10),(2,20);;",
        "INSERT INTO items VALUES (1,10) ON CONFLICT (id) DO",
        "INSERT INTO items VALUES (1,10) ON CONFLICT (id) DO UPDATE SET n=",
        "INSERT INTO items VALUES (1,10) ON CONFLICT (id) DO UPDATE SET n=10 WHERE (n=1",
        "INSERT INTO items VALUES (1,10) ON CONFLICT DO UPDATE SET n=10",
    ] {
        assert_eq!(
            Prepared::parse(text, 1, 0).unwrap_err(),
            SqlError::Syntax,
            "{text}"
        );
    }
}

#[test]
fn sql_insert_plain_three_rows_versions_in_actual_key_order() {
    let (_, mut s, _) = fixture();
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO items VALUES (3,'c','z',30),(1,'a','x',10),(2,'b','y',20)"
        )
        .affected,
        3
    );
    let got = rows(&mut s);
    assert_eq!(got.len(), 3);
    for (i, row) in got.iter().enumerate() {
        assert_eq!(row.values[0], Value::Int(i as i64 + 1));
        assert_eq!(row.values[3], Value::Int((i as i64 + 1) * 10));
    }
    assert_eq!(got[1].version, got[0].version + 1);
    assert_eq!(got[2].version, got[1].version + 1);
}

#[test]
fn sql_insert_plain_primary_duplicates_are_atomic() {
    let (_, mut s, _) = fixture();
    fail(
        &mut s,
        "INSERT INTO items VALUES (1,'a','x',10),(1,'b','y',20)",
        SqlError::Duplicate,
    );
    assert!(rows(&mut s).is_empty());
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    let before = rows(&mut s);
    let version = snapshot(&mut s).next_version_engine();
    fail(
        &mut s,
        "INSERT INTO items VALUES (2,'b','y',20),(1,'c','z',30)",
        SqlError::Duplicate,
    );
    assert_eq!(rows(&mut s), before);
    assert_eq!(snapshot(&mut s).next_version_engine(), version);
}

#[test]
fn sql_insert_plain_unique_violation_is_atomic() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    indexes(&mut s);
    let before = rows(&mut s);
    for text in [
        "INSERT INTO items VALUES (2,'b','y',20),(3,'a','z',30)",
        "INSERT INTO items VALUES (2,'b','y',20),(3,'b','z',30)",
        "INSERT INTO items VALUES (2,'b','y',20),(3,'c','x',30)",
    ] {
        fail(&mut s, text, SqlError::Duplicate);
        assert_eq!(rows(&mut s), before);
    }
    snapshot(&mut s)
        .secondary_validate_engine(|| false)
        .unwrap();
}

#[test]
fn sql_insert_null_fill_not_null_arity_and_type_match_existing_insert() {
    let (_, mut s, _) = fixture();
    let old = s
        .execute("INSERT INTO items (id,n) VALUES (1,NULL)", &[], None)
        .unwrap_err();
    assert_eq!(
        s.execute("INSERT INTO items (id,n) VALUES (1,10),(2,NULL)", &[], None)
            .unwrap_err(),
        old
    );
    let old = s
        .execute("INSERT INTO items (id,n) VALUES (NULL,10)", &[], None)
        .unwrap_err();
    assert_eq!(
        s.execute(
            "INSERT INTO items (id,n) VALUES (1,10),(NULL,20)",
            &[],
            None
        )
        .unwrap_err(),
        old
    );
    assert!(rows(&mut s).is_empty());
    fail(
        &mut s,
        "INSERT INTO items (id,n) VALUES (1,10),(2)",
        SqlError::Syntax,
    );
    fail(
        &mut s,
        "INSERT INTO items (id,n) VALUES (1,10),(2,'wrong')",
        SqlError::InvalidTextRepresentation,
    );
    assert!(rows(&mut s).is_empty());
    assert_eq!(
        run(&mut s, "INSERT INTO items (n,id) VALUES (10,1),(20,2)").affected,
        2
    );
    for row in rows(&mut s) {
        assert_eq!(row.values[1], Value::Null);
        assert_eq!(row.values[2], Value::Null);
    }
}

#[test]
fn repeated_defaults_evaluate_in_values_order() {
    let (_, mut session, _) = fixture();
    run(
        &mut session,
        "CREATE TABLE defaults_batch (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            label TEXT DEFAULT 'stored', stamp TIMESTAMPTZ DEFAULT statement_timestamp()
        )",
    );
    let clock = clock::ExecutionClock::pinned(100, None);
    let result = session.execute_at(
        "INSERT INTO defaults_batch (id, label)
         VALUES (DEFAULT, 'explicit'), (DEFAULT, DEFAULT), (DEFAULT, DEFAULT)
         RETURNING id, label, stamp",
        &[],
        None,
        &clock,
    ).unwrap();
    let stamp = Value::TimestampTz(giggles_pg_types::temporal::TimestampTz::from_microseconds(100).unwrap());
    assert_eq!(result.rows.len(), 3);
    for (row, (id, label)) in result.rows.iter().zip([(1, "explicit"), (2, "stored"), (3, "stored")]) {
        assert_eq!(row.values[..2], [Value::Int(id), Value::Text(label.into())]);
        assert_eq!(row.values[2], stamp);
    }
}

#[test]
fn repeated_defaults_keep_draws_before_failed_assignment() {
    let (_, mut session, _) = fixture();
    run(&mut session, "CREATE TABLE defaults_batch (id BIGSERIAL PRIMARY KEY, n INTEGER)");
    let clock = clock::ExecutionClock::pinned(100, None);
    let expected = session.execute(
        "INSERT INTO items (id,n) VALUES (1,10), (2,'bad'), (3,30)", &[], None,
    ).unwrap_err();
    assert_eq!(crate::sqlstate(&expected), "22P02");
    let expected_message = take_sql_error_message();

    for (start, restart) in [(1, false), (40, true)] {
        if restart {
            run(&mut session, "ALTER SEQUENCE defaults_batch_id_seq RESTART WITH 40");
        }
        run(&mut session, "BEGIN");
        let error = session.execute_at(
            "INSERT INTO defaults_batch (n) VALUES (10), ('bad'), (30)", &[], None, &clock,
        ).unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(error.to_string(), expected.to_string());
        assert_eq!(take_sql_error_message(), expected_message);
        run(&mut session, "ROLLBACK");
        assert!(run(&mut session, "SELECT id FROM defaults_batch LIMIT 3").rows.is_empty());

        let result = session.execute_at(
            "INSERT INTO defaults_batch (n) VALUES (10), (20) RETURNING id", &[], None, &clock,
        ).unwrap();
        assert_eq!(result.rows[0].values, [Value::Int(start + 2)]);
        assert_eq!(result.rows[1].values, [Value::Int(start + 3)]);
        run(&mut session, "DELETE FROM defaults_batch WHERE id > 0");
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn prepared_paged_serial_insert_uses_current_defaults_and_sequence() {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "paged-serial-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), crate::Engine::V2).unwrap();
    let mut session = admin.sql_session();
    run(
        &mut session,
        "CREATE TABLE defaults_batch (
            id BIGSERIAL PRIMARY KEY, label TEXT DEFAULT 'original', n INTEGER NOT NULL
        )",
    );

    let count = 202; // The V2 VALUES page holds 200 rows.
    let tuples = (1..=count)
        .map(|n| format!("({n})"))
        .collect::<Vec<_>>()
        .join(",");
    let prepared = session
        .prepare(&format!(
            "INSERT INTO defaults_batch (n) VALUES {tuples} RETURNING id, label, n"
        ))
        .unwrap();
    assert!(matches!(
        prepared.statement.get(), Statement::InsertMulti(insert) if insert.needs_pages()
    ));
    let clock = clock::ExecutionClock::pinned(100, None);
    let check = |session: &mut SqlSession, start, label: &str| {
        let result = session
            .execute_prepared_at(&prepared, &[], None, &clock)
            .unwrap();
        assert_eq!(result.affected, count as u64);
        assert_eq!(result.rows.len(), count);
        for (offset, row) in result.rows.iter().enumerate() {
            let offset = i64::try_from(offset).unwrap();
            assert_eq!(
                row.values,
                [Value::Int(start + offset), Value::Text(label.into()), Value::Int(offset + 1)]
            );
        }
    };
    check(&mut session, 1, "original");
    run(
        &mut session,
        "ALTER TABLE defaults_batch ALTER COLUMN label SET DEFAULT 'changed'",
    );
    run(
        &mut session,
        "ALTER SEQUENCE defaults_batch_id_seq RESTART WITH 1000",
    );
    check(&mut session, 1000, "changed");

    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn repeated_volatile_defaults_evaluate_each_row() {
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };

    let (admin, mut session, path) = fixture();
    run(
        &mut session,
        "CREATE TABLE defaults_batch (
            id INTEGER PRIMARY KEY, stamp TIMESTAMPTZ DEFAULT clock_timestamp(),
            uid UUID DEFAULT gen_random_uuid()
        )",
    );
    let mut clock = clock::ExecutionClock::pinned(100, None);
    let wall = AtomicI64::new(101);
    clock.wall = Some(Arc::new(move || Ok(wall.fetch_add(1, Ordering::Relaxed))));
    let result = session
        .execute_at(
            "INSERT INTO defaults_batch (id) VALUES (9), (1), (5) RETURNING id, stamp, uid",
            &[],
            None,
            &clock,
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    let mut uuids = std::collections::BTreeSet::new();
    for (row, (id, us)) in result.rows.iter().zip([(9, 101), (1, 102), (5, 103)]) {
        let stamp = giggles_pg_types::temporal::TimestampTz::from_microseconds(us).unwrap();
        assert_eq!(row.values[..2], [Value::Int(id), Value::TimestampTz(stamp)]);
        let Value::Text(uuid) = &row.values[2] else {
            panic!("expected a UUID, got {:?}", row.values[2]);
        };
        assert!(uuids.insert(uuid), "reused UUID {uuid}");
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn single_values_append_clears_order_after_staging_error() {
    let mut state = Relational::default();
    let create = Prepared::parse("CREATE TABLE items (id INTEGER PRIMARY KEY)", 1, state.epoch)
        .unwrap();
    state.execute(&create, &[], None).unwrap();
    let insert = Prepared::parse("INSERT INTO items VALUES (1) RETURNING id", 1, state.epoch)
        .unwrap();
    assert!(matches!(insert.statement.get(), Statement::Insert { .. }));
    let description = StatementDescription::new(&insert);
    let mut events = Vec::new();
    let mut order = Some(row_sink::AppendOrder::RejectedCandidates(Vec::new()));

    // An index-free empty preimage makes its first cancellation check in append staging.
    let error = state.execute_statement_described_with_append(
        &description, &[], None, || true, None, None, &mut events, Some(&mut order), None,
    ).unwrap_err();
    assert_eq!(error, SqlError::Aborted);
    assert!(order.is_none());
    assert!(state.rows[&state.tables["items"].id].is_empty());

    let result = state.execute_statement_described_with_append(
        &description, &[], None, || false, None, None, &mut events, Some(&mut order), None,
    ).unwrap();
    assert_eq!(result.affected, 1);
    assert_eq!(result.rows[0].values, [Value::Int(1)]);
    assert!(matches!(
        order, Some(row_sink::AppendOrder::Published(row_sink::AppendReplay::SortedOrdinals(ordinals)))
            if ordinals == [0]
    ));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_single_values_append_succeeds_after_unique_error_in_same_session() {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "single-append-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), crate::Engine::V2).unwrap();
    let mut session = admin.sql_session();
    run(&mut session, "CREATE TABLE items (id INTEGER PRIMARY KEY, email TEXT UNIQUE)");
    run(&mut session, "INSERT INTO items VALUES (1, 'taken')");
    let error = session
        .execute("INSERT INTO items VALUES (2, 'taken')", &[], None)
        .unwrap_err();
    assert_eq!(crate::sqlstate(&error), "23505");
    let result = run(
        &mut session,
        "INSERT INTO items VALUES (2, 'free') RETURNING id, email",
    );
    assert_eq!(result.affected, 1);
    assert_eq!(result.rows[0].values, [Value::Int(2), Value::Text("free".into())]);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sql_insert_primary_do_nothing_affected_counts() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    let before = rows(&mut s)[0].clone();
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO items VALUES (1,'b','y',20),(2,'c','z',30) ON CONFLICT (id) DO NOTHING"
        )
        .affected,
        1
    );
    assert_eq!(rows(&mut s)[0], before);
    assert_eq!(rows(&mut s).len(), 2);
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO items VALUES (1,'b','y',20) ON CONFLICT (id) DO NOTHING"
        )
        .affected,
        0
    );
}

#[test]
fn sql_insert_conflict_unchanged_update_returns_accepted_postimage() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    let before = rows(&mut s)[0].clone();
    let result = run(
        &mut s,
        "INSERT INTO items VALUES (1,'b','y',99) ON CONFLICT (id) DO UPDATE SET n=items.n RETURNING *",
    );
    assert_eq!(result.affected, 1);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values, before.values);
    let stored = rows(&mut s);
    assert_eq!(result.rows, stored);
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].values, before.values);
    assert!(stored[0].version > before.version);
}

#[test]
fn sql_insert_primary_update_excluded_existing_and_parameter() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (1,'b','y',20),(2,'c','z',30) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n").affected,2);
    assert_eq!(rows(&mut s)[0].values[3], Value::Int(20));
    assert_eq!(rows(&mut s)[0].values[1], Value::Text("a".into()));
    let before = rows(&mut s)[0].clone();
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO items VALUES (1,'b','y',99) ON CONFLICT (id) DO UPDATE SET n=items.n"
        )
        .affected,
        1
    );
    let after = rows(&mut s)[0].clone();
    assert_eq!(after.values, before.values);
    assert!(after.version > before.version);
    let p = s
        .prepare("INSERT INTO items (id,n) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET n=$3")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[Value::Int(1), Value::Int(90), Value::Int(40)], None)
            .unwrap()
            .affected,
        1
    );
    assert_eq!(rows(&mut s)[0].values[3], Value::Int(40));
    assert_eq!(run(
        &mut s,
        "INSERT INTO items (id,n) VALUES (1,1) ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id",
    ).affected,1);
    assert_eq!(rows(&mut s)[0].values[0],Value::Int(1));
    fail(
        &mut s,
        "INSERT INTO items (id,n) VALUES (1,1) ON CONFLICT (id) DO UPDATE SET n='wrong'",
        SqlError::InvalidTextRepresentation,
    );
}

#[test]
fn sql_insert_where_true_false_and_null_boolean_semantics() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    let before = rows(&mut s);
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (1,'b','y',20) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n WHERE items.n>EXCLUDED.n").affected,0);
    assert_eq!(rows(&mut s), before);
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (1,'b','y',20) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n WHERE NOT (items.n>=EXCLUDED.n OR items.email IS NULL) AND items.code IS NOT NULL").affected,1);
    assert_eq!(rows(&mut s)[0].values[3], Value::Int(20));
    assert_eq!(run(&mut s,"INSERT INTO items (id,n) VALUES (1,30) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n WHERE EXCLUDED.email=items.email").affected,0);
}

#[test]
fn sql_insert_secondary_arbiter_and_constraints() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    indexes(&mut s);
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO items VALUES (2,'a','y',20) ON CONFLICT (email) DO UPDATE SET n=EXCLUDED.n"
        )
        .affected,
        1
    );
    assert_eq!(rows(&mut s).len(), 1);
    assert_eq!(rows(&mut s)[0].values[3], Value::Int(20));
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (3,'a','z',30) ON CONFLICT ON CONSTRAINT email_unique DO NOTHING").affected,0);
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (1,'b','z',30) ON CONFLICT ON CONSTRAINT items_pkey DO UPDATE SET n=EXCLUDED.n").affected,1);
    assert_eq!(rows(&mut s)[0].values[3], Value::Int(30));
    fail(
        &mut s,
        "INSERT INTO items VALUES (2,'b','y',10) ON CONFLICT (n) DO NOTHING",
        SqlError::Unsupported,
    );
    fail(
        &mut s,
        "INSERT INTO items VALUES (2,'b','y',10) ON CONFLICT ON CONSTRAINT missing DO NOTHING",
        SqlError::Unsupported,
    );
}

#[test]
fn sql_insert_composite_unique_target_is_a_set_and_uses_equivalent_indexes() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    run(
        &mut s,
        "CREATE UNIQUE INDEX pair_unique ON items (email,code)",
    );
    run(
        &mut s,
        "CREATE UNIQUE INDEX pair_reversed ON items (code,email)",
    );
    for target in ["email,code", "code,email"] {
        assert_eq!(run(&mut s,&format!("INSERT INTO items VALUES (2,'a','x',20) ON CONFLICT ({target}) DO UPDATE SET n=EXCLUDED.n")).affected,1);
        assert_eq!(rows(&mut s).len(), 1);
    }
    assert_eq!(rows(&mut s)[0].values[3], Value::Int(20));
    snapshot(&mut s)
        .secondary_validate_engine(|| false)
        .unwrap();
}

#[test]
fn sql_insert_composite_primary_target_is_a_set() {
    let (_, mut s, _) = fixture();
    run(
        &mut s,
        "CREATE TABLE pairs (a BIGINT,b BIGINT,n INTEGER,PRIMARY KEY (a,b))",
    );
    run(&mut s, "INSERT INTO pairs VALUES (1,2,10)");
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO pairs VALUES (1,2,20) ON CONFLICT (b,a) DO UPDATE SET n=EXCLUDED.n"
        )
        .affected,
        1
    );
    assert_eq!(
        run(&mut s, "SELECT n FROM pairs LIMIT 1024").rows[0].values,
        [Value::Int(20)]
    );
}

#[test]
fn sql_insert_no_target_do_nothing_checks_every_unique_constraint() {
    let (_, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    indexes(&mut s);
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (1,'b','y',20),(2,'a','z',20),(3,'c','x',20),(4,'d','w',40) ON CONFLICT DO NOTHING").affected,1);
    let got = rows(&mut s);
    assert_eq!(got.len(), 2);
    assert_eq!(got[1].values[0], Value::Int(4));
}

#[test]
fn sql_insert_in_statement_do_nothing_keeps_first() {
    let (_, mut s, _) = fixture();
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (1,'a','x',10),(1,'b','y',20),(2,'c','z',30) ON CONFLICT (id) DO NOTHING").affected,2);
    assert_eq!(rows(&mut s)[0].values[3], Value::Int(10));
    let (_, mut s, _) = fixture();
    indexes(&mut s);
    assert_eq!(run(&mut s,"INSERT INTO items VALUES (1,'a','x',10),(2,'a','y',20),(3,'b','x',30) ON CONFLICT DO NOTHING").affected,1);
    assert_eq!(rows(&mut s)[0].values[0], Value::Int(1));
}

#[test]
fn sql_insert_in_statement_update_duplicate_target_is_atomic() {
    let (_, mut s, _) = fixture();
    fail(
        &mut s,
        "INSERT INTO items VALUES (1,'a','x',10),(1,'b','y',20) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n",
        SqlError::CardinalityViolation,
    );
    assert!(rows(&mut s).is_empty());
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    let before = rows(&mut s);
    fail(
        &mut s,
        "INSERT INTO items VALUES (2,'b','y',20),(1,'c','z',30),(1,'d','w',40) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n",
        SqlError::CardinalityViolation,
    );
    assert_eq!(rows(&mut s), before);
}

#[test]
fn sql_insert_transaction_rollback_commit_reopen_and_sidecar() {
    let (admin, mut s, path) = fixture();
    indexes(&mut s);
    run(&mut s, "INSERT INTO items VALUES (1,'a','x',10)");
    let before = rows(&mut s);
    run(&mut s, "BEGIN");
    run(
        &mut s,
        "INSERT INTO items VALUES (1,'b','y',20),(2,'c','z',30) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n",
    );
    assert_eq!(rows(&mut s).len(), 2);
    snapshot(&mut s)
        .secondary_validate_engine(|| false)
        .unwrap();
    run(&mut s, "ROLLBACK");
    assert_eq!(rows(&mut s), before);
    run(&mut s, "BEGIN");
    run(
        &mut s,
        "INSERT INTO items VALUES (1,'b','y',20),(2,'c','z',30) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n",
    );
    run(&mut s, "COMMIT");
    let committed = rows(&mut s);
    drop(s);
    drop(admin);
    let admin = Admin::open(path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(rows(&mut s), committed);
    snapshot(&mut s)
        .secondary_validate_engine(|| false)
        .unwrap();
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO items VALUES (3,'a','w',99) ON CONFLICT (email) DO NOTHING"
        )
        .affected,
        0
    );
}

#[test]
fn sql_insert_describe_values_set_where_parameters() {
    let (_, mut s, _) = fixture();
    let p=s.prepare("INSERT INTO items VALUES ($1,$2,'x',$3),($4,$5,'y',$6) ON CONFLICT (id) DO UPDATE SET n=$7 WHERE items.n<$8 AND items.email=$9").unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(
        d.parameters,
        [
            DataType::BigInt,
            DataType::Text,
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Integer,
            DataType::Integer,
            DataType::Integer,
            DataType::Text
        ]
    );
    assert!(d.columns.is_empty());
    let p=s.prepare("INSERT INTO items (id,n) VALUES ($1,10) ON CONFLICT (id) DO UPDATE SET n=EXCLUDED.n WHERE $2>items.n").unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        [DataType::BigInt, DataType::Integer]
    );
}

#[test]
fn sql_insert_describe_unresolvable_and_conflicting_parameters() {
    let (_, mut s, _) = fixture();
    let p=s.prepare("INSERT INTO items (id,n) VALUES ($1,10) ON CONFLICT (id) DO UPDATE SET n=10 WHERE $2 IS NULL").unwrap();
    // PG18.6 oracle: J:/gigglesdb-work/e1-sql/evidence/upsert-finish-20261108-f40c1191/pg186-parameter-description.txt
    assert_eq!(
        s.describe(&p).unwrap_err(),
        CoreError::Sql(SqlError::IndeterminateDatatype)
    );
    let p = s
        .prepare("INSERT INTO items (id,email,n) VALUES ($1,$1,10),($1,'a',20)")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap_err(), CoreError::Sql(SqlError::Type));
}

#[test]
fn sql_insert_prepared_parameters_and_epoch_fence() {
    let (_, mut s, _) = fixture();
    let p = s
        .prepare("INSERT INTO items (id,n) VALUES ($1,$2),($3,$4)")
        .unwrap();
    assert_eq!(
        s.execute_prepared(&p, &[Value::Int(1)], None),
        Err(CoreError::Sql(SqlError::Parameters))
    );
    assert_eq!(
        s.execute_prepared(
            &p,
            &[
                Value::Int(1),
                Value::Int(10),
                Value::Int(2),
                Value::Text("bad".into())
            ],
            None
        ),
        Err(CoreError::Sql(SqlError::Type))
    );
    assert!(rows(&mut s).is_empty());
    run(&mut s, "CREATE INDEX by_n ON items (n)");
    assert_eq!(
        s.execute_prepared(
            &p,
            &[Value::Int(1), Value::Int(10), Value::Int(2), Value::Int(20)],
            None
        ),
        Err(CoreError::Sql(SqlError::StaleSchema))
    );
    assert!(rows(&mut s).is_empty());
}

#[test]
fn sql_insert_row_cap_and_expected_version_rejected() {
    let (_, mut s, _) = fixture();
    let text = format!(
        "INSERT INTO items (id,n) VALUES {}",
        (0..=MAX_ROWS)
            .map(|i| format!("({i},0)"))
            .collect::<Vec<_>>()
            .join(",")
    );
    fail(&mut s, &text, SqlError::Limit);
    assert!(rows(&mut s).is_empty());
    assert_eq!(
        s.execute(
            "INSERT INTO items (id,n) VALUES (1,10),(2,20)",
            &[],
            Some(1)
        ),
        Err(CoreError::Sql(SqlError::Unsupported))
    );
    assert!(rows(&mut s).is_empty());
    let text = format!(
        "INSERT INTO items (id,n) VALUES {}",
        (0..512)
            .map(|i| format!("({i},0)"))
            .collect::<Vec<_>>()
            .join(",")
    );
    assert_eq!(run(&mut s, &text).affected, 512);
    assert_eq!(rows(&mut s).len(), 512);
}

#[test]
fn sql_insert_parser_row_cap_1024_and_1025() {
    // Parse large VALUES lists, then check the unchanged legacy admission cap.
    let parser = |count: usize| {
        let mut tokens = lex("INSERT INTO items (id,n) VALUES").unwrap();
        for id in 0..count {
            if id != 0 {
                tokens.push(Token::Symbol(','));
            }
            tokens.extend(lex(&format!("({id},0)")).unwrap());
        }
        Parser { privilege_seen: false, privilege_casts: false,
            tokens,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        }
    };
    assert_eq!(
        admit_statement(&parse(&mut parser(MAX_ROWS + 1)).unwrap(), &[]).unwrap_err(),
        SqlError::Limit
    );
    let insert = parse(&mut parser(MAX_ROWS)).unwrap();
    let (_, mut session, _) = fixture();
    let mut state = snapshot(&mut session);
    assert_eq!(
        execute(&mut state, &insert, &[], None, &None, None, &mut || false)
            .unwrap()
            .affected,
        MAX_ROWS as u64
    );
    state.secondary_validate_engine(|| false).unwrap();
}

#[test]
fn sql_insert_constraint_descriptor_precedes_synthetic_primary_name() {
    let (_, mut session, _) = fixture();
    run(&mut session, "INSERT INTO items VALUES (1,'a','x',10)");
    run(
        &mut session,
        "CREATE UNIQUE INDEX items_pkey ON items (email)",
    );
    assert_eq!(run(&mut session, "INSERT INTO items VALUES (2,'a','y',20) ON CONFLICT ON CONSTRAINT items_pkey DO UPDATE SET n=EXCLUDED.n").affected, 1);
    let got = rows(&mut session);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].values[0], Value::Int(1));
    assert_eq!(got[0].values[3], Value::Int(20));
    fail(
        &mut session,
        "INSERT INTO items VALUES (1,'b','z',30) ON CONFLICT ON CONSTRAINT items_pkey DO NOTHING",
        SqlError::Duplicate,
    );
    assert_eq!(rows(&mut session), got);
    snapshot(&mut session)
        .secondary_validate_engine(|| false)
        .unwrap();
}

fn admission_insert() -> Insert {
    Insert {
        alias: None,
        casts: BTreeMap::new(),
        copy_input: None,
        normalized: None, copy_precoerced: false,
        source_types: None,
        table: "items".into(),
        columns: vec!["id".into(), "email".into(), "code".into(), "n".into()],
        rows: vec![
            vec![
                Expr::Literal(Value::Int(2)),
                Expr::Literal(Value::Text("b".into())),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Int(20)),
            ],
            vec![
                Expr::Literal(Value::Int(3)),
                Expr::Literal(Value::Text("c".into())),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Int(30)),
            ],
        ],
        defaults: vec![vec![false; 4]; 2],
        default_values: false,
        overriding: None,
        conflict: None,
        returning: None,
        source: None,
    }
}
fn create_wide_numeric_table(session: &mut SqlSession) {
    let columns = (0..50)
        .map(|i| format!("n{i} NUMERIC"))
        .collect::<Vec<_>>()
        .join(",");
    run(
        session,
        &format!("CREATE TABLE wide_numeric (id BIGINT PRIMARY KEY,{columns})"),
    );
}
fn wide_numeric_row_over_raw_budget() -> Vec<Value> {
    let number = Numeric::parse(&"9".repeat(1500)).unwrap();
    let raw = number.display_byte_bound();
    let row_raw = 9 + 50 * raw;
    let row_work = 9 + 50 * (raw + 8 * raw * raw);
    assert!(row_raw > MAX_ROW_BYTES);
    assert!(row_raw < operations::MAX_INSERT_BYTES);
    assert!(row_work < secondary_index::MAX_WORK);
    std::iter::once(Value::Int(2))
        .chain((0..50).map(|_| Value::Numeric(number.clone())))
        .collect()
}
fn assert_admission_rejected_without_copy(
    state: &mut Relational,
    insert: &Insert,
    params: &[Value],
    expected: SqlError,
) {
    // This owned baseline is deliberately captured before resetting witnesses.
    let before = state.clone();
    let mut ticks = 0;
    Relational::sql_insert_test_reset();
    assert_eq!(
        execute(state, insert, params, None, &None, None, &mut || {
            ticks += 1;
            false
        }),
        Err(expected)
    );
    assert_eq!(
        ticks, 1,
        "admission must fail between the two cancel fences"
    );
    assert_eq!(
        Relational::sql_insert_test_counts(),
        [0; 5],
        "[table clone, materialize, state clone, planner, apply]"
    );
    assert_eq!(*state, before);
    state.secondary_validate_engine(|| false).unwrap();
}

#[test]
fn sql_insert_admission_rejects_oversized_literals_before_copy() {
    let (_, mut session, _) = fixture();
    run(&mut session, "INSERT INTO items VALUES (1,'a','x',10)");
    run(
        &mut session,
        "CREATE TABLE heavy (id BIGINT PRIMARY KEY, n NUMERIC)",
    );
    create_wide_numeric_table(&mut session);
    let mut state = snapshot(&mut session);
    let mut insert = admission_insert();
    insert.rows = (0..=MAX_ROWS)
        .map(|id| {
            vec![
                Expr::Literal(Value::Int(id as i64)),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Int(1)),
            ]
        })
        .collect();
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Limit);
    let mut insert = admission_insert();
    insert.rows[0][1] = Expr::Literal(Value::Text("x".repeat(MAX_ROW_BYTES + 1)));
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Limit);
    let mut insert = admission_insert();
    insert.rows = (0..40)
        .map(|id| {
            vec![
                Expr::Literal(Value::Int(id)),
                Expr::Literal(Value::Text("x".repeat(MAX_ROW_BYTES / 2))),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Int(1)),
            ]
        })
        .collect();
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Limit);
    // Each numeric fits the per-value work bound and the combined raw bytes fit
    // the input budget, but their aggregate conversion work exceeds the bound.
    let number = Numeric::parse(&"9".repeat(9000)).unwrap();
    let raw = number.display_byte_bound();
    let per_row_work = raw + 8 * raw * raw + 9;
    assert!(per_row_work < secondary_index::MAX_WORK);
    assert!(2 * per_row_work > secondary_index::MAX_WORK);
    assert!(2 * (raw + 9) < operations::MAX_INSERT_BYTES);
    let insert = Insert {
        alias: None,
        casts: BTreeMap::new(),
        copy_input: None,
        normalized: None, copy_precoerced: false,
        source_types: None,
        table: "heavy".into(),
        columns: vec!["id".into(), "n".into()],
        rows: vec![
            vec![
                Expr::Literal(Value::Int(1)),
                Expr::Literal(Value::Numeric(number.clone())),
            ],
            vec![
                Expr::Literal(Value::Int(2)),
                Expr::Literal(Value::Numeric(number)),
            ],
        ],
        defaults: vec![vec![false; 2]; 2],
        default_values: false,
        overriding: None,
        conflict: None,
        returning: None,
        source: None,
    };
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Limit);
    // Fifty individually admitted numerics exceed the row's raw byte budget
    // while remaining below both the input byte and aggregate work budgets.
    let insert = Insert {
        alias: None,
        casts: BTreeMap::new(),
        copy_input: None,
        normalized: None, copy_precoerced: false,
        source_types: None,
        table: "wide_numeric".into(),
        columns: std::iter::once("id".to_owned())
            .chain((0..50).map(|i| format!("n{i}")))
            .collect(),
        rows: vec![
            wide_numeric_row_over_raw_budget()
                .into_iter()
                .map(Expr::Literal)
                .collect(),
            std::iter::once(Expr::Literal(Value::Int(3)))
                .chain((0..50).map(|_| Expr::Literal(Value::Null)))
                .collect(),
        ],
        defaults: vec![vec![false; 51]; 2],
        default_values: false,
        overriding: None,
        conflict: None,
        returning: None,
        source: None,
    };
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Limit);
}

#[test]
fn sql_insert_admission_rejects_arity_before_materialization() {
    let (_, mut session, _) = fixture();
    let mut state = snapshot(&mut session);
    let mut insert = admission_insert();
    insert.rows[1].pop();
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Syntax);
    insert.rows[1] = vec![Expr::Literal(Value::Null); MAX_TABLE_COLUMNS + 1];
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Syntax);
}

#[test]
fn sql_insert_admission_rejects_parameters_and_conflict_operands_before_copy() {
    let (_, mut session, _) = fixture();
    run(&mut session, "INSERT INTO items VALUES (1,'a','x',10)");
    let mut state = snapshot(&mut session);
    let mut insert = admission_insert();
    insert.rows[0][1] = Expr::Param(1);
    let params = [Value::Text("x".repeat(MAX_ROW_BYTES + 1))];
    assert_admission_rejected_without_copy(&mut state, &insert, &params, SqlError::Limit);
    let mut insert = admission_insert();
    insert.conflict = Some(Conflict {
        columns: Some(vec!["id".into()]),
        constraint: None,
        predicate: None,
        update: None,
    });
    insert.rows[0][1] = Expr::Literal(Value::Text("x".repeat(MAX_ROW_BYTES + 1)));
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Limit);
    for operand in [
        Operand::Value(Expr::Literal(Value::Text("x".repeat(MAX_ROW_BYTES + 1)))),
        Operand::Value(Expr::Param(1)),
    ] {
        let mut insert = admission_insert();
        insert.conflict = Some(Conflict {
            columns: Some(vec!["id".into()]),
            constraint: None,
            predicate: None,
            update: Some(Update {
                sets: vec![("email".into(), operand)],
                guard: None,
            }),
        });
        assert_admission_rejected_without_copy(&mut state, &insert, &params, SqlError::Limit);
    }
    let mut insert = admission_insert();
    insert.conflict = Some(Conflict {
        columns: Some(vec!["id".into()]),
        constraint: None,
        predicate: None,
        update: Some(Update {
            sets: vec![(
                "n".into(),
                Operand::Field(ConflictSource::Incoming, "n".into()),
            )],
            guard: Some(Condition::Compare(
                Operand::Field(ConflictSource::Existing, "email".into()),
                operations::Compare::Eq,
                Operand::Value(Expr::Literal(Value::Text("x".repeat(MAX_ROW_BYTES + 1)))),
            )),
        }),
    });
    assert_admission_rejected_without_copy(&mut state, &insert, &[], SqlError::Limit);
}

#[test]
fn sql_insert_admission_cancel_at_entry_before_any_copy() {
    let (_, mut session, _) = fixture();
    run(&mut session, "INSERT INTO items VALUES (1,'a','x',10)");
    let mut state = snapshot(&mut session);
    for conflict in [
        None,
        Some(Conflict {
            columns: None,
            constraint: None,
            predicate: None,
            update: None,
        }),
    ] {
        let mut insert = admission_insert();
        insert.conflict = conflict;
        let before = state.clone();
        let mut ticks = 0;
        Relational::sql_insert_test_reset();
        assert_eq!(
            execute(&mut state, &insert, &[], None, &None, None, &mut || {
                ticks += 1;
                true
            }),
            Err(SqlError::Aborted)
        );
        assert_eq!(ticks, 1);
        assert_eq!(Relational::sql_insert_test_counts(), [0; 5]);
        assert_eq!(state, before);
        state.secondary_validate_engine(|| false).unwrap();
    }
}

#[test]
fn sql_insert_conflict_bridge_admission_precedes_relational_clone() {
    let (_, mut session, _) = fixture();
    run(&mut session, "INSERT INTO items VALUES (1,'a','x',10)");
    create_wide_numeric_table(&mut session);
    let mut state = snapshot(&mut session);
    let table = state.tables["items"].clone();
    let before = state.clone();
    let too_many =
        vec![vec![Value::Int(2), Value::Null, Value::Null, Value::Int(20)]; MAX_ROWS + 1];
    let oversized = vec![vec![
        Value::Int(2),
        Value::Text("x".repeat(MAX_ROW_BYTES + 1)),
        Value::Null,
        Value::Int(20),
    ]];
    for incoming in [&too_many, &oversized] {
        let mut ticks = 0;
        Relational::sql_insert_test_reset();
        assert_eq!(
            state.sql_conflict_engine(
                &table,
                (None, None),
                incoming,
                &ConflictAction::Nothing,
                &mut || {
                    ticks += 1;
                    false
                }
            ),
            Err(SqlError::Limit)
        );
        assert_eq!(ticks, 1);
        assert_eq!(Relational::sql_insert_test_counts(), [0; 5]);
        assert_eq!(state, before);
        state.secondary_validate_engine(|| false).unwrap();
    }
    let incoming = vec![vec![
        Value::Int(2),
        Value::Null,
        Value::Null,
        Value::Int(20),
    ]];
    let action = ConflictAction::Update {
        guard: None,
        values: vec![(
            "email".into(),
            operations::Expr::Value(operations::ValueExpr::Literal(Value::Text(
                "x".repeat(MAX_ROW_BYTES + 1),
            ))),
        )],
    };
    let mut ticks = 0;
    Relational::sql_insert_test_reset();
    assert_eq!(
        state.sql_conflict_engine(
            &table,
            (Some(&["id".into()]), None),
            &incoming,
            &action,
            &mut || {
                ticks += 1;
                false
            }
        ),
        Err(SqlError::Limit)
    );
    assert_eq!(ticks, 1);
    assert_eq!(Relational::sql_insert_test_counts(), [0; 5]);
    assert_eq!(state, before);
    state.secondary_validate_engine(|| false).unwrap();
    let mut ticks = 0;
    Relational::sql_insert_test_reset();
    assert_eq!(
        state.sql_conflict_engine(
            &table,
            (None, None),
            &incoming,
            &ConflictAction::Nothing,
            &mut || {
                ticks += 1;
                true
            }
        ),
        Err(SqlError::Aborted)
    );
    assert_eq!(ticks, 1);
    assert_eq!(Relational::sql_insert_test_counts(), [0; 5]);
    assert_eq!(state, before);
    state.secondary_validate_engine(|| false).unwrap();
    let wide_table = state.tables["wide_numeric"].clone();
    let incoming = vec![wide_numeric_row_over_raw_budget()];
    let mut ticks = 0;
    Relational::sql_insert_test_reset();
    assert_eq!(
        state.sql_conflict_engine(
            &wide_table,
            (None, None),
            &incoming,
            &ConflictAction::Nothing,
            &mut || {
                ticks += 1;
                false
            }
        ),
        Err(SqlError::Limit)
    );
    assert_eq!(ticks, 1);
    assert_eq!(Relational::sql_insert_test_counts(), [0; 5]);
    assert_eq!(state, before);
    state.secondary_validate_engine(|| false).unwrap();
}

#[test]
fn sql_insert_admission_witness_counters_observe_successful_boundaries() {
    let (_, mut session, _) = fixture();
    let mut state = snapshot(&mut session);
    let mut insert = admission_insert();
    insert.conflict = Some(Conflict {
        columns: Some(vec!["id".into()]),
        constraint: None,
        predicate: None,
        update: None,
    });
    Relational::sql_insert_test_reset();
    assert_eq!(
        execute(&mut state, &insert, &[], None, &None, None, &mut || false)
            .unwrap()
            .affected,
        2
    );
    let counts = Relational::sql_insert_test_counts();
    assert!(counts[1] > 0, "owned incoming rows materialized");
    assert_eq!(counts[2], 1, "immutable planner source cloned");
    assert_eq!(counts[3], 1, "planner invoked");
    assert_eq!(counts[4], 1, "batch publisher invoked");
    state.secondary_validate_engine(|| false).unwrap();
}

#[test]
fn wide_input_keeps_multi_row_total_bound() {
    let _budget = crate::relational::ValueBudget::enter(4 * MAX_RESULT_BYTES, true);
    let mut input = InputAdmission::default();
    input.row();
    input.value(&Value::Text("x".repeat(MAX_RESULT_BYTES + 1))).unwrap();
    input.row();
    assert!(matches!(input.value(&Value::Int(1)), Err(SqlError::Limit)));
    let mut input = InputAdmission::default();
    for _ in 0..16 {
        input.row();
        input.value(&Value::Text("x".repeat(64 * 1024))).unwrap();
    }
    input.row();
    assert!(matches!(input.value(&Value::Int(1)), Err(SqlError::Limit)));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_single_wide_insert_returning_update_and_scan_cursor() {
    let mut nonce = [0; 16]; getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!("budget-single-{:032x}", u128::from_be_bytes(nonce)));
    let admin = Admin::create_engine(&path, Limits::default(), crate::Engine::V2).unwrap();
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE wide (id int PRIMARY KEY, body text)", &[], None).unwrap();
    for (id, size) in [(1, 1_048_577), (2, 204_800), (3, 2 * 1024 * 1024)] {
        let result = session.execute("INSERT INTO wide VALUES ($1,$2) RETURNING body",
            &[Value::Int(id), Value::Text("x".repeat(size))], None).unwrap();
        assert_eq!(result.rows[0].values, vec![Value::Text("x".repeat(size))]);
    }
    session.execute("UPDATE wide SET body=$1 WHERE id=2", &[Value::Text("y".repeat(204_800))], None).unwrap();
    let p = session.prepare("SELECT body FROM wide WHERE id=3").unwrap();
    let mut cursor = session.open_cursor(&p, &[]).unwrap();
    assert_eq!(cursor.fetch(256, 1024 * 1024).unwrap()[0].values,
        vec![Value::Text("x".repeat(2 * 1024 * 1024))]);
    drop(cursor); drop(session); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}


#[test]
fn sorted_values_append_matches_general_row_and_index_bytes() {
    use crate::storage::engine::{codec, indexes};

    let mut general = Relational::default();
    for ddl in [
        "CREATE COLLATION ci(provider=icu,locale='und-u-ks-level2',deterministic=false)",
        "CREATE TABLE sample (id TEXT COLLATE ci PRIMARY KEY, email TEXT COLLATE ci, n INTEGER)",
        "CREATE UNIQUE INDEX sample_email ON sample (email)",
    ] {
        let stmt = Prepared::parse(ddl, 0, general.epoch).unwrap();
        general.execute(&stmt, &[], None).unwrap();
    }
    let mut optimized = general.clone();
    let stmt = Prepared::parse(
        "INSERT INTO sample VALUES ('z',NULL,3), ($1,'Äbc',2) RETURNING id",
        1, general.epoch,
    ).unwrap();
    let params = [Value::Text("m".repeat(8192))];
    let expected = general.execute(&stmt, &params, None).unwrap();
    let mut order = None;
    let actual = optimized.execute_statement_described_with_append(
        &StatementDescription::new(&stmt), &params, None, || false, None, None,
        &mut Vec::new(), Some(&mut order), None,
    ).unwrap();
    assert_eq!(actual, expected);
    assert!(matches!(
        order, Some(row_sink::AppendOrder::Published(row_sink::AppendReplay::SortedOrdinals(ordinals)))
            if ordinals == [1, 0]
    ));
    let table = &general.tables["sample"];
    let expected = &general.rows[&table.id];
    let actual = &optimized.rows[&table.id];
    assert_eq!(actual, expected);
    for (key, row) in actual {
        let pk = codec::pk_bytes_iter(table.primary_key.iter().map(|i| &row.values[*i])).unwrap();
        assert_eq!(&pk, key);
        let before = &expected[key];
        assert_eq!(
            codec::encode_row(key, row.version, &row.values).unwrap(),
            codec::encode_row(&pk, before.version, &before.values).unwrap(),
        );
    }
    let entries = indexes::entries(
        Some(&optimized), table, actual, optimized.secondary_descriptors_engine(),
    ).unwrap();
    assert_eq!(
        entries,
        indexes::entries(Some(&general), table, expected, general.secondary_descriptors_engine()).unwrap(),
    );

    // The baseline at c761b044f wrote these bytes for the NULL and collated
    // index entries, the wide primary key, and the row with a NULL column.
    let nullable_pk = [0x03, 0x7a, 0x00, 0x00];
    let nullable_entry = [0, 0, 0, 0, 0, 0, 0, 6, 0xff, 0x03, 0x7a, 0, 0];
    assert_eq!(entries[nullable_entry.as_slice()], nullable_pk);
    let collated_entry = [0, 0, 0, 0, 0, 0, 0, 6, 0x03, 0xc3, 0x84, 0x62, 0x63, 0, 0];
    let mut wide_pk = vec![0x03];
    wide_pk.resize(8193, 0x6d);
    wide_pk.extend([0, 0]);
    assert_eq!(entries[collated_entry.as_slice()], wide_pk);
    assert!(actual.contains_key(&wide_pk));

    let ts = crate::storage::engine::hlc::Ts(42);
    let nullable_row = &actual[nullable_pk.as_slice()];
    assert_eq!(
        codec::row_key(table.id, &nullable_pk, ts),
        [0, 0, 0, 0, 0, 0, 0, 2, 0x03, 0x7a, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xd5],
    );
    assert_eq!(
        codec::encode_row(&nullable_pk, nullable_row.version, &nullable_row.values).unwrap(),
        [
            0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0x02, 0, 0x03, 0, 0, 0, 0x25,
            0, 0, 0, 0, 0, 0, 0, 0x03, 0, 0, 0, 0x03, 0, 0, 0, 0,
            0, 0, 0, 0x01, 0x7a, 0, 0, 0, 0, 0, 0, 0, 0x02, 0, 0, 0,
            0, 0, 0, 0, 0x03, 0x29, 0x92, 0x1b, 0x2f,
        ],
    );
    assert_eq!(
        optimized.secondary_persistence_engine(|| false).unwrap(),
        general.secondary_persistence_engine(|| false).unwrap(),
    );
}
