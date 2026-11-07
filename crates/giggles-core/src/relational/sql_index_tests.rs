use super::*;
use crate::{Admin, CoreError, Limits, SqlSession};
use std::path::PathBuf;

fn fixture() -> (Admin, SqlSession, PathBuf) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "gd42-sql-index-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE items (id BIGINT, email TEXT, other BIGINT, PRIMARY KEY (id))",
            &[],
            None,
        )
        .unwrap();
    (admin, session, path)
}
fn run(s: &mut SqlSession, text: &str) {
    s.execute(text, &[], None).unwrap();
}
fn snapshot(s: &mut SqlSession) -> Relational {
    let own = !s.in_transaction();
    if own {
        run(s, "BEGIN");
    }
    let fence = s.batch_fence_engine().unwrap();
    let (result, _) = s
        .apply_batch_engine(fence, &mut || false, |src, apply| {
            apply.apply(&[], src.next_version)?;
            Ok(src.clone())
        })
        .unwrap();
    if own {
        run(s, "ROLLBACK");
    }
    result
}
fn parsed(text: &str) -> CreateIndex {
    let p = Prepared::parse(text, 1, 0).unwrap();
    assert!(p.is_ddl());
    assert_eq!(p.command(), "CREATE INDEX");
    assert!(!p.is_read());
    match p.statement {
        Statement::CreateIndex(index) => index,
        _ => panic!("not index"),
    }
}
#[test]
fn sql_index_grammar_accept_matrix() {
    for text in [
        "CREATE INDEX ix ON items (email)",
        "create unique index if not exists ix on only items using btree (email asc nulls last, other ASC);",
        "CREATE INDEX \"ix\" ON \"items\" (\"email\")",
    ] {
        let p = parsed(text);
        assert_eq!(p.name, "ix");
        assert_eq!(p.table, "items");
        assert_eq!(p.columns[0], "email");
    }
    let p = parsed("CREATE UNIQUE INDEX IF NOT EXISTS ON items (email, other);");
    assert_eq!(p.name, "items_email_other_idx");
    assert_eq!(p.columns, ["email", "other"]);
    assert!(p.unique && p.if_not_exists);
    let column = "c".repeat(63);
    let p = parsed(&format!("CREATE INDEX ON items ({column})"));
    assert_eq!(p.name.len(), 63);
    assert!(p.name.ends_with("_idx"));
    ident(&p.name).unwrap();
}
#[test]
fn sql_index_grammar_reject_matrix() {
    for text in [
        "CREATE INDEX CONCURRENTLY ix ON items (email)",
        "CREATE INDEX ix ON items (email DESC)",
        "CREATE INDEX ix ON items (email NULLS FIRST)",
        "CREATE INDEX ix ON items ((email))",
        "CREATE INDEX ix ON items (lower(email))",
        "CREATE INDEX ix ON items (email text_ops)",
        "CREATE INDEX ix ON items (email COLLATE foo)",
        "CREATE INDEX ix ON items (email) INCLUDE (other)",
        "CREATE INDEX ix ON items (email) WHERE id > 0",
        "CREATE INDEX ix ON items (email) WITH (fillfactor=50)",
        "CREATE INDEX ix ON items (email) TABLESPACE foo",
        "CREATE INDEX ix ON items USING hash (email)",
        "CREATE INDEX ix ON items ()",
        "CREATE INDEX \"Ix\" ON items (email)",
        "CREATE INDEX ix ON \"Items\" (email)",
        "CREATE INDEX ix ON items (\"Email\")",
        "CREATE INDEX \"pg_ix\" ON items (email)",
    ] {
        assert_eq!(
            Prepared::parse(text, 1, 0).unwrap_err(),
            SqlError::Unsupported,
            "{text}"
        );
    }
    for text in [
        "CREATE INDEX ix",
        "CREATE INDEX \"\" ON items (email)",
        "CREATE INDEX \"bad\0name\" ON items (email)",
        "CREATE INDEX ix ON",
        "CREATE INDEX ix ON items",
        "CREATE INDEX ix ON items (email",
        "CREATE INDEX ix ON items (email,)",
        "CREATE INDEX ix ON items (email); SELECT 1",
        "CREATE INDEX ix ON items (email);;",
        "CREATE INDEX ix ON items (email) junk",
    ] {
        assert_eq!(
            Prepared::parse(text, 1, 0).unwrap_err(),
            SqlError::Syntax,
            "{text}"
        );
    }
}
#[test]
fn sql_index_empty_unique_nulls_and_descriptor() {
    let (_, mut s, _) = fixture();
    run(&mut s, "CREATE UNIQUE INDEX ix ON items (email, other)");
    let state = snapshot(&mut s);
    let d = &state.secondary_descriptors_engine()[0];
    assert_eq!(d.name, "ix");
    assert!(d.unique);
    assert_eq!(
        d.columns,
        [
            state.tables["items"].columns[1].id,
            state.tables["items"].columns[2].id
        ]
    );
    run(&mut s, "INSERT INTO items VALUES (1,'a',1)");
    assert_eq!(
        s.execute("INSERT INTO items VALUES (2,'a',1)", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    run(&mut s, "INSERT INTO items VALUES (3,NULL,1)");
    run(&mut s, "INSERT INTO items VALUES (4,NULL,1)");
}
fn verify_rows(state: &Relational, count: usize, unique: bool) {
    state.secondary_validate_engine(|| false).unwrap();
    let t = &state.tables["items"];
    let d = &state.secondary_descriptors_engine()[0];
    assert_eq!(d.unique, unique);
    let entries = state
        .secondary_state_engine(|| false)
        .unwrap()
        .lookup(d.id, None, None, || false)
        .unwrap();
    assert_eq!(entries.len(), count);
    let actual: BTreeSet<Vec<u8>> = entries.iter().map(|(_, key)| key.to_vec()).collect();
    let expected: BTreeSet<Vec<u8>> = state.rows[&t.id]
        .values()
        .map(|row| pk(t, &row.values).unwrap())
        .collect();
    assert_eq!(actual, expected);
    if unique {
        let limits = secondary_unique_lookup::Limits {
            bytes: 64 * 1024 * 1024,
            work: secondary_index::MAX_WORK * 3,
            candidates: secondary_index::MAX_CANDIDATES,
        };
        let mut matched = 0;
        for row in state.rows[&t.id].values() {
            let result = state
                .secondary_unique_lookup_engine(
                    t,
                    state.epoch,
                    d,
                    &[row.values[1].clone()],
                    &limits,
                    || false,
                )
                .unwrap();
            if row.values[1] == Value::Null {
                assert!(result.is_none());
            } else {
                assert_eq!(result.unwrap().1, row);
                matched += 1;
            }
        }
        assert_eq!(matched, count - 2);
    }
}
#[test]
fn sql_index_populated_unique_lookup_commit_reopen() {
    let (admin, mut s, path) = fixture();
    for id in 0..50 {
        run(
            &mut s,
            &format!(
                "INSERT INTO items VALUES ({id},{},0)",
                if id < 2 {
                    "NULL".into()
                } else {
                    format!("'mail{id}'")
                }
            ),
        );
    }
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE UNIQUE INDEX ix ON items (email)");
    verify_rows(&snapshot(&mut s), 50, true);
    run(&mut s, "COMMIT");
    verify_rows(&snapshot(&mut s), 50, true);
    assert_eq!(
        s.execute("INSERT INTO items VALUES (99,'mail2',0)", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    drop(s);
    drop(admin);
    let admin = Admin::open(path, Limits::default()).unwrap();
    verify_rows(&snapshot(&mut admin.sql_session()), 50, true);
}
#[test]
fn sql_index_populated_nonunique_and_other_table_rows() {
    let (admin, mut s, path) = fixture();
    run(
        &mut s,
        "CREATE TABLE unrelated (id BIGINT, PRIMARY KEY (id))",
    );
    run(&mut s, "INSERT INTO unrelated VALUES (1)");
    for id in 0..50 {
        run(&mut s, &format!("INSERT INTO items VALUES ({id},'same',0)"));
    }
    run(&mut s, "CREATE INDEX ix ON items (email)");
    verify_rows(&snapshot(&mut s), 50, false);
    run(&mut s, "INSERT INTO items VALUES (99,'same',0)");
    verify_rows(&snapshot(&mut s), 51, false);
    drop(s);
    drop(admin);
    let admin = Admin::open(path, Limits::default()).unwrap();
    verify_rows(&snapshot(&mut admin.sql_session()), 51, false);
}
#[test]
fn sql_index_existing_duplicates_atomic_and_transaction_recovers() {
    let (admin, mut s, _) = fixture();
    run(&mut s, "INSERT INTO items VALUES (1,'same',0)");
    run(&mut s, "INSERT INTO items VALUES (2,'same',0)");
    let epoch = admin.catalog().unwrap().0;
    assert_eq!(
        s.execute("CREATE UNIQUE INDEX ix ON items (email)", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    let state = snapshot(&mut s);
    assert_eq!(state.epoch, epoch);
    assert!(state.secondary_descriptors_engine().is_empty());
    run(&mut s, "BEGIN");
    assert_eq!(
        s.execute("CREATE UNIQUE INDEX ix ON items (email)", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert!(s.is_aborted());
    assert_eq!(
        s.execute("SELECT * FROM items", &[], None),
        Err(CoreError::Sql(SqlError::Aborted))
    );
    run(&mut s, "ROLLBACK");
    assert!(!s.is_aborted());
    assert_eq!(admin.catalog().unwrap().0, epoch);
    run(&mut s, "CREATE INDEX ix ON items (email)");
    verify_rows(&snapshot(&mut s), 2, false);
}
#[test]
fn sql_index_transaction_rollback_name_reuse_commit_reopen() {
    let (admin, mut s, path) = fixture();
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE INDEX ix ON items (email)");
    assert_eq!(snapshot(&mut s).secondary_descriptors_engine().len(), 1);
    run(&mut s, "ROLLBACK");
    assert!(snapshot(&mut s).secondary_descriptors_engine().is_empty());
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE INDEX ix ON items (email)");
    run(&mut s, "COMMIT");
    drop(s);
    drop(admin);
    let admin = Admin::open(path, Limits::default()).unwrap();
    let state = snapshot(&mut admin.sql_session());
    assert_eq!(state.secondary_descriptors_engine()[0].name, "ix");
    state.secondary_validate_engine(|| false).unwrap();
}
#[test]
fn sql_index_exact_errors_and_if_not_exists_noop() {
    let (admin, mut s, _) = fixture();
    for (text, error) in [
        ("CREATE INDEX ix ON missing (email)", SqlError::UnknownTable),
        (
            "CREATE INDEX ix ON items (missing)",
            SqlError::UnknownColumn,
        ),
        (
            "CREATE INDEX ix ON items (email,email)",
            SqlError::Duplicate,
        ),
    ] {
        assert_eq!(s.execute(text, &[], None), Err(CoreError::Sql(error)));
    }
    run(&mut s, "CREATE INDEX ix ON items (email)");
    assert_eq!(
        s.execute("CREATE INDEX ix ON items (email)", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    run(&mut s, "INSERT INTO items VALUES (1,'a',0)");
    let before = snapshot(&mut s);
    let epoch = admin.catalog().unwrap().0;
    run(
        &mut s,
        "CREATE UNIQUE INDEX IF NOT EXISTS ix ON items (other)",
    );
    let mut after = snapshot(&mut s);
    assert!(after.generation > before.generation);
    after.generation = before.generation;
    assert_eq!(after, before);
    assert_eq!(admin.catalog().unwrap().0, epoch);
}
#[test]
fn sql_index_prepared_epoch_stale_and_ddl_metadata() {
    let (_, mut s, _) = fixture();
    let old = s.prepare("SELECT * FROM items").unwrap();
    let ddl = s.prepare("CREATE INDEX ix ON items (email)").unwrap();
    assert!(ddl.is_ddl());
    assert_eq!(ddl.command(), "CREATE INDEX");
    assert!(!ddl.is_read());
    s.execute_prepared(&ddl, &[], None).unwrap();
    assert_eq!(
        s.execute_prepared(&old, &[], None),
        Err(CoreError::Sql(SqlError::StaleSchema))
    );
    assert_eq!(
        s.execute_prepared(&ddl, &[], None),
        Err(CoreError::Sql(SqlError::StaleSchema))
    );
}
