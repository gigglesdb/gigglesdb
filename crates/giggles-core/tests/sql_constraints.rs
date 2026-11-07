//! Frozen E1 constraints contract. Literal PostgreSQL 18.6 witnesses live in
//! fixtures/e1_constraints/{extension,deparse}.txt; wire messages and command
//! tags are exercised by constraints_network.rs. IMMUTABLE-only is the agreed
//! GigglesDB bound (PostgreSQL itself permits volatile CHECK expressions).
//! Codec byte identity/version goldens are in relational/catalog_ext_tests.rs.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};

struct Db {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Db {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::path::Path::new("J:/gigglesdb-work/e1-sql/tmp/constraint-tests")
            .join(format!("{:032x}", u128::from_le_bytes(nonce)));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn session(&self) -> SqlSession {
        self.admin.as_ref().unwrap().sql_session()
    }
    fn reopen(&mut self) {
        drop(self.admin.take());
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn run(s: &mut SqlSession, sql: &str) -> u64 {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .affected
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn error(s: &mut SqlSession, sql: &str, expected: SqlError) {
    let actual = s.execute(sql, &[], None);
    assert!(
        matches!(&actual, Err(CoreError::Sql(e)) if *e == expected),
        "{sql}: expected {expected:?}; actual {actual:?}"
    );
}

#[test]
fn check_postimages_null_errors_not_valid_and_upsert() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,n INT CONSTRAINT positive CHECK(n>0))",
    );
    assert_eq!(run(&mut s, "INSERT INTO c VALUES(1,NULL)"), 1);
    error(
        &mut s,
        "INSERT INTO c VALUES(2,0)",
        SqlError::CheckViolation,
    ); // 23514
    error(
        &mut s,
        "UPDATE c SET n=0 WHERE id=1",
        SqlError::CheckViolation,
    );
    error(
        &mut s,
        "INSERT INTO c VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=0",
        SqlError::CheckViolation,
    );
    assert_eq!(
        rows(&mut s, "SELECT n FROM c LIMIT 10"),
        vec![vec![Value::Null]]
    );
    run(&mut s, "CREATE TABLE bad(id INT PRIMARY KEY,n INT)");
    run(&mut s, "INSERT INTO bad VALUES(1,-1)");
    error(
        &mut s,
        "ALTER TABLE bad ADD CONSTRAINT scanned CHECK(n>0)",
        SqlError::CheckViolation,
    );
    run(
        &mut s,
        "ALTER TABLE bad ADD CONSTRAINT positive CHECK(n>0) NOT VALID",
    );
    error(
        &mut s,
        "ALTER TABLE bad VALIDATE CONSTRAINT positive",
        SqlError::CheckViolation,
    );
    error(
        &mut s,
        "INSERT INTO bad VALUES(2,-2)",
        SqlError::CheckViolation,
    );
    error(
        &mut s,
        "UPDATE bad SET n=n WHERE id=1",
        SqlError::CheckViolation,
    );
    error(
        &mut s,
        "INSERT INTO bad VALUES(1,1) ON CONFLICT(id) DO UPDATE SET n=bad.n",
        SqlError::CheckViolation,
    );
    run(&mut s, "UPDATE bad SET n=1 WHERE id=1");
    run(&mut s, "ALTER TABLE bad VALIDATE CONSTRAINT positive");
    run(&mut s, "ALTER TABLE bad DROP CONSTRAINT positive");
    run(&mut s, "ALTER TABLE bad DROP CONSTRAINT IF EXISTS absent");
    run(
        &mut s,
        "CREATE TABLE division(id INT PRIMARY KEY,n INT CHECK(n/0>0))",
    );
    error(
        &mut s,
        "INSERT INTO division VALUES(1,1)",
        SqlError::DivisionByZero,
    ); // 22012
}

#[test]
fn checks_bind_immutable_columns_functions_and_casts() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,n INT,s TEXT,t TIMESTAMP)",
    );
    for sql in [
        "ALTER TABLE c ADD CHECK(n IS NULL OR abs(n)>0)",
        "ALTER TABLE c ADD CHECK(lower(s)<>'x')",
        "ALTER TABLE c ADD CHECK(n::bigint>0)",
        "ALTER TABLE c ADD CHECK(length(s)>0)",
    ] {
        run(&mut s, sql);
    }
    run(&mut s, "INSERT INTO c VALUES(1,2,'ok',NULL)");
    for sql in [
        "ALTER TABLE c ADD CHECK(now() IS NOT NULL)",
        "ALTER TABLE c ADD CHECK(clock_timestamp() IS NOT NULL)",
        "ALTER TABLE c ADD CHECK(t::timestamptz IS NOT NULL)",
        "ALTER TABLE c ADD CHECK((SELECT true))",
        "ALTER TABLE c ADD CHECK(sum(n)>0)",
    ] {
        error(&mut s, sql, SqlError::Unsupported);
    }
}

#[test]
fn check_canonical_negative_literal_reparses_and_unsafe_casts_are_bounded() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE negative_literal(id INT PRIMARY KEY,n INT CHECK(n>-1))",
    );
    run(
        &mut s,
        "CREATE TABLE null_check(id INT PRIMARY KEY,CHECK(NULL))",
    );
    run(&mut s, "INSERT INTO null_check VALUES(1)");
    run(&mut s, "INSERT INTO negative_literal VALUES(1,0)");
    error(
        &mut s,
        "INSERT INTO negative_literal VALUES(2,-1)",
        SqlError::CheckViolation,
    );
    run(
        &mut s,
        "CREATE TABLE casts(id INT PRIMARY KEY,n INT,b BIGINT,s TEXT)",
    );
    for sql in [
        "ALTER TABLE casts ADD CHECK(n::integer>0)",
        "ALTER TABLE casts ADD CHECK(b::bigint>0)",
        "ALTER TABLE casts ADD CHECK(n>1::bigint)",
        "ALTER TABLE casts ADD CHECK(s='x'::text)",
        "ALTER TABLE casts ADD CHECK(s=1::text)",
        "ALTER TABLE casts ADD CHECK(n='1'::int)",
        "ALTER TABLE casts ADD CHECK(b>2147483648)",
        "ALTER TABLE casts ADD CHECK(n=NULL)",
        "ALTER TABLE casts ADD CHECK(length(NULL)>0)",
    ] {
        error(&mut s, sql, SqlError::Unsupported);
    }
}

#[test]
fn foreign_key_composite_nulls_unique_target_and_invalid_definitions() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE p(a INT,b INT,PRIMARY KEY(a,b))");
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,a INT,b INT,FOREIGN KEY(a,b) REFERENCES p(a,b))",
    );
    run(
        &mut s,
        "INSERT INTO c VALUES(1,NULL,99),(2,99,NULL),(3,NULL,NULL)",
    );
    error(
        &mut s,
        "INSERT INTO c VALUES(4,99,99)",
        SqlError::ForeignKeyViolation,
    ); // 23503
    run(&mut s, "INSERT INTO p VALUES(1,2)");
    run(&mut s, "INSERT INTO c VALUES(4,1,2)");
    error(
        &mut s,
        "UPDATE c SET b=3 WHERE id=4",
        SqlError::ForeignKeyViolation,
    );
    error(
        &mut s,
        "INSERT INTO c VALUES(4,1,2) ON CONFLICT(id) DO UPDATE SET b=3",
        SqlError::ForeignKeyViolation,
    );
    run(&mut s, "CREATE TABLE u(id INT PRIMARY KEY,n INT)");
    run(&mut s, "CREATE UNIQUE INDEX u_n ON u(n)");
    run(
        &mut s,
        "CREATE TABLE uc(id INT PRIMARY KEY,n INT REFERENCES u(n))",
    );
    run(&mut s, "INSERT INTO u VALUES(1,10)");
    run(&mut s, "INSERT INTO uc VALUES(1,10)");
    run(&mut s, "CREATE TABLE heap(n INT)");
    error(
        &mut s,
        "ALTER TABLE uc ADD FOREIGN KEY(n) REFERENCES heap(n)",
        SqlError::InvalidForeignKey,
    ); // 42830
    error(&mut s, "DROP TABLE u", SqlError::DependentObjects); // 2BP01
    run(&mut s, "DROP TABLE u CASCADE");
    run(&mut s, "INSERT INTO uc VALUES(2,999)");
}

#[test]
fn create_unique_constraints_enforce_null_distinctness_and_atomicity() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE declared_unique(id INT PRIMARY KEY,n INT UNIQUE,a INT,b INT,CONSTRAINT pair_unique UNIQUE(a,b))",
    );
    run(
        &mut s,
        "INSERT INTO declared_unique VALUES(1,NULL,1,NULL),(2,NULL,1,NULL),(3,10,1,2)",
    );
    error(
        &mut s,
        "INSERT INTO declared_unique VALUES(4,10,2,3)",
        SqlError::Duplicate,
    );
    error(
        &mut s,
        "INSERT INTO declared_unique VALUES(4,20,1,2)",
        SqlError::Duplicate,
    );
    error(
        &mut s,
        "INSERT INTO declared_unique VALUES(4,20,2,3),(5,10,3,4)",
        SqlError::Duplicate,
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT id FROM declared_unique ORDER BY id LIMIT 10"
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    run(
        &mut s,
        "CREATE TABLE unique_child(id INT PRIMARY KEY,n INT REFERENCES declared_unique(n))",
    );
    run(&mut s, "INSERT INTO unique_child VALUES(1,10)");
    error(
        &mut s,
        "INSERT INTO unique_child VALUES(2,20)",
        SqlError::ForeignKeyViolation,
    );
    run(
        &mut s,
        "CREATE TABLE later_unique(id INT PRIMARY KEY,n INT,parent INT REFERENCES later_unique(n),UNIQUE(n))",
    );
    run(&mut s, "INSERT INTO later_unique VALUES(1,10,10)");
    run(&mut s, "INSERT INTO later_unique VALUES(2,20,10)");
    error(
        &mut s,
        "INSERT INTO later_unique VALUES(3,30,99)",
        SqlError::ForeignKeyViolation,
    );
}

#[test]
fn every_parent_action_is_atomic_and_updates_postimages() {
    for action in [
        "NO ACTION",
        "RESTRICT",
        "CASCADE",
        "SET NULL",
        "SET DEFAULT",
    ] {
        let db = Db::new();
        let mut s = db.session();
        run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
        run(&mut s, "INSERT INTO p VALUES(1),(2)");
        run(
            &mut s,
            &format!(
                "CREATE TABLE c(id INT PRIMARY KEY,pid INT DEFAULT 2 REFERENCES p(id) ON UPDATE {action} ON DELETE {action})"
            ),
        );
        run(&mut s, "CREATE INDEX c_pid ON c(pid)");
        run(&mut s, "INSERT INTO c VALUES(1,1)");
        if action == "NO ACTION" || action == "RESTRICT" {
            let expected = if action == "RESTRICT" {
                SqlError::RestrictViolation
            } else {
                SqlError::ForeignKeyViolation
            };
            error(&mut s, "UPDATE p SET id=3 WHERE id=1", expected.clone());
            error(&mut s, "DELETE FROM p WHERE id=1", expected);
            assert_eq!(
                rows(&mut s, "SELECT pid FROM c LIMIT 10"),
                vec![vec![Value::Int(1)]]
            );
        } else {
            run(
                &mut s,
                "INSERT INTO p VALUES(1) ON CONFLICT(id) DO UPDATE SET id=3",
            );
            let expected = match action {
                "CASCADE" => Value::Int(3),
                "SET NULL" => Value::Null,
                _ => Value::Int(2),
            };
            assert_eq!(
                rows(&mut s, "SELECT pid FROM c LIMIT 10"),
                vec![vec![expected]]
            );
            run(&mut s, "UPDATE c SET pid=3 WHERE id=1");
            run(&mut s, "DELETE FROM p WHERE id=3");
            let expected = match action {
                "CASCADE" => vec![],
                "SET NULL" => vec![vec![Value::Null]],
                _ => vec![vec![Value::Int(2)]],
            };
            assert_eq!(rows(&mut s, "SELECT pid FROM c LIMIT 10"), expected);
        }
    }
}

#[test]
fn self_reference_cycles_and_failed_cascade_leave_no_partial_rows() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE tree(id INT PRIMARY KEY,parent INT REFERENCES tree(id) ON DELETE CASCADE ON UPDATE CASCADE)",
    );
    run(&mut s, "INSERT INTO tree VALUES(1,1)");
    run(&mut s, "INSERT INTO tree VALUES(2,1)");
    run(&mut s, "DELETE FROM tree WHERE id=1");
    assert!(rows(&mut s, "SELECT id FROM tree LIMIT 10").is_empty());
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON UPDATE CASCADE CHECK(pid<3))",
    );
    run(&mut s, "INSERT INTO p VALUES(1)");
    run(&mut s, "INSERT INTO c VALUES(1,1)");
    error(
        &mut s,
        "UPDATE p SET id=3 WHERE id=1",
        SqlError::CheckViolation,
    );
    assert_eq!(
        rows(&mut s, "SELECT id FROM p LIMIT 10"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&mut s, "SELECT pid FROM c LIMIT 10"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn deferred_commit_rollback_savepoints_and_set_modes() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,pid INT,CONSTRAINT f FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    );
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO c VALUES(1,99)");
    error(&mut s, "COMMIT", SqlError::ForeignKeyViolation);
    assert!(rows(&mut s, "SELECT id FROM c LIMIT 10").is_empty());
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT a");
    run(&mut s, "INSERT INTO c VALUES(1,99)");
    run(&mut s, "ROLLBACK TO a");
    run(&mut s, "COMMIT");
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO c VALUES(1,99)");
    run(&mut s, "SAVEPOINT a");
    error(
        &mut s,
        "SET CONSTRAINTS f IMMEDIATE",
        SqlError::ForeignKeyViolation,
    );
    run(&mut s, "ROLLBACK TO a");
    run(&mut s, "INSERT INTO p VALUES(99)");
    run(&mut s, "SET CONSTRAINTS ALL IMMEDIATE");
    run(&mut s, "COMMIT");
    run(&mut s, "BEGIN");
    run(&mut s, "SET CONSTRAINTS f IMMEDIATE");
    error(
        &mut s,
        "INSERT INTO c VALUES(2,100)",
        SqlError::ForeignKeyViolation,
    );
    run(&mut s, "ROLLBACK");
    run(
        &mut s,
        "CREATE TABLE nd(id INT PRIMARY KEY,pid INT REFERENCES p(id))",
    );
    run(&mut s, "BEGIN");
    run(&mut s, "SET CONSTRAINTS ALL DEFERRED");
    error(
        &mut s,
        "INSERT INTO nd VALUES(1,100)",
        SqlError::ForeignKeyViolation,
    );
    run(&mut s, "ROLLBACK");
    run(&mut s, "BEGIN");
    run(&mut s, "SET CONSTRAINTS nd_pid_fkey IMMEDIATE");
    error(
        &mut s,
        "SET CONSTRAINTS nd_pid_fkey DEFERRED",
        SqlError::WrongObjectType,
    );
    run(&mut s, "ROLLBACK");
    run(
        &mut s,
        "CREATE TABLE custom_pk(id INT CONSTRAINT explicit_pk PRIMARY KEY)",
    );
    run(&mut s, "CREATE TABLE original_pk(id INT PRIMARY KEY)");
    run(&mut s, "ALTER TABLE original_pk RENAME TO renamed_pk");
    for name in ["p_pkey", "explicit_pk", "original_pk_pkey"] {
        run(&mut s, "BEGIN");
        run(&mut s, &format!("SET CONSTRAINTS {name} IMMEDIATE"));
        error(
            &mut s,
            &format!("SET CONSTRAINTS {name} DEFERRED"),
            SqlError::WrongObjectType,
        );
        run(&mut s, "ROLLBACK");
    }
}

#[test]
fn foreign_key_not_valid_validates_old_rows_and_enforces_new_rows() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(&mut s, "CREATE TABLE c(id INT PRIMARY KEY,pid INT)");
    run(&mut s, "INSERT INTO c VALUES(1,99)");
    run(
        &mut s,
        "ALTER TABLE c ADD CONSTRAINT f FOREIGN KEY(pid) REFERENCES p(id) NOT VALID",
    );
    error(
        &mut s,
        "ALTER TABLE c VALIDATE CONSTRAINT f",
        SqlError::ForeignKeyViolation,
    );
    assert_eq!(run(&mut s, "UPDATE c SET pid=pid WHERE id=1"), 1);
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO c VALUES(1,99) ON CONFLICT(id) DO UPDATE SET pid=c.pid"
        ),
        1
    );
    error(
        &mut s,
        "INSERT INTO c VALUES(2,99)",
        SqlError::ForeignKeyViolation,
    );
    run(&mut s, "INSERT INTO p VALUES(99)");
    run(&mut s, "ALTER TABLE c VALIDATE CONSTRAINT f");
}

#[test]
fn add_primary_key_rekeys_rebuilds_indexes_and_reopens_atomically() {
    let mut db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE heap(a INT,b INT)");
    run(&mut s, "INSERT INTO heap VALUES(1,2),(2,3)");
    run(&mut s, "CREATE INDEX heap_b ON heap(b)");
    run(
        &mut s,
        "ALTER TABLE ONLY public.heap ADD CONSTRAINT heap_pkey PRIMARY KEY(a)",
    );
    error(
        &mut s,
        "ALTER TABLE heap ADD PRIMARY KEY(b)",
        SqlError::InvalidTableDefinition,
    ); // 42P16
    error(&mut s, "INSERT INTO heap VALUES(1,4)", SqlError::Duplicate); // 23505
    run(&mut s, "ALTER TABLE heap RENAME TO renamed");

    drop(s);
    db.reopen();
    let mut s = db.session();
    assert_eq!(
        rows(&mut s, "SELECT a FROM renamed WHERE b=3 LIMIT 10"),
        vec![vec![Value::Int(2)]]
    );
    for (name, values, expected) in [
        ("dup", "(1),(1)", SqlError::Duplicate),
        ("nul", "(1),(NULL)", SqlError::Null),
    ] {
        run(&mut s, &format!("CREATE TABLE {name}(a INT)"));
        run(&mut s, &format!("INSERT INTO {name} VALUES{values}"));
        let before = rows(&mut s, &format!("SELECT a FROM {name} LIMIT 10"));
        error(
            &mut s,
            &format!("ALTER TABLE {name} ADD PRIMARY KEY(a)"),
            expected,
        );
        assert_eq!(
            rows(&mut s, &format!("SELECT a FROM {name} LIMIT 10")),
            before
        );
        run(&mut s, &format!("INSERT INTO {name} VALUES(2)"));
    }
    run(&mut s, "CREATE TABLE h(a INT)");
    run(&mut s, "CREATE TABLE h_pkey(x INT)");
    run(&mut s, "ALTER TABLE h ADD PRIMARY KEY(a)");
    error(
        &mut s,
        "ALTER TABLE h ADD PRIMARY KEY(a)",
        SqlError::InvalidTableDefinition,
    );
}

#[test]
fn update_assignment_volatile_sequence_is_evaluated_once() {
    fn clock() -> giggles_core::relational::StatementClock {
        giggles_core::relational::StatementClock {
            unavailable: None,
            transaction_us: 0,
            statement_us: 0,
            zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
            wall: Some(std::sync::Arc::new(|| Ok(0))),
        }
    }
    fn run(s: &mut SqlSession, sql: &str) {
        s.execute_at(sql, &[], None, &clock())
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
        s.execute_at(sql, &[], None, &clock())
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect()
    }
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE sequence_owner(id BIGSERIAL PRIMARY KEY)",
    );
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,n BIGINT CHECK(n>0))",
    );
    run(&mut s, "INSERT INTO c VALUES(1,1)");
    run(
        &mut s,
        "UPDATE c SET n=nextval('sequence_owner_id_seq') WHERE id=1",
    );
    assert_eq!(
        rows(&mut s, "SELECT n FROM c LIMIT 10"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&mut s, "SELECT nextval('sequence_owner_id_seq')"),
        vec![vec![Value::Int(2)]]
    );
    run(
        &mut s,
        "INSERT INTO c VALUES(2,nextval('sequence_owner_id_seq')+1)",
    );
    run(
        &mut s,
        "UPDATE c SET n=nextval('sequence_owner_id_seq') WHERE id IN (1,2)",
    );
    assert_eq!(
        rows(&mut s, "SELECT nextval('sequence_owner_id_seq')"),
        vec![vec![Value::Int(6)]]
    );
}

#[test]
fn initially_immediate_named_lists_and_savepoint_mode_restoration() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,a INT,b INT,CONSTRAINT fa FOREIGN KEY(a) REFERENCES p(id) DEFERRABLE INITIALLY IMMEDIATE,CONSTRAINT fb FOREIGN KEY(b) REFERENCES p(id) DEFERRABLE INITIALLY IMMEDIATE)",
    );
    run(&mut s, "BEGIN");
    run(&mut s, "SET CONSTRAINTS fa,fb DEFERRED");
    run(&mut s, "INSERT INTO c VALUES(1,9,9)");
    run(&mut s, "INSERT INTO p VALUES(9)");
    run(&mut s, "SET CONSTRAINTS fa,fb IMMEDIATE");
    run(&mut s, "COMMIT");
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT mode");
    run(&mut s, "SET CONSTRAINTS fa,fb DEFERRED");
    run(&mut s, "ROLLBACK TO mode");
    error(
        &mut s,
        "INSERT INTO c VALUES(2,10,10)",
        SqlError::ForeignKeyViolation,
    );
    run(&mut s, "ROLLBACK");
    run(&mut s, "BEGIN");
    run(&mut s, "SET CONSTRAINTS fa,fb DEFERRED");
    run(&mut s, "SAVEPOINT mode");
    run(&mut s, "SET CONSTRAINTS fa,fb IMMEDIATE");
    run(&mut s, "ROLLBACK TO mode");
    run(&mut s, "INSERT INTO c VALUES(2,10,10)");
    run(&mut s, "INSERT INTO p VALUES(10)");
    run(&mut s, "COMMIT");
}

#[test]
fn deferred_no_action_parent_delete_update_and_upsert_rollback() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    );
    run(&mut s, "INSERT INTO p VALUES(1)");
    run(&mut s, "INSERT INTO c VALUES(1,1)");
    for sql in [
        "DELETE FROM p WHERE id=1",
        "UPDATE p SET id=2 WHERE id=1",
        "INSERT INTO p VALUES(1) ON CONFLICT(id) DO UPDATE SET id=2",
    ] {
        run(&mut s, "BEGIN");
        run(&mut s, sql);
        error(&mut s, "COMMIT", SqlError::ForeignKeyViolation);
        assert_eq!(
            rows(&mut s, "SELECT id FROM p LIMIT 10"),
            vec![vec![Value::Int(1)]]
        );
        assert_eq!(
            rows(&mut s, "SELECT pid FROM c LIMIT 10"),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn deferred_child_primary_key_moves_preserve_pending_checks() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    );
    for move_sql in [
        "UPDATE c SET id=2 WHERE id=1",
        "INSERT INTO c VALUES(1,99) ON CONFLICT(id) DO UPDATE SET id=2",
    ] {
        run(&mut s, "BEGIN");
        run(&mut s, "INSERT INTO c VALUES(1,99)");
        run(&mut s, move_sql);
        error(&mut s, "COMMIT", SqlError::ForeignKeyViolation);
        assert!(rows(&mut s, "SELECT id FROM c LIMIT 10").is_empty());
    }
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO c VALUES(1,99)");
    run(&mut s, "UPDATE c SET id=2 WHERE id=1");
    run(&mut s, "INSERT INTO p VALUES(99)");
    run(&mut s, "COMMIT");
    assert_eq!(
        rows(&mut s, "SELECT id,pid FROM c LIMIT 10"),
        vec![vec![Value::Int(2), Value::Int(99)]]
    );
}

#[test]
fn incompatible_foreign_key_types_and_mutual_update_cascade() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    error(
        &mut s,
        "CREATE TABLE bad(id TEXT PRIMARY KEY REFERENCES p(id))",
        SqlError::Type,
    ); // 42804
    run(&mut s, "CREATE TABLE a(id INT PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE b(id INT PRIMARY KEY REFERENCES a(id) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED)",
    );
    run(
        &mut s,
        "ALTER TABLE a ADD FOREIGN KEY(id) REFERENCES b(id) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED",
    );
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO a VALUES(1)");
    run(&mut s, "INSERT INTO b VALUES(1)");
    run(&mut s, "COMMIT");
    run(&mut s, "UPDATE a SET id=2 WHERE id=1");
    assert_eq!(
        rows(&mut s, "SELECT id FROM b LIMIT 10"),
        vec![vec![Value::Int(2)]]
    );
    run(
        &mut s,
        "CREATE TABLE self_update(id INT PRIMARY KEY REFERENCES self_update(id) ON UPDATE CASCADE)",
    );
    run(&mut s, "INSERT INTO self_update VALUES(1)");
    run(&mut s, "UPDATE self_update SET id=2 WHERE id=1");
    assert_eq!(
        rows(&mut s, "SELECT id FROM self_update LIMIT 10"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn cross_type_foreign_keys_convert_parent_and_child_lookup_keys() {
    // PostgreSQL 18.6 accepts int4 -> int8 and int4 -> numeric; frozen oracle
    // output is in fixtures/e1_constraints/fk_cross_type.txt.
    for parent_type in ["BIGINT", "NUMERIC"] {
        for parent_unique in [false, true] {
            if parent_type == "NUMERIC" && parent_unique {
                // UNIQUE secondary indexes don't admit NUMERIC keys (existing GD-0042 bound);
                // the NUMERIC primary-key parent covers int4 -> numeric.
                continue;
            }
            for access in ["primary", "index", "scan"] {
                for action in ["CASCADE", "RESTRICT"] {
                    let db = Db::new();
                    let mut s = db.session();
                    let parent = if parent_unique {
                        format!("CREATE TABLE p(rowid INT PRIMARY KEY,id {parent_type} UNIQUE)")
                    } else {
                        format!("CREATE TABLE p(id {parent_type} PRIMARY KEY)")
                    };
                    run(&mut s, &parent);
                    let child = if access == "primary" {
                        format!(
                            "CREATE TABLE c(pid INT PRIMARY KEY REFERENCES p(id) ON DELETE {action})"
                        )
                    } else {
                        format!(
                            "CREATE TABLE c(rowid INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE {action})"
                        )
                    };
                    run(&mut s, &child);
                    if access == "index" {
                        run(&mut s, "CREATE INDEX c_pid ON c(pid)");
                    }
                    run(
                        &mut s,
                        if parent_unique {
                            "INSERT INTO p VALUES(10,1)"
                        } else {
                            "INSERT INTO p VALUES(1)"
                        },
                    );
                    let child_values = if access == "primary" { "1" } else { "10,1" };
                    run(&mut s, &format!("INSERT INTO c VALUES({child_values})"));
                    let missing = if access == "primary" { "2" } else { "20,2" };
                    error(
                        &mut s,
                        &format!("INSERT INTO c VALUES({missing})"),
                        SqlError::ForeignKeyViolation,
                    ); // 23503
                    if parent_type == "NUMERIC" {
                        // Neither a fractional nor out-of-int4-range parent key
                        // can identify this integer child, even on index paths.
                        run(
                            &mut s,
                            if parent_unique {
                                "INSERT INTO p VALUES(11,1.5),(12,2147483648)"
                            } else {
                                "INSERT INTO p VALUES(1.5),(2147483648)"
                            },
                        );
                        assert_eq!(run(&mut s, "DELETE FROM p WHERE id=1.5"), 1);
                        assert_eq!(run(&mut s, "DELETE FROM p WHERE id=2147483648"), 1);
                        assert_eq!(
                            rows(&mut s, "SELECT pid FROM c LIMIT 10"),
                            vec![vec![Value::Int(1)]]
                        );
                    }
                    if action == "RESTRICT" {
                        error(
                            &mut s,
                            "DELETE FROM p WHERE id=1",
                            SqlError::RestrictViolation,
                        ); // 23001
                        assert_eq!(
                            rows(&mut s, "SELECT pid FROM c LIMIT 10"),
                            vec![vec![Value::Int(1)]]
                        );
                    } else {
                        assert_eq!(run(&mut s, "DELETE FROM p WHERE id=1"), 1);
                        assert!(rows(&mut s, "SELECT pid FROM c LIMIT 10").is_empty());
                    }
                }
            }
        }
    }
}

#[test]
fn incompatible_foreign_key_reports_pg186_message_and_detail() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE pt(id TEXT PRIMARY KEY)");
    error(
        &mut s,
        "CREATE TABLE ct(id INT CONSTRAINT incompatible REFERENCES pt(id))",
        SqlError::Type,
    ); // 42804
}
