//! PostgreSQL 18.6 ALTER TABLE qualification (oracle: e1-sql/tmp/alter-tests/oracle*.txt).
//! Oracle SQLSTATEs: Duplicate=23505, Null=23502, DuplicateColumn=42701,
//! UnknownColumn=42703, UnknownTable=42P01, DuplicateTable=42P07,
//! DuplicateObject=42710, InvalidTableDefinition=42P16, Overflow=22003.
//! Bounded engine rules: Unsupported=0A000, Limit=54000, Reserved=0A000.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, StatementClock, Value},
};
use giggles_pg_types::temporal::{SessionZone, TimestampTz};
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

struct Db {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Db {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::path::Path::new("J:/gigglesdb-work/e1-sql/tmp/alter-finish-reopen")
            .join(format!("alter-{:032x}", u128::from_le_bytes(nonce)));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn reopen(&mut self) {
        drop(self.admin.take());
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
    // Every supported ALTER kind invalidates prepared handles and survives reopen.
    fn alter(&mut self, sql: &str, table: &str, after: &str) {
        let mut s = self.admin().sql_session();
        let prepared = s.prepare(&format!("SELECT * FROM {table}")).unwrap();
        run(&mut s, sql);
        assert!(
            matches!(
                s.execute_prepared(&prepared, &[], None),
                Err(CoreError::Sql(SqlError::StaleSchema))
            ),
            "{sql}"
        );
        let expected = rows(&mut s, after);
        let catalog = self.admin().catalog().unwrap();
        let typmods = self.admin().catalog_typmods().unwrap();
        let sequence_owners = s.sequence_owners().unwrap();
        drop(s);
        self.reopen();
        assert_eq!(
            self.admin().sql_session().sequence_owners().unwrap(),
            sequence_owners,
            "{sql}: sequence owners"
        );
        assert_eq!(self.admin().catalog().unwrap(), catalog, "{sql}: catalog");
        assert_eq!(
            self.admin().catalog_typmods().unwrap(),
            typmods,
            "{sql}: typmods"
        );
        assert_eq!(
            rows(&mut self.admin().sql_session(), after),
            expected,
            "{sql}"
        );
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn fixed_clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: Some(SessionZone::Fixed(0)),
        wall: Some(Arc::new(|| Ok(0))),
    }
}
fn run(s: &mut SqlSession, sql: &str) {
    s.execute_at(sql, &[], None, &fixed_clock())
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute_at(sql, &[], None, &fixed_clock())
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn error(s: &mut SqlSession, sql: &str, expected: SqlError) {
    assert!(
        matches!(s.execute_at(sql,&[],None,&fixed_clock()),Err(CoreError::Sql(e)) if e==expected),
        "{sql}: expected {expected:?}"
    );
}

#[test]
fn all_alter_kinds_invalidate_prepared_handles_and_persist() {
    let mut db = Db::new();
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE TABLE t (id int PRIMARY KEY, old_name text)");
        run(&mut s, "INSERT INTO t VALUES (1,'old')");
    }
    db.alter(
        "ALTER TABLE t ADD COLUMN score int DEFAULT 7",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    db.alter(
        "ALTER TABLE t ALTER COLUMN score SET DEFAULT 8",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "INSERT INTO t (id) VALUES (2)");
        assert_eq!(
            rows(&mut s, "SELECT score FROM t ORDER BY id"),
            vec![vec![Value::Int(7)], vec![Value::Int(8)]]
        );
    }
    db.alter(
        "ALTER TABLE t ALTER COLUMN score DROP DEFAULT",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "INSERT INTO t (id) VALUES (3)");
        assert_eq!(
            rows(&mut s, "SELECT score FROM t WHERE id=3"),
            vec![vec![Value::Null]]
        );
    }
    db.alter(
        "ALTER TABLE t ALTER COLUMN old_name DROP NOT NULL",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "UPDATE t SET old_name='new' WHERE id>=2");
    }
    db.alter(
        "ALTER TABLE t ALTER COLUMN old_name SET NOT NULL",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    db.alter(
        "ALTER TABLE t RENAME COLUMN old_name TO label",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    db.alter(
        "ALTER TABLE t ADD CONSTRAINT uq_score UNIQUE(score)",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        error(
            &mut s,
            "INSERT INTO t VALUES (4,'x',7)",
            SqlError::Duplicate,
        );
    }
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE INDEX score_lookup ON t(score)");
    }
    db.alter(
        "ALTER TABLE t DROP COLUMN score",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE INDEX score_lookup ON t(id)");
        run(&mut s, "INSERT INTO t VALUES (4,'x')");
        assert_eq!(db.admin().catalog().unwrap().1[0].columns.len(), 2);
    }
    db.alter(
        "ALTER TABLE t RENAME TO renamed",
        "t",
        "SELECT * FROM renamed ORDER BY id",
    );
    let mut s = db.admin().sql_session();
    error(&mut s, "SELECT * FROM t", SqlError::UnknownTable);
}

#[test]
fn heap_add_drop_rename_and_unique_constraint() {
    let mut db = Db::new();
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE TABLE h (v text)");
        run(&mut s, "INSERT INTO h VALUES ('a'),('b')");
    }
    db.alter(
        "ALTER TABLE h ADD COLUMN n int DEFAULT 9",
        "h",
        "SELECT v,n FROM h ORDER BY v",
    );
    db.alter(
        "ALTER TABLE h RENAME COLUMN n TO score",
        "h",
        "SELECT v,score FROM h ORDER BY v",
    );
    db.alter(
        "ALTER TABLE h ADD UNIQUE(v)",
        "h",
        "SELECT * FROM h ORDER BY v",
    );
    db.alter(
        "ALTER TABLE h DROP COLUMN score",
        "h",
        "SELECT * FROM h ORDER BY v",
    );
    db.alter(
        "ALTER TABLE h RENAME TO heap_renamed",
        "h",
        "SELECT * FROM heap_renamed ORDER BY v",
    );
    let mut s = db.admin().sql_session();
    error(
        &mut s,
        "INSERT INTO heap_renamed VALUES ('a')",
        SqlError::Duplicate,
    );
}

#[test]
fn add_defaults_null_typmods_and_volatile_backfill() {
    let mut db = Db::new();
    let mut s = db.admin().sql_session();
    run(&mut s, "CREATE TABLE t (id int PRIMARY KEY)");
    run(&mut s, "INSERT INTO t VALUES (1),(2)");
    run(&mut s, "CREATE TABLE tickets(id serial PRIMARY KEY)");
    run(
        &mut s,
        "ALTER TABLE t ADD COLUMN ticket bigint DEFAULT nextval('tickets_id_seq')",
    );
    assert_eq!(
        rows(&mut s, "SELECT ticket FROM t ORDER BY ticket"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    let calls = Arc::new(AtomicI64::new(100));
    let wall = calls.clone();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 10,
        statement_us: 20,
        zone: Some(SessionZone::Fixed(0)),
        wall: Some(Arc::new(move || Ok(wall.fetch_add(1, Ordering::SeqCst)))),
    };
    s.execute_at(
        "ALTER TABLE t ADD COLUMN stamp timestamptz DEFAULT clock_timestamp()",
        &[],
        None,
        &clock,
    )
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 102);
    assert_eq!(
        rows(&mut s, "SELECT stamp FROM t ORDER BY stamp"),
        (100..102)
            .map(|v| vec![Value::TimestampTz(
                TimestampTz::from_microseconds(v).unwrap()
            )])
            .collect::<Vec<_>>()
    );
    run(&mut s, "ALTER TABLE t ADD COLUMN missing int");
    run(
        &mut s,
        "ALTER TABLE t ADD COLUMN explicit_null int DEFAULT NULL",
    );
    assert_eq!(
        rows(&mut s, "SELECT missing,explicit_null FROM t ORDER BY id"),
        vec![vec![Value::Null, Value::Null]; 2]
    );
    run(
        &mut s,
        "ALTER TABLE t ADD COLUMN rounded numeric(4,2) DEFAULT 1.235",
    );
    let rounded = Value::Numeric(giggles_core::relational::Numeric::parse("1.24").unwrap());
    assert_eq!(
        rows(&mut s, "SELECT rounded FROM t ORDER BY id"),
        vec![vec![rounded.clone()]; 2]
    );
    error(
        &mut s,
        "ALTER TABLE t ADD COLUMN too_large numeric(4,2) DEFAULT 1000",
        SqlError::Overflow,
    );
    error(
        &mut s,
        "ALTER TABLE t ADD COLUMN required int NOT NULL",
        SqlError::Null,
    );
    drop(s);
    db.reopen();
    let mut s = db.admin().sql_session();
    assert_eq!(
        rows(&mut s, "SELECT rounded FROM t ORDER BY id"),
        vec![vec![rounded.clone()]; 2]
    );
    let inserted = rows(
        &mut s,
        "INSERT INTO t(id) VALUES(3) RETURNING rounded,ticket",
    );
    assert_eq!(inserted[0][0], rounded);
    let Value::Int(ticket) = inserted[0][1] else {
        panic!("integer ticket")
    };
    // Durable sequence reservations can leave gaps across reopen.
    assert!(ticket > 2);
    run(&mut s, "ALTER TABLE t RENAME COLUMN ticket TO receipt");
    run(&mut s, "ALTER TABLE t RENAME TO renamed");
    assert_eq!(
        rows(&mut s, "SELECT nextval('tickets_id_seq')"),
        vec![vec![Value::Int(ticket + 1)]]
    );
}

#[test]
fn failures_are_atomic_and_validate_names_nullability_and_uniqueness() {
    let db = Db::new();
    let mut s = db.admin().sql_session();
    run(
        &mut s,
        "CREATE TABLE t (id int PRIMARY KEY,v text,score int)",
    );
    run(&mut s, "INSERT INTO t VALUES (1,'same',NULL),(2,'same',2)");
    let before = db.admin().catalog().unwrap();
    let values = rows(&mut s, "SELECT * FROM t ORDER BY id");
    for (sql, expected) in [
        ("ALTER TABLE t ADD COLUMN v int", SqlError::DuplicateColumn),
        (
            "ALTER TABLE t RENAME COLUMN v TO id",
            SqlError::DuplicateColumn,
        ),
        (
            "ALTER TABLE t ALTER COLUMN score SET NOT NULL",
            SqlError::Null,
        ),
        (
            "ALTER TABLE t ADD CONSTRAINT uq UNIQUE(v)",
            SqlError::Duplicate,
        ),
        (
            "ALTER TABLE t ADD COLUMN added int DEFAULT 9, ADD COLUMN id int",
            SqlError::DuplicateColumn,
        ),
        ("ALTER TABLE t DROP COLUMN absent", SqlError::UnknownColumn),
        (
            "ALTER TABLE t RENAME COLUMN absent TO other",
            SqlError::UnknownColumn,
        ),
        (
            "ALTER TABLE t ALTER COLUMN absent SET DEFAULT 1",
            SqlError::UnknownColumn,
        ),
        (
            "ALTER TABLE t DROP COLUMN score, ALTER COLUMN v SET DEFAULT 'changed', ADD COLUMN id int",
            SqlError::DuplicateColumn,
        ),
        (
            "ALTER TABLE absent ADD COLUMN x int",
            SqlError::UnknownTable,
        ),
    ] {
        error(&mut s, sql, expected);
        assert_eq!(db.admin().catalog().unwrap(), before, "{sql}");
        assert_eq!(rows(&mut s, "SELECT * FROM t ORDER BY id"), values, "{sql}");
    }
    error(
        &mut s,
        "ALTER TABLE t ALTER COLUMN id DROP NOT NULL",
        SqlError::InvalidTableDefinition,
    );
    run(&mut s, "CREATE TABLE occupied (id int)");
    error(
        &mut s,
        "ALTER TABLE t RENAME TO occupied",
        SqlError::DuplicateTable,
    );
    run(&mut s, "ALTER TABLE t ADD CONSTRAINT uq UNIQUE(score)");
    error(
        &mut s,
        "ALTER TABLE t ADD CONSTRAINT uq UNIQUE(id)",
        SqlError::DuplicateTable,
    );
}

#[test]
fn bounded_rejections_and_column_limits() {
    let db = Db::new();
    let mut s = db.admin().sql_session();
    run(&mut s, "CREATE TABLE t (id int PRIMARY KEY,v int)");
    run(&mut s, "CREATE TABLE heap (only_column int)");
    // Deliberate bounded 0A000 rules: PG permits dropping a key column or the last user column.
    for sql in [
        "ALTER TABLE t DROP COLUMN id",
        "ALTER TABLE heap DROP COLUMN only_column",
        "ALTER TABLE t ALTER COLUMN v TYPE bigint",
        "ALTER TABLE t ADD COLUMN serial_col serial",
    ] {
        error(&mut s, sql, SqlError::Unsupported);
    }
    error(
        &mut s,
        "ALTER TABLE t ADD PRIMARY KEY(v)",
        SqlError::InvalidTableDefinition,
    );
    run(&mut s, "ALTER TABLE t ADD CHECK(v>0)");
    run(&mut s, "ALTER TABLE t ADD FOREIGN KEY(v) REFERENCES t(id)");
    error(
        &mut s,
        "ALTER TABLE t ADD COLUMN giggles_rowid int",
        SqlError::Reserved,
    );
    error(
        &mut s,
        "ALTER TABLE heap RENAME COLUMN only_column TO giggles_rowid",
        SqlError::Reserved,
    );
    error(
        &mut s,
        "ALTER TABLE heap DROP COLUMN giggles_rowid",
        SqlError::Reserved,
    );
    let columns = (0..64)
        .map(|i| format!("c{i} int{}", if i == 0 { " PRIMARY KEY" } else { "" }))
        .collect::<Vec<_>>()
        .join(",");
    run(&mut s, &format!("CREATE TABLE wide ({columns})"));
    error(
        &mut s,
        "ALTER TABLE wide ADD COLUMN overflow int",
        SqlError::Limit,
    );
}

#[test]
fn owned_sequence_names_survive_column_and_table_rename() {
    let mut db = Db::new();
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE TABLE owner (id serial PRIMARY KEY,v int)");
        run(&mut s, "INSERT INTO owner(v) VALUES(7)");
    }
    db.alter(
        "ALTER TABLE owner RENAME COLUMN id TO renamed_id",
        "owner",
        "SELECT * FROM owner",
    );
    db.alter(
        "ALTER TABLE owner RENAME TO renamed_owner",
        "owner",
        "SELECT * FROM renamed_owner",
    );
    let mut s = db.admin().sql_session();
    let inserted = rows(
        &mut s,
        "INSERT INTO renamed_owner(v) VALUES(8) RETURNING renamed_id",
    );
    let Value::Int(id) = inserted[0][0] else {
        panic!("integer identity")
    };
    // Reopening may discard the remainder of a durable reservation.
    assert!(id > 1);
    assert_eq!(
        rows(&mut s, "SELECT nextval('owner_id_seq')"),
        vec![vec![Value::Int(id + 1)]]
    );
}

#[test]
fn multiple_actions_indexes_and_reopened_constraint_metadata() {
    let mut db = Db::new();
    {
        let mut s = db.admin().sql_session();
        run(
            &mut s,
            "CREATE TABLE t(id int PRIMARY KEY,obsolete int,kept int)",
        );
        run(&mut s, "INSERT INTO t VALUES(1,8,9)");
        run(&mut s, "CREATE UNIQUE INDEX kept_idx ON t(kept)");
    }
    db.alter("ALTER TABLE t DROP COLUMN obsolete, ADD COLUMN added int DEFAULT 7, ALTER COLUMN kept SET NOT NULL","t","SELECT * FROM t");
    {
        let mut s = db.admin().sql_session();
        error(
            &mut s,
            "INSERT INTO t(id,kept) VALUES(2,9)",
            SqlError::Duplicate,
        ); // 23505
        error(
            &mut s,
            "INSERT INTO t(id,kept) VALUES(2,NULL)",
            SqlError::Null,
        ); // 23502
        assert_eq!(
            rows(
                &mut s,
                "INSERT INTO t(id,kept) VALUES(2,10) RETURNING added"
            ),
            vec![vec![Value::Int(7)]]
        );
        run(&mut s, "ALTER TABLE t DROP COLUMN IF EXISTS absent");
        let before = rows(&mut s, "SELECT * FROM t ORDER BY id");
        let catalog = db.admin().catalog().unwrap();
        error(
            &mut s,
            "ALTER TABLE t DROP COLUMN kept, ALTER COLUMN added SET DEFAULT 99, ADD COLUMN id int",
            SqlError::DuplicateColumn,
        ); // 42701
        assert_eq!(rows(&mut s, "SELECT * FROM t ORDER BY id"), before);
        assert_eq!(db.admin().catalog().unwrap(), catalog);
        error(
            &mut s,
            "INSERT INTO t(id,kept) VALUES(3,9)",
            SqlError::Duplicate,
        );
        assert_eq!(
            rows(
                &mut s,
                "INSERT INTO t(id,kept) VALUES(3,11) RETURNING added"
            ),
            vec![vec![Value::Int(7)]]
        );
    }
}

#[test]
fn added_backfill_obeys_row_byte_limit_atomically() {
    let db = Db::new();
    let mut s = db.admin().sql_session();
    run(&mut s, "CREATE TABLE t(id int PRIMARY KEY,v text)");
    run(
        &mut s,
        &format!("INSERT INTO t VALUES(1,'{}')", "x".repeat(40_000)),
    );
    let before = db.admin().catalog().unwrap();
    error(
        &mut s,
        &format!(
            "ALTER TABLE t ADD COLUMN extra text DEFAULT '{}'",
            "y".repeat(40_000)
        ),
        SqlError::Limit,
    );
    assert_eq!(db.admin().catalog().unwrap(), before);
    assert_eq!(rows(&mut s, "SELECT id FROM t"), vec![vec![Value::Int(1)]]);
}

#[test]
fn reopened_column_ids_extensions_and_indexes_follow_each_alter() {
    let mut db = Db::new();
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE TABLE t(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO t VALUES(1),(2)");
    }
    db.alter(
        "ALTER TABLE t ADD COLUMN obsolete int DEFAULT 8",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    db.alter(
        "ALTER TABLE t ADD COLUMN kept int",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "UPDATE t SET kept=id*10");
        run(&mut s, "CREATE INDEX kept_lookup ON t(kept)");
    }
    db.alter(
        "ALTER TABLE t ADD CONSTRAINT kept_unique UNIQUE(kept)",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    let original = db.admin().catalog().unwrap().1.remove(0);
    let removed_id = original.columns[1].id;
    let kept_id = original.columns[2].id;
    db.alter(
        "ALTER TABLE t DROP COLUMN obsolete",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    assert!(
        !db.admin()
            .catalog_typmods()
            .unwrap()
            .1
            .contains_key(&removed_id)
    );
    assert_eq!(db.admin().catalog().unwrap().1[0].columns[1].id, kept_id);
    db.alter(
        "ALTER TABLE t ADD COLUMN added int DEFAULT 1",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    let added_id = db.admin().catalog().unwrap().1[0].columns[2].id;
    assert!(added_id > kept_id);
    assert_ne!(added_id, removed_id);

    {
        let mut s = db.admin().sql_session();
        assert_eq!(
            rows(&mut s, "SELECT added FROM t ORDER BY id"),
            vec![vec![Value::Int(1)]; 2]
        );
        error(
            &mut s,
            "INSERT INTO t(id,kept) VALUES(3,10)",
            SqlError::Duplicate,
        );
        run(&mut s, "INSERT INTO t(id,kept) VALUES(3,30)");
        assert_eq!(
            rows(&mut s, "SELECT added FROM t WHERE id=3"),
            vec![vec![Value::Int(1)]]
        );
    }
    db.alter(
        "ALTER TABLE t ALTER COLUMN added SET DEFAULT 2",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        assert_eq!(
            rows(
                &mut s,
                "INSERT INTO t(id,kept) VALUES(4,40) RETURNING added"
            ),
            vec![vec![Value::Int(2)]]
        );
    }
    db.alter(
        "ALTER TABLE t ALTER COLUMN added DROP DEFAULT",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        assert_eq!(
            rows(
                &mut s,
                "INSERT INTO t(id,kept) VALUES(5,50) RETURNING added"
            ),
            vec![vec![Value::Null]]
        );
    }
    db.alter(
        "ALTER TABLE t ALTER COLUMN kept SET NOT NULL",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        error(
            &mut s,
            "INSERT INTO t(id,kept) VALUES(6,NULL)",
            SqlError::Null,
        );
    }
    db.alter(
        "ALTER TABLE t ALTER COLUMN kept DROP NOT NULL",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "INSERT INTO t(id,kept) VALUES(6,NULL)");
    }
    db.alter(
        "ALTER TABLE t RENAME COLUMN kept TO renamed_key",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    assert_eq!(db.admin().catalog().unwrap().1[0].columns[1].id, kept_id);
    {
        let mut s = db.admin().sql_session();
        error(&mut s, "SELECT kept FROM t", SqlError::UnknownColumn);
        assert_eq!(
            rows(&mut s, "SELECT id FROM t WHERE renamed_key=20"),
            vec![vec![Value::Int(2)]]
        );
        error(
            &mut s,
            "INSERT INTO t(id,renamed_key) VALUES(7,20)",
            SqlError::Duplicate,
        );
        run(&mut s, "UPDATE t SET renamed_key=21 WHERE id=2");
        assert!(rows(&mut s, "SELECT id FROM t WHERE renamed_key=20").is_empty());
        assert_eq!(
            rows(&mut s, "SELECT id FROM t WHERE renamed_key=21"),
            vec![vec![Value::Int(2)]]
        );
    }
    db.alter(
        "ALTER TABLE t RENAME TO renamed",
        "t",
        "SELECT * FROM renamed ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        error(&mut s, "SELECT * FROM t", SqlError::UnknownTable);
        error(
            &mut s,
            "INSERT INTO renamed(id,renamed_key) VALUES(7,21)",
            SqlError::Duplicate,
        );
        assert_eq!(
            rows(&mut s, "SELECT id FROM renamed WHERE renamed_key=21"),
            vec![vec![Value::Int(2)]]
        );
    }
    db.alter(
        "ALTER TABLE renamed DROP COLUMN renamed_key",
        "renamed",
        "SELECT * FROM renamed ORDER BY id",
    );
    {
        let mut s = db.admin().sql_session();
        // Both relation names are reusable only if both persisted indexes were removed.
        run(&mut s, "CREATE INDEX kept_lookup ON renamed(id)");
        run(&mut s, "CREATE UNIQUE INDEX kept_unique ON renamed(id)");
        error(
            &mut s,
            "SELECT renamed_key FROM renamed",
            SqlError::UnknownColumn,
        );
    }
}

#[test]
fn reopened_typmod_entries_follow_dropped_and_added_column_ids() {
    let mut db = Db::new();
    {
        let mut s = db.admin().sql_session();
        run(&mut s, "CREATE TABLE t(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO t VALUES(1),(2)");
    }
    db.alter(
        "ALTER TABLE t ADD COLUMN obsolete numeric(8,0) DEFAULT 8",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    let obsolete_id = db.admin().catalog().unwrap().1[0].columns[1].id;
    assert!(
        db.admin()
            .catalog_typmods()
            .unwrap()
            .1
            .contains_key(&obsolete_id)
    );
    db.alter(
        "ALTER TABLE t DROP COLUMN obsolete",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    assert!(
        !db.admin()
            .catalog_typmods()
            .unwrap()
            .1
            .contains_key(&obsolete_id)
    );
    db.alter(
        "ALTER TABLE t ADD COLUMN added numeric(4,2) DEFAULT 1.25",
        "t",
        "SELECT * FROM t ORDER BY id",
    );
    let added_id = db.admin().catalog().unwrap().1[0].columns[1].id;
    assert!(added_id > obsolete_id);
    assert!(
        db.admin()
            .catalog_typmods()
            .unwrap()
            .1
            .contains_key(&added_id)
    );
    let expected = Value::Numeric(giggles_core::relational::Numeric::parse("1.25").unwrap());
    let mut s = db.admin().sql_session();
    assert_eq!(
        rows(&mut s, "SELECT added FROM t ORDER BY id"),
        vec![vec![expected.clone()]; 2]
    );
    assert_eq!(
        rows(&mut s, "INSERT INTO t(id) VALUES(3) RETURNING added"),
        vec![vec![expected]]
    );
}
