use super::*;
use engine::{
    catalog_codec, codec,
    hlc::Ts,
    kv::{Direction, Durability, Keyspace, WriteBatch},
    mvcc,
};
use std::ops::Bound;

struct Fixture {
    path: std::path::PathBuf,
    admin: Option<Admin>,
}
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("v2-sql-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        Self {
            path,
            admin: Some(admin),
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn db(&self) -> &v2::V2Db {
        match &self.admin().inner.backend {
            Backend::V2(db) => db,
            _ => unreachable!(),
        }
    }
    fn reopen(&mut self) {
        self.admin.take();
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
    fn parts(&self) -> CatalogParts {
        let mut txn = self.db().begin(Isolation::ReadCommitted);
        self.db().catalog().parts(&mut txn).unwrap()
    }
    fn catalog_bytes(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db()
            .kv
            .snapshot()
            .unwrap()
            .range(
                Keyspace::Catalog,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
            )
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
    fn execute(&self, sql: &str) {
        self.admin()
            .sql_session()
            .execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        fs::remove_dir_all(&self.path).unwrap();
    }
}

#[test]
fn ddl_defaults_and_renames_persist() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v text DEFAULT 'x')");
    let original = f.parts();
    let n = original.tables["t"].columns[1].id;
    assert_eq!(
        original.ext.columns[&n].default.as_deref(),
        Some("'x'::text")
    );
    f.reopen();
    assert_eq!(f.parts(), original);
    f.execute("ALTER TABLE t RENAME COLUMN v TO w");
    f.execute("ALTER TABLE t RENAME TO u");
    let renamed = f.parts();
    assert_eq!(renamed.tables["u"].columns[1].name, "w");
    let id = renamed.tables["u"].id;
    assert_eq!(renamed.ext.constraints[&id][0].name, "t_pkey");
    f.reopen();
    assert_eq!(f.parts(), renamed);
    f.execute("ALTER TABLE u ALTER COLUMN w SET DEFAULT 'y'");
    assert_eq!(
        f.parts().ext.columns[&n].default.as_deref(),
        Some("'y'::text")
    );
    f.reopen();
    assert_eq!(
        f.parts().ext.columns[&n].default.as_deref(),
        Some("'y'::text")
    );
    f.execute("ALTER TABLE u ALTER COLUMN w DROP DEFAULT");
    assert!(
        f.parts()
            .ext
            .columns
            .get(&n)
            .is_none_or(|v| v.default.is_none())
    );
    let dropped_default = f.parts();
    f.reopen();
    assert_eq!(f.parts(), dropped_default);
}

#[test]
fn serial_catalog_and_owned_counter_are_removed_on_drop() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id serial PRIMARY KEY)");
    let parts = f.parts();
    let (&id, sequence) = parts.ext.sequences.iter().next().unwrap();
    assert_eq!(parts.ext.sequences.len(), 1);
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    assert!(
        mvcc::read_at(
            f.db().kv.as_ref(),
            Keyspace::Catalog,
            &catalog_codec::sequence_key(id),
            txn.snapshot_ts()
        )
        .unwrap()
        .is_some()
    );
    drop(txn);
    let features = f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
    assert_ne!(
        u64::from_be_bytes(features.try_into().unwrap()) & (1 << 6),
        0
    );
    let counter = catalog_codec::counter_key(id, sequence.incarnation);
    // Counters are lazy; seed an owned counter to prove DROP removes it.
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Seq,
        counter.clone(),
        catalog_codec::encode_counter(
            &sequences::SeqCounter {
                incarnation: sequence.incarnation,
                last_value: sequence.start,
                is_called: false,
                log_hw: sequence.start,
                stamp: 0,
            },
            sequence,
        )
        .unwrap(),
    );
    engine::meta::put_features(&mut batch, (1 << 6) | (1 << 8)).unwrap();
    f.db().kv.write(batch, Durability::Sync).unwrap();
    assert!(f.db().kv.get(Keyspace::Seq, &counter).unwrap().is_some());
    f.reopen();
    assert_eq!(f.parts(), parts);
    f.execute("DROP TABLE t");
    assert!(f.parts().tables.is_empty());
    assert!(f.parts().ext.sequences.is_empty());
    assert!(f.db().kv.get(Keyspace::Seq, &counter).unwrap().is_none());
    f.reopen();
    assert!(f.parts().tables.is_empty());
}

#[test]
fn explicit_commit_rollback_and_failure_abort() {
    let mut f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    assert!(s.in_transaction());
    s.execute("CREATE TABLE rolled (id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(f.parts().tables.is_empty());
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("CREATE TABLE committed (id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    s.execute("COMMIT", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("CREATE TABLE failed (id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    assert!(matches!(
        s.execute("CREATE TABLE failed (id bigint PRIMARY KEY)", &[], None),
        Err(CoreError::Sql(_))
    ));
    assert!(s.is_aborted());
    assert!(matches!(
        s.execute("COMMIT", &[], None),
        Err(CoreError::Sql(relational::SqlError::Aborted))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(
        f.parts()
            .tables
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["committed"]
    );
    drop(s);
    f.reopen();
    assert!(f.parts().tables.contains_key("committed"));
    assert!(!f.parts().tables.contains_key("failed"));
}

#[test]
fn implicit_failure_and_refusals_leave_catalog_unchanged() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY)");
    let before = f.parts();
    let bytes = f.catalog_bytes();
    assert!(matches!(
        f.admin()
            .sql_session()
            .execute("CREATE TABLE t (id bigint PRIMARY KEY)", &[], None),
        Err(CoreError::Sql(_))
    ));
    assert_eq!(f.parts(), before);
    assert_eq!(f.catalog_bytes(), bytes);
    for (sql, feature) in [
        ("CREATE INDEX idx ON t (id)", "secondary indexes"),
        (
            "CREATE TABLE u (id bigint PRIMARY KEY, n bigint UNIQUE)",
            "secondary indexes",
        ),
        (
            "ALTER TABLE t ADD COLUMN n bigint",
            "ALTER TABLE row rewrite",
        ),
        ("INSERT INTO t VALUES (1)", SQL_DML),
        ("SELECT * FROM t", SQL_QUERIES),
    ] {
        assert!(
            matches!(f.admin().sql_session().execute(sql, &[], None),
            Err(CoreError::EngineUnsupported { engine: "v2", feature: got }) if got == feature),
            "{sql}"
        );
        assert_eq!(f.parts(), before, "{sql}");
        assert_eq!(f.catalog_bytes(), bytes, "{sql}");
    }
}

#[test]
fn invalid_stored_constraint_fails_image_validation_without_catalog_writes() {
    use relational::catalog_ext::{Action, Constraint, ConstraintKind, Deferral};
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY)");
    let parts = f.parts();
    let table = &parts.tables["t"];
    let ids = f.db().catalog().reserve_ids(1).unwrap();
    let constraint = Constraint {
        id: ids.start,
        name: "broken_fk".into(),
        kind: ConstraintKind::ForeignKey {
            columns: vec![table.columns[0].id],
            ref_table: table.id,
            ref_columns: vec![u64::MAX],
            on_delete: Action::NoAction,
            on_update: Action::NoAction,
            not_valid: false,
            deferral: Deferral::NotDeferrable,
        },
    };
    let ts = engine::meta::open_checks(f.db().kv.as_ref())
        .unwrap()
        .last_commit_ts
        .unwrap();
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Catalog,
        codec::user_key_seek_at(
            &catalog_codec::constraint_key(table.id, constraint.id),
            Ts(ts),
        ),
        mvcc::encode_live(&catalog_codec::encode_constraint(&constraint, "t").unwrap()),
    );
    engine::meta::put_features(&mut batch, 1 << 6).unwrap();
    f.db().kv.write(batch, Durability::Sync).unwrap();
    f.reopen();
    let before = f.parts();
    let bytes = f.catalog_bytes();
    assert!(matches!(
        f.admin()
            .sql_session()
            .execute("CREATE TABLE x (id bigint PRIMARY KEY)", &[], None),
        Err(CoreError::Corrupt(_))
    ));
    assert_eq!(f.parts(), before);
    assert_eq!(f.catalog_bytes(), bytes);
}

#[test]
fn check_constraint_is_refused_without_catalog_changes() {
    let f = Fixture::new();
    f.execute("CREATE TABLE q (id bigint PRIMARY KEY, n bigint)");
    // The classifier refuses these statements before the post-image gate is
    // reachable. Direct check_post_image tests below exercise that gate; this
    // SQL witness proves the earlier refusal leaves catalog bytes untouched.
    // Every DDL form that would create an unenforced CHECK or FOREIGN KEY is
    // refused with 0A000 and leaves the catalog byte-identical (DD-0068 X3 #2).
    for sql in [
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint CHECK (n > 0))",
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint, CHECK (n > 0))",
        "CREATE TABLE t (id bigint PRIMARY KEY, p bigint REFERENCES q(id))",
        "CREATE TABLE t (id bigint PRIMARY KEY, p bigint, FOREIGN KEY (p) REFERENCES q(id))",
        "ALTER TABLE q ADD CHECK (n > 0)",
        "ALTER TABLE q ADD CONSTRAINT q_n_fkey FOREIGN KEY (n) REFERENCES q(id)",
    ] {
        let before = f.parts();
        let bytes = f.catalog_bytes();
        assert!(
            matches!(
                f.admin().sql_session().execute(sql, &[], None),
                Err(CoreError::EngineUnsupported {
                    engine: "v2",
                    feature: "SQL statement"
                })
            ),
            "{sql}"
        );
        assert_eq!(f.parts(), before, "{sql}");
        assert_eq!(f.catalog_bytes(), bytes, "{sql}");
    }
}

fn created_catalog(
    sql: &str,
) -> (
    BTreeMap<String, relational::TableDef>,
    relational::catalog_ext::CatalogExt,
    u64,
    u64,
) {
    let mut image = relational::Relational::default();
    let prepared = relational::Prepared::parse(sql, 0, 0).unwrap();
    image.execute(&prepared, &[], None).unwrap();
    image.catalog_parts()
}

#[test]
fn post_image_invalid_constraint_is_corrupt() {
    use relational::catalog_ext::ConstraintKind;
    let (tables, mut ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, p bigint REFERENCES t(id))");
    let constraints = ext.constraints.get_mut(&tables["t"].id).unwrap();
    let foreign_key = constraints
        .iter_mut()
        .find_map(|constraint| {
            if let ConstraintKind::ForeignKey { columns, .. } = &mut constraint.kind {
                Some(columns)
            } else {
                None
            }
        })
        .unwrap();
    foreign_key[0] = u64::MAX;
    assert!(matches!(
        check_post_image(&tables, &ext, next_id),
        Err(CoreError::Corrupt(message)) if message.starts_with("v2 DDL produced an invalid catalog: ")
    ));
}

#[test]
fn post_image_check_is_unsupported() {
    let (tables, ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, n bigint CHECK (n > 0))");
    assert!(matches!(
        check_post_image(&tables, &ext, next_id),
        Err(CoreError::EngineUnsupported {
            engine: "v2",
            feature: "SQL statement"
        })
    ));
}

#[test]
fn post_image_varchar_is_unsupported() {
    let (tables, ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, v varchar(10))");
    assert!(matches!(
        check_post_image(&tables, &ext, next_id),
        Err(CoreError::EngineUnsupported {
            engine: "v2",
            feature: "SQL statement"
        })
    ));
}

#[test]
fn post_image_primary_key_is_valid() {
    let (tables, ext, next_id, _) = created_catalog("CREATE TABLE t (id bigint PRIMARY KEY)");
    check_post_image(&tables, &ext, next_id).unwrap();
}

#[test]
fn varchar_char_refusals_leave_catalog_bytes_unchanged() {
    let f = Fixture::new();
    f.execute("CREATE TABLE q (id bigint PRIMARY KEY)");
    for sql in [
        "CREATE TABLE t (id bigint PRIMARY KEY, v varchar(10))",
        "CREATE TABLE t (id bigint PRIMARY KEY, v char(3))",
        "ALTER TABLE q ADD COLUMN v varchar(10)",
    ] {
        let bytes = f.catalog_bytes();
        assert!(
            matches!(
                f.admin().sql_session().execute(sql, &[], None),
                Err(CoreError::EngineUnsupported {
                    engine: "v2",
                    feature: "varchar/char types"
                })
            ),
            "{sql}"
        );
        assert_eq!(f.catalog_bytes(), bytes, "{sql}");
    }
}
