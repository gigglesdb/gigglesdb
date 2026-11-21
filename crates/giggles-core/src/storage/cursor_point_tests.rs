use super::*;
use engine::{
    codec,
    kv::{Direction, Durability, Keyspace, KvError, KvIter, KvPair, KvRead, WriteBatch},
    mvcc,
};
use relational::TableDef;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    path: std::path::PathBuf,
    admin: Option<Admin>,
}

impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("cursor-point-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        Self {
            path,
            admin: Some(admin),
        }
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn db(&self) -> &Arc<v2::V2Db> {
        let Backend::V2(db) = &self.admin().inner.backend else {
            unreachable!()
        };
        db
    }

    fn snapshot(&self) -> V2Snapshot {
        let db = self.db();
        let guard = db.sequencer.snapshot();
        V2Snapshot {
            index_reads: Default::default(),
            indexes: Vec::new(),
            _db: db.clone(),
            kv: db.kv.clone(),
            ts: guard.ts(),
            _guard: guard,
            error: RefCell::new(None),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn scan_one<'s>(
    source: &'s V2Snapshot,
    table: &TableDef,
    key: &[u8],
) -> Result<Option<scan::SourceRow<'s>>, SqlError> {
    let range = scan::KeyRange {
        lo: Bound::Included(key.to_vec()),
        hi: Bound::Included(key.to_vec()),
    };
    source
        .scan(table, &range, scan::Direction::Forward)?
        .next()
        .transpose()
}

#[track_caller]
fn assert_point_matches_scan(
    source: &V2Snapshot,
    table: &TableDef,
    key: &[u8],
    expected: Option<&[Value]>,
) {
    let point = source.get(table, key).unwrap();
    let scanned = scan_one(source, table, key).unwrap();
    let owned = |item: scan::SourceRow<'_>| (item.key.into_owned(), item.row.into_owned());
    assert_eq!(
        point.as_ref().map(|row| row.row.values.as_slice()),
        expected
    );
    if let Some(row) = &point {
        assert_eq!(row.key.as_ref(), key);
    }
    assert_eq!(point.map(owned), scanned.map(owned));
}

#[test]
fn point_get_matches_single_key_scan_at_each_snapshot() {
    let f = Fixture::new();
    let mut sql = f.admin().sql_session();
    let mut execute = |statement| sql.execute(statement, &[], None).unwrap();
    for statement in [
        "CREATE TABLE items (id text PRIMARY KEY, v bigint)",
        "CREATE TABLE neighbor (id text PRIMARY KEY, v bigint)",
        "CREATE TABLE pair (a text, b bigint, v bigint, PRIMARY KEY (a,b))",
        "INSERT INTO items VALUES ('kx', 90)",
        "INSERT INTO neighbor VALUES ('k', 99)",
        "INSERT INTO pair VALUES ('k', 7, 70), ('k', 8, 80)",
    ] {
        execute(statement);
    }
    let db = f.db();
    let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
    let tables = db.catalog().parts(&mut txn).unwrap().tables;
    drop(txn);
    let items = &tables["items"];
    let neighbor = &tables["neighbor"];
    let pair = &tables["pair"];
    let text = |s: &str| Value::Text(s.into());
    let key = codec::pk_bytes(&[text("k")]).unwrap();
    let longer_text = codec::pk_bytes(&[text("kx")]).unwrap();
    let composite = codec::pk_bytes(&[text("k"), Value::Int(7)]).unwrap();
    let missing_composite = codec::pk_bytes(&[text("k"), Value::Int(9)]).unwrap();
    let missing = codec::pk_bytes(&[text("missing")]).unwrap();
    assert!(composite.starts_with(&key) && composite.len() > key.len());

    let before_insert = f.snapshot();
    execute("INSERT INTO items VALUES ('k', 10)");
    let before_update = f.snapshot();
    execute("UPDATE items SET v = 20 WHERE id = 'k'");
    execute("UPDATE items SET v = 30 WHERE id = 'k'");
    let before_delete = f.snapshot();
    execute("DELETE FROM items WHERE id = 'k'");
    let latest = f.snapshot();

    assert_point_matches_scan(&before_insert, items, &key, None);
    assert_point_matches_scan(
        &before_update,
        items,
        &key,
        Some(&[text("k"), Value::Int(10)]),
    );
    assert_point_matches_scan(
        &before_delete,
        items,
        &key,
        Some(&[text("k"), Value::Int(30)]),
    );
    assert_point_matches_scan(&latest, items, &key, None);
    assert_point_matches_scan(&latest, items, &missing, None);
    assert_point_matches_scan(
        &latest,
        items,
        &longer_text,
        Some(&[text("kx"), Value::Int(90)]),
    );
    assert_point_matches_scan(&latest, neighbor, &key, Some(&[text("k"), Value::Int(99)]));
    assert_point_matches_scan(&latest, pair, &missing_composite, None);
    // A single component is a strict byte prefix, not a complete key on pair.
    assert!(latest.get(pair, &key).unwrap().is_none());
    assert_point_matches_scan(
        &latest,
        pair,
        &composite,
        Some(&[text("k"), Value::Int(7), Value::Int(70)]),
    );
}

#[test]
fn point_get_preserves_row_padding_and_corruption_errors() {
    let f = Fixture::new();
    f.admin()
        .sql_session()
        .execute(
            "CREATE TABLE t (id bigint PRIMARY KEY, v bigint)",
            &[],
            None,
        )
        .unwrap();
    let db = f.db();
    let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
    let table = db.catalog().parts(&mut txn).unwrap().tables["t"].clone();
    drop(txn);
    let source = f.snapshot();
    let mut batch = WriteBatch::default();
    let mut put = |id, payload: &dyn Fn(&[u8]) -> Vec<u8>| {
        let key = codec::pk_bytes(&[Value::Int(id)]).unwrap();
        let stored_key = codec::row_key(table.id, &key, source.ts);
        let full_key = codec::split_versioned(&stored_key).unwrap().0;
        let value = payload(full_key);
        batch.put(Keyspace::Rows, stored_key, value);
        key
    };
    let short = put(1, &|key| {
        mvcc::encode_live(&codec::encode_row(key, 42, &[Value::Int(1)]).unwrap())
    });
    let overfull = put(2, &|key| {
        let values = [Value::Int(2), Value::Null, Value::Null];
        mvcc::encode_live(&codec::encode_row(key, 42, &values).unwrap())
    });
    // A row-format tombstone under a live MVCC envelope is corrupt.
    let inner_tombstone = put(3, &|key| {
        mvcc::encode_live(&codec::encode_tombstone(key, 42).unwrap())
    });
    let invalid_row = put(4, &|_| mvcc::encode_live(&[]));
    let invalid_envelope = put(5, &|_| vec![2]);
    db.kv.write(batch, Durability::Sync).unwrap();

    assert_point_matches_scan(&source, &table, &short, Some(&[Value::Int(1), Value::Null]));
    assert_eq!(source.get(&table, &short).unwrap().unwrap().row.version, 42);
    for (key, message) in [
        (overfull, "cursor row arity"),
        (inner_tombstone, "cursor row arity"),
        (invalid_row, "short row"),
        (invalid_envelope, "invalid MVCC value envelope"),
    ] {
        assert_eq!(source.get(&table, &key).err(), Some(SqlError::Unsupported));
        let point_error = source.error.take().unwrap();
        assert_eq!(
            scan_one(&source, &table, &key).err(),
            Some(SqlError::Unsupported)
        );
        let scan_error = source.error.take().unwrap();
        assert!(matches!(&point_error, CoreError::Corrupt(detail) if detail.contains(message)));
        assert_eq!(point_error, scan_error);
    }
}

struct CountingKv {
    inner: Arc<dyn KvRead>,
    ranges: AtomicUsize,
    points: AtomicUsize,
}

impl KvRead for CountingKv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.inner.get(ks, key)
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        self.inner.range(ks, start, end, dir)
    }

    fn first_in(&self, ks: Keyspace, start: &[u8], end: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.points.fetch_add(1, Ordering::Relaxed);
        self.inner.first_in(ks, start, end)
    }
}

#[test]
fn prepared_cursor_point_read_tracks_transaction_isolation() {
    let f = Fixture::new();
    let mut writer = f.admin().sql_session();
    writer
        .execute(
            "CREATE TABLE t (id bigint PRIMARY KEY, v bigint)",
            &[],
            None,
        )
        .unwrap();
    writer
        .execute("INSERT INTO t VALUES (1, 10)", &[], None)
        .unwrap();
    let mut repeatable = f.admin().sql_session();
    repeatable.set_isolation(SqlIsolation::RepeatableRead);
    let mut committed = f.admin().sql_session();
    committed.set_isolation(SqlIsolation::ReadCommitted);
    let prepare = |reader: &mut SqlSession| {
        reader.execute("BEGIN", &[], None).unwrap();
        reader.prepare("SELECT v FROM t WHERE id = $1").unwrap()
    };
    let repeatable_query = prepare(&mut repeatable);
    let committed_query = prepare(&mut committed);
    let read = |reader: &mut SqlSession, query: &relational::Prepared, expected: Option<i64>| {
        let mut cursor = reader.open_cursor(query, &[Value::Int(1)]).unwrap();
        let Data::Scan {
            source: Some(source),
            image,
            ..
        } = &mut cursor.data
        else {
            panic!("expected a streaming V2 cursor");
        };
        let counting = Arc::new(CountingKv {
            inner: source.kv.clone(),
            ranges: AtomicUsize::new(0),
            points: AtomicUsize::new(0),
        });
        source.kv = counting.clone();
        let table = &image.tables["t"];
        let key = codec::pk_bytes(&[Value::Int(1)]).unwrap();
        assert_eq!(
            scan_one(source, table, &key).unwrap().is_some(),
            expected.is_some()
        );
        assert_eq!(counting.ranges.swap(0, Ordering::Relaxed), 1);
        assert_eq!(counting.points.load(Ordering::Relaxed), 0);

        let page = cursor.fetch(2, 4096).unwrap();
        let actual: Vec<_> = page.into_iter().map(|row| row.values).collect();
        assert_eq!(
            actual,
            expected
                .map(|n| vec![Value::Int(n)])
                .into_iter()
                .collect::<Vec<_>>()
        );
        assert_eq!(counting.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(counting.points.load(Ordering::Relaxed), 1);
    };
    read(&mut repeatable, &repeatable_query, Some(10));
    read(&mut committed, &committed_query, Some(10));
    writer
        .execute("UPDATE t SET v = 20 WHERE id = 1", &[], None)
        .unwrap();
    read(&mut repeatable, &repeatable_query, Some(10));
    read(&mut committed, &committed_query, Some(20));
    writer
        .execute("DELETE FROM t WHERE id = 1", &[], None)
        .unwrap();
    read(&mut repeatable, &repeatable_query, Some(10));
    read(&mut committed, &committed_query, None);
    repeatable.execute("ROLLBACK", &[], None).unwrap();
    committed.execute("ROLLBACK", &[], None).unwrap();
}
