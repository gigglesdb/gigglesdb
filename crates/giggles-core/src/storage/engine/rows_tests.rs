use super::super::{
    catalog::{Catalog, TableEntry},
    codec,
    commit::{ManualClock, Sequencer, SequencerConfig},
    kv::{Direction, Keyspace, Kv},
    kv_mem::MemKv,
    locks::{LockConfig, LockManager, LockMode, TxnId},
    seq::SequenceAllocator,
    txn::{Isolation, Transaction},
};
use super::{Row, RowStore};
use crate::{
    CoreError,
    relational::{ColumnDef, DataType, SqlError, TableDef, Value},
};
use giggles_pg_types::{
    Float4, Float8, Numeric,
    temporal::{Date, Interval, ParserSettings, Time, TimeTz, Timestamp, TimestampTz},
};
use std::{
    collections::BTreeSet,
    ops::Bound::{Excluded, Included, Unbounded},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

struct Env {
    mem: Arc<MemKv>,
    seq: Arc<Sequencer>,
    locks: Arc<LockManager>,
    allocator: Arc<SequenceAllocator>,
    rows: Arc<RowStore>,
}
impl Env {
    fn new() -> Self {
        Self::open(Arc::new(MemKv::new()))
    }
    fn open(mem: Arc<MemKv>) -> Self {
        let kv = Arc::new(Kv::new(mem.clone()));
        let seq = Sequencer::open(
            kv.clone(),
            Arc::new(ManualClock::new(10)),
            SequencerConfig {
                group_max: 0,
                group_wait: Duration::ZERO,
                ..SequencerConfig::default()
            },
        )
        .unwrap();
        let locks = Arc::new(LockManager::new(LockConfig::default()));
        let allocator = Arc::new(SequenceAllocator::new(kv, 64));
        let rows = Arc::new(RowStore::new(allocator.clone()));
        Self {
            mem,
            seq,
            locks,
            allocator,
            rows,
        }
    }
    fn txn(&self, id: u64) -> Transaction {
        self.isolated(id, Isolation::ReadCommitted)
    }
    fn isolated(&self, id: u64, isolation: Isolation) -> Transaction {
        Transaction::begin(&self.seq, &self.locks, TxnId(id), isolation)
    }
    fn catalog(&self) -> Catalog {
        Catalog::open(self.seq.clone(), self.locks.clone(), self.allocator.clone()).unwrap()
    }
}
fn column(id: u64, name: &str, data_type: DataType, not_null: bool) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        data_type,
        not_null,
    }
}
fn table(id: u64) -> TableEntry {
    TableEntry {
        primary_index_birth: None,
        def: TableDef {
            id,
            name: format!("table_{id}"),
            columns: vec![
                column(1, "id", DataType::BigInt, true),
                column(2, "value", DataType::BigInt, false),
            ],
            primary_key: vec![0],
        },
        heap: false,
        columns_ext: Default::default(),
    }
}
fn pk(n: i64) -> Vec<Value> {
    vec![Value::Int(n)]
}
fn encoded(n: i64) -> Vec<u8> {
    codec::pk_bytes(&pk(n)).unwrap()
}
fn user_key(entry: &TableEntry, n: i64) -> Vec<u8> {
    let mut key = entry.def.id.to_be_bytes().to_vec();
    key.extend(encoded(n));
    key
}
fn insert(env: &Env, txn: &mut Transaction, entry: &TableEntry, n: i64, value: i64) -> Row {
    env.rows
        .insert(
            txn,
            entry,
            vec![Value::Int(n), Value::Int(value)],
            None,
            &|| false,
        )
        .unwrap()
}
fn scan(env: &Env, txn: &mut Transaction, entry: &TableEntry) -> Vec<(Vec<u8>, Row)> {
    env.rows
        .scan(txn, entry, Unbounded, Unbounded, Direction::Forward, None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
fn sql_error<T: std::fmt::Debug>(result: Result<T, CoreError>, expected: SqlError) {
    assert!(
        matches!(&result, Err(CoreError::Sql(actual)) if *actual == expected),
        "expected {expected:?}, got {result:?}"
    );
}

fn typed_values() -> Vec<(DataType, Value, Value)> {
    let settings = ParserSettings::default();
    vec![
        (DataType::Boolean, Value::Bool(true), Value::Bool(false)),
        (DataType::SmallInt, Value::Int(-32768), Value::Int(32767)),
        (
            DataType::Integer,
            Value::Int(i32::MIN as i64),
            Value::Int(i32::MAX as i64),
        ),
        (DataType::BigInt, Value::Int(i64::MIN), Value::Int(i64::MAX)),
        (
            DataType::Text,
            Value::Text("hello\0世界".into()),
            Value::Text("updated 🦀".into()),
        ),
        (
            DataType::Bytes,
            Value::Bytes(vec![0, 255, 1]),
            Value::Bytes(vec![255, 0, 128]),
        ),
        (
            DataType::Numeric,
            Value::Numeric(Numeric::parse("-123456789.00100").unwrap()),
            Value::Numeric(Numeric::parse("999.25").unwrap()),
        ),
        (
            DataType::Real,
            Value::Float4(Float4::parse("-1.25").unwrap()),
            Value::Float4(Float4::parse("2.5").unwrap()),
        ),
        (
            DataType::DoublePrecision,
            Value::Float8(Float8::parse("-123.125").unwrap()),
            Value::Float8(Float8::parse("987.0625").unwrap()),
        ),
        (
            DataType::Date,
            Value::Date(Date::parse("2000-02-29", &settings).unwrap()),
            Value::Date(Date::parse("2024-12-31", &settings).unwrap()),
        ),
        (
            DataType::Time,
            Value::Time(Time::parse("12:34:56.123456").unwrap()),
            Value::Time(Time::parse("23:59:59.999999").unwrap()),
        ),
        (
            DataType::TimeTz,
            Value::TimeTz(TimeTz::parse("12:34:56+02", &settings).unwrap()),
            Value::TimeTz(TimeTz::parse("01:02:03-05", &settings).unwrap()),
        ),
        (
            DataType::Timestamp,
            Value::Timestamp(Timestamp::parse("2000-02-29 12:34:56", &settings).unwrap()),
            Value::Timestamp(Timestamp::parse("2024-12-31 23:59:59", &settings).unwrap()),
        ),
        (
            DataType::TimestampTz,
            Value::TimestampTz(TimestampTz::parse("2000-02-29 12:34:56+02", &settings).unwrap()),
            Value::TimestampTz(TimestampTz::parse("2024-12-31 23:59:59-05", &settings).unwrap()),
        ),
        (
            DataType::Interval,
            Value::Interval(Interval::new(2, -3, 456789)),
            Value::Interval(Interval::new(-4, 5, -678901)),
        ),
    ]
}

#[test]
fn rows_crud_all_types_owned_values_versions_and_crash_reopen() {
    let env = Env::new();
    let cases = typed_values();
    let mut entry = table(7);
    entry.def.columns.truncate(1);
    for (i, (ty, _, _)) in cases.iter().enumerate() {
        entry
            .def
            .columns
            .push(column(i as u64 + 2, &format!("c{i}"), ty.clone(), false));
    }
    entry
        .def
        .columns
        .push(column(100, "nullable", DataType::Text, false));
    let mut original = vec![Value::Int(1)];
    original.extend(cases.iter().map(|(_, value, _)| value.clone()));
    original.push(Value::Null);
    let mut writer = env.txn(1);
    let inserted = env
        .rows
        .insert(&mut writer, &entry, original.clone(), None, &|| false)
        .unwrap();
    assert_eq!(inserted.values, original);
    assert!(inserted.version > 0);
    assert_eq!(
        env.rows.get(&mut writer, &entry, &pk(1)).unwrap(),
        Some(inserted.clone())
    );
    assert_eq!(
        scan(&env, &mut writer, &entry),
        vec![(encoded(1), inserted.clone())]
    );
    writer.commit().unwrap();
    let mut updater = env.txn(2);
    let sets: Vec<_> = cases
        .iter()
        .enumerate()
        .map(|(i, (_, _, value))| (i + 1, value.clone()))
        .collect();
    let changed = env
        .rows
        .update(
            &mut updater,
            &entry,
            &pk(1),
            &sets,
            Some(inserted.version),
            None,
            &|| false,
        )
        .unwrap()
        .unwrap();
    assert!(changed.version > inserted.version);
    for (i, (_, _, expected)) in cases.iter().enumerate() {
        assert_eq!(&changed.values[i + 1], expected);
    }
    assert_eq!(changed.values.last(), Some(&Value::Null));
    updater.commit().unwrap();
    let reopened = Env::open(Arc::new(env.mem.crash()));
    let mut reader = reopened.txn(3);
    assert_eq!(
        reopened.rows.get(&mut reader, &entry, &pk(1)).unwrap(),
        Some(changed.clone())
    );
    assert_eq!(
        scan(&reopened, &mut reader, &entry),
        vec![(encoded(1), changed.clone())]
    );
    reader.rollback();
    let mut deleter = reopened.txn(4);
    assert!(
        reopened
            .rows
            .delete(
                &mut deleter,
                &entry,
                &pk(1),
                Some(changed.version),
                None,
                &|| false
            )
            .unwrap()
            .is_some()
    );
    assert!(
        reopened
            .rows
            .get(&mut deleter, &entry, &pk(1))
            .unwrap()
            .is_none()
    );
    assert!(scan(&reopened, &mut deleter, &entry).is_empty());
    deleter.commit().unwrap();
    let twice_reopened = Env::open(Arc::new(reopened.mem.crash()));
    assert!(
        twice_reopened
            .rows
            .get(&mut twice_reopened.txn(5), &entry, &pk(1))
            .unwrap()
            .is_none()
    );
}

#[test]
fn rows_validation_matches_legacy_sql_errors() {
    let env = Env::new();
    let entry = table(7);
    let mut txn = env.txn(1);
    for values in [
        vec![],
        vec![Value::Int(1)],
        vec![Value::Int(1), Value::Int(2), Value::Null],
        vec![Value::Int(1), Value::Text("bad".into())],
    ] {
        sql_error(
            env.rows.insert(&mut txn, &entry, values, None, &|| false),
            SqlError::Type,
        );
    }
    sql_error(
        env.rows.insert(
            &mut txn,
            &entry,
            vec![Value::Null, Value::Int(2)],
            None,
            &|| false,
        ),
        SqlError::Null,
    );
    let mut required = entry.clone();
    required.def.columns[1].not_null = true;
    sql_error(
        env.rows.insert(
            &mut txn,
            &required,
            vec![Value::Int(1), Value::Null],
            None,
            &|| false,
        ),
        SqlError::Null,
    );
    let mut nullable_pk = entry.clone();
    nullable_pk.def.columns[0].not_null = false;
    sql_error(
        env.rows.insert(
            &mut txn,
            &nullable_pk,
            vec![Value::Null, Value::Int(2)],
            None,
            &|| false,
        ),
        SqlError::Null,
    );
    let mut heap = entry.clone();
    heap.heap = true;
    sql_error(
        env.rows.insert(
            &mut txn,
            &heap,
            vec![Value::Int(1), Value::Int(2)],
            None,
            &|| false,
        ),
        SqlError::Type,
    );
    for (ty, value) in [
        (DataType::SmallInt, Value::Int(32768)),
        (DataType::Integer, Value::Int(i32::MAX as i64 + 1)),
    ] {
        let mut narrow = entry.clone();
        narrow.def.columns[1].data_type = ty;
        sql_error(
            env.rows
                .insert(&mut txn, &narrow, vec![Value::Int(1), value], None, &|| {
                    false
                }),
            SqlError::Type,
        );
    }
    insert(&env, &mut txn, &entry, 1, 2);
    sql_error(
        env.rows.update(
            &mut txn,
            &entry,
            &pk(1),
            &[(0, Value::Int(1))],
            None,
            None,
            &|| false,
        ),
        SqlError::Unsupported,
    );
    sql_error(
        env.rows.update(
            &mut txn,
            &entry,
            &pk(1),
            &[(1, Value::Text("bad".into()))],
            None,
            None,
            &|| false,
        ),
        SqlError::Type,
    );
    assert_eq!(
        env.rows
            .get(&mut txn, &entry, &pk(1))
            .unwrap()
            .unwrap()
            .values,
        vec![Value::Int(1), Value::Int(2)]
    );
    txn.rollback();
}

#[test]
fn rows_duplicate_same_transaction_and_waiting_insert_commit_or_rollback() {
    for commit_first in [false, true] {
        let env = Env::new();
        let entry = table(7);
        let mut first = env.txn(1);
        insert(&env, &mut first, &entry, 1, 10);
        sql_error(
            env.rows.insert(
                &mut first,
                &entry,
                vec![Value::Int(1), Value::Int(11)],
                None,
                &|| false,
            ),
            SqlError::Duplicate,
        );
        let (queued_tx, queued_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let seq = env.seq.clone();
        let locks = env.locks.clone();
        let rows = env.rows.clone();
        let worker_entry = entry.clone();
        let worker = thread::spawn(move || {
            let mut second = Transaction::begin(&seq, &locks, TxnId(2), Isolation::ReadCommitted);
            let sent = AtomicBool::new(false);
            let result = rows.insert(
                &mut second,
                &worker_entry,
                vec![Value::Int(1), Value::Int(20)],
                None,
                &|| {
                    if !sent.swap(true, Ordering::SeqCst) {
                        assert_eq!(locks.waiting_count(), 1);
                        queued_tx.send(()).unwrap();
                    }
                    false
                },
            );
            done_tx.send(()).unwrap();
            if commit_first {
                sql_error(result, SqlError::Duplicate);
                second.rollback();
            } else {
                let row = result.unwrap();
                assert_eq!(row.values[1], Value::Int(20));
                second.commit().unwrap();
            }
        });
        queued_rx.recv().unwrap();
        assert!(done_rx.try_recv().is_err());
        assert_eq!(env.locks.waiting_count(), 1);
        if commit_first {
            first.commit().unwrap();
        } else {
            first.rollback();
        }
        worker.join().unwrap();
        let actual = env
            .rows
            .get(&mut env.txn(3), &entry, &pk(1))
            .unwrap()
            .unwrap();
        assert_eq!(
            actual.values[1],
            Value::Int(if commit_first { 10 } else { 20 })
        );
    }
}

#[test]
fn rows_expected_versions_noop_and_global_version_namespace() {
    let env = Env::new();
    let entry = table(7);
    let adjacent = table(8);
    let mut seed = env.txn(1);
    let first = insert(&env, &mut seed, &entry, 1, 10);
    let second = insert(&env, &mut seed, &adjacent, 1, 20);
    seed.commit().unwrap();
    assert!(second.version > first.version);
    let mut update = env.txn(2);
    sql_error(
        env.rows.update(
            &mut update,
            &entry,
            &pk(1),
            &[(1, Value::Int(11))],
            Some(first.version + 99),
            None,
            &|| false,
        ),
        SqlError::Version,
    );
    sql_error(
        env.rows.delete(
            &mut update,
            &entry,
            &pk(1),
            Some(first.version + 99),
            None,
            &|| false,
        ),
        SqlError::Version,
    );
    assert!(!update.writes_keyspace(Keyspace::Rows));
    for sets in [vec![], vec![(1, Value::Int(10))]] {
        let unchanged = env
            .rows
            .update(
                &mut update,
                &entry,
                &pk(1),
                &sets,
                Some(first.version),
                None,
                &|| false,
            )
            .unwrap()
            .unwrap();
        assert_eq!(unchanged, first);
        assert!(!update.writes_keyspace(Keyspace::Rows));
    }
    // An untouched column added after this row was stored is padded on read.
    // Validate assigned values before recognizing a no-op; the padding itself
    // must not turn the unchanged assignment into a constraint failure or write.
    let mut expanded = entry.clone();
    expanded
        .def
        .columns
        .push(column(3, "later_required", DataType::BigInt, true));
    let unchanged = env
        .rows
        .update(
            &mut update,
            &expanded,
            &pk(1),
            &[(1, Value::Int(10))],
            Some(first.version),
            None,
            &|| false,
        )
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.version, first.version);
    assert_eq!(
        unchanged.values,
        vec![Value::Int(1), Value::Int(10), Value::Null]
    );
    assert!(!update.writes_keyspace(Keyspace::Rows));
    let visible = env.seq.visible_ts();
    update.commit().unwrap();
    assert_eq!(env.seq.visible_ts(), visible);
    assert_eq!(
        env.allocator.next(3, 0).unwrap(),
        second.version + 1,
        "rejected and no-op writes must not allocate a version"
    );
    let mut update = env.txn(3);
    let changed = env
        .rows
        .update(
            &mut update,
            &entry,
            &pk(1),
            &[(1, Value::Int(11))],
            Some(first.version),
            None,
            &|| false,
        )
        .unwrap()
        .unwrap();
    assert!(changed.version > second.version);
    update.commit().unwrap();
    let mut reader = env.txn(4);
    assert_eq!(
        env.rows.get(&mut reader, &entry, &pk(1)).unwrap(),
        Some(changed)
    );
    assert!(
        env.rows
            .update(
                &mut reader,
                &entry,
                &pk(99),
                &[(1, Value::Int(0))],
                None,
                None,
                &|| false
            )
            .unwrap()
            .is_none()
    );
    assert!(
        env.rows
            .delete(&mut reader, &entry, &pk(99), None, None, &|| false)
            .unwrap()
            .is_none()
    );
}

#[test]
fn rows_heap_concurrent_1600_ids_survive_crash_and_restart_above_previous() {
    let env = Arc::new(Env::new());
    let mut entry = table(7);
    entry.heap = true;
    let start = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let env = env.clone();
            let entry = entry.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                let mut ids = Vec::new();
                for i in 0..200 {
                    let mut txn = env.txn(1 + worker * 200 + i);
                    let row = env
                        .rows
                        .insert(
                            &mut txn,
                            &entry,
                            vec![Value::Null, Value::Int((worker * 200 + i) as i64)],
                            None,
                            &|| false,
                        )
                        .unwrap();
                    let Value::Int(id) = row.values[0] else {
                        panic!("heap id must be Int")
                    };
                    assert!(id > 0);
                    ids.push(id);
                    txn.commit().unwrap();
                }
                ids
            })
        })
        .collect();
    let ids: BTreeSet<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(ids.len(), 1600);
    let mut reader = env.txn(2001);
    let all = scan(&env, &mut reader, &entry);
    assert_eq!(all.len(), 1600);
    let scanned: BTreeSet<_> = all
        .iter()
        .map(|(_, row)| match row.values[0] {
            Value::Int(id) => id,
            _ => panic!("heap id"),
        })
        .collect();
    assert_eq!(scanned, ids);
    reader.rollback();
    let reopened = Env::open(Arc::new(env.mem.crash()));
    let mut txn = reopened.txn(2002);
    assert_eq!(scan(&reopened, &mut txn, &entry).len(), 1600);
    let next = reopened
        .rows
        .insert(
            &mut txn,
            &entry,
            vec![Value::Null, Value::Int(1600)],
            None,
            &|| false,
        )
        .unwrap();
    assert!(matches!(next.values[0], Value::Int(id) if id > *ids.last().unwrap()));
    txn.commit().unwrap();
}

#[test]
fn rows_scan_all_bound_combinations_directions_limits_and_table_edges() {
    let env = Env::new();
    let entry = table(7);
    let mut seed = env.txn(1);
    for id in [6, 7, 8, u64::MAX] {
        for n in 1..=5 {
            insert(&env, &mut seed, &table(id), n, id as i64);
        }
    }
    seed.commit().unwrap();
    let bounds = [
        Unbounded,
        Included(Vec::new()),
        Excluded(Vec::new()),
        Included(encoded(2)),
        Excluded(encoded(2)),
        Included(encoded(4)),
        Excluded(encoded(4)),
    ];
    let mut reader = env.txn(2);
    for lo in &bounds {
        for hi in &bounds {
            for direction in [Direction::Forward, Direction::Reverse] {
                for limit in [None, Some(0), Some(1), Some(3), Some(99)] {
                    let mut expected: Vec<_> = (1..=5)
                        .filter(|&n| {
                            let key = encoded(n);
                            (match lo {
                                Included(k) => &key >= k,
                                Excluded(k) => &key > k,
                                Unbounded => true,
                            }) && (match hi {
                                Included(k) => &key <= k,
                                Excluded(k) => &key < k,
                                Unbounded => true,
                            })
                        })
                        .collect();
                    if direction == Direction::Reverse {
                        expected.reverse();
                    }
                    if let Some(limit) = limit {
                        expected.truncate(limit);
                    }
                    let actual = env
                        .rows
                        .scan(
                            &mut reader,
                            &entry,
                            lo.clone(),
                            hi.clone(),
                            direction,
                            limit,
                        )
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    assert_eq!(
                        actual
                            .iter()
                            .map(|(key, _)| key.clone())
                            .collect::<Vec<_>>(),
                        expected.iter().map(|&n| encoded(n)).collect::<Vec<_>>(),
                        "{lo:?}..{hi:?} {direction:?} {limit:?}"
                    );
                    assert!(actual.iter().all(|(_, row)| row.values[1] == Value::Int(7)));
                }
            }
        }
    }
    for id in [7, 8, u64::MAX] {
        for direction in [Direction::Forward, Direction::Reverse] {
            let actual = env
                .rows
                .scan(
                    &mut reader,
                    &table(id),
                    Unbounded,
                    Unbounded,
                    direction,
                    None,
                )
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(actual.len(), 5);
            assert!(
                actual
                    .iter()
                    .all(|(_, row)| row.values[1] == Value::Int(id as i64))
            );
        }
    }
}

#[test]
fn rows_scan_and_get_overlay_only_the_reading_transactions_writes() {
    let env = Env::new();
    let entry = table(7);
    let mut seed = env.txn(1);
    insert(&env, &mut seed, &entry, 1, 10);
    insert(&env, &mut seed, &entry, 2, 20);
    seed.commit().unwrap();
    let mut own = env.txn(2);
    let mut other = env.txn(3);
    env.rows
        .delete(&mut own, &entry, &pk(1), None, None, &|| false)
        .unwrap();
    insert(&env, &mut own, &entry, 3, 30);
    assert!(env.rows.get(&mut own, &entry, &pk(1)).unwrap().is_none());
    assert!(env.rows.get(&mut other, &entry, &pk(1)).unwrap().is_some());
    assert!(env.rows.get(&mut other, &entry, &pk(3)).unwrap().is_none());
    assert_eq!(
        scan(&env, &mut own, &entry)
            .iter()
            .map(|(_, row)| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(2), Value::Int(3)]
    );
    assert_eq!(
        scan(&env, &mut other, &entry)
            .iter()
            .map(|(_, row)| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(2)]
    );
    own.rollback();
    other.rollback();
}

#[test]
fn rows_catalog_add_column_pads_existing_rows_and_accepts_new_shape() {
    let env = Env::new();
    let catalog = env.catalog();
    let mut create = env.txn(1);
    let entry = catalog
        .create_table(&mut create, table(0).def, false, None, &|| false)
        .unwrap();
    catalog.commit_ddl(create).unwrap();
    let mut seed = env.txn(2);
    let original = insert(&env, &mut seed, &entry, 1, 10);
    seed.commit().unwrap();
    let mut ddl = env.txn(3);
    let expanded = catalog
        .add_column(
            &mut ddl,
            &entry.def.name,
            column(0, "added", DataType::Text, false),
            None,
            &|| false,
        )
        .unwrap();
    catalog.commit_ddl(ddl).unwrap();
    let mut reader = env.txn(4);
    let resolved = catalog
        .resolve(
            &mut reader,
            &entry.def.name,
            LockMode::RowExclusive,
            None,
            &|| false,
        )
        .unwrap();
    assert_eq!(resolved, expanded);
    let padded = env
        .rows
        .get(&mut reader, &resolved, &pk(1))
        .unwrap()
        .unwrap();
    assert_eq!(
        padded.values,
        vec![Value::Int(1), Value::Int(10), Value::Null]
    );
    assert_eq!(padded.version, original.version);
    let new = env
        .rows
        .insert(
            &mut reader,
            &resolved,
            vec![
                Value::Int(2),
                Value::Int(20),
                Value::Text("new column".into()),
            ],
            None,
            &|| false,
        )
        .unwrap();
    assert_eq!(
        scan(&env, &mut reader, &resolved),
        vec![(encoded(1), padded), (encoded(2), new)]
    );
    reader.commit().unwrap();
    let reopened = Env::open(Arc::new(env.mem.crash()));
    let catalog = reopened.catalog();
    let mut reader = reopened.txn(5);
    let resolved = catalog
        .resolve(
            &mut reader,
            &entry.def.name,
            LockMode::AccessShare,
            None,
            &|| false,
        )
        .unwrap();
    assert_eq!(scan(&reopened, &mut reader, &resolved).len(), 2);
    assert_eq!(
        reopened
            .rows
            .get(&mut reader, &resolved, &pk(1))
            .unwrap()
            .unwrap()
            .values[2],
        Value::Null
    );
}

#[test]
fn rows_repeatable_read_lost_update_routes_to_sql_conflict() {
    let env = Env::new();
    let entry = table(7);
    let mut seed = env.txn(1);
    insert(&env, &mut seed, &entry, 1, 0);
    seed.commit().unwrap();
    let mut first = env.isolated(2, Isolation::RepeatableRead);
    let mut second = env.isolated(3, Isolation::RepeatableRead);
    let original = env.rows.get(&mut first, &entry, &pk(1)).unwrap().unwrap();
    assert_eq!(
        env.rows.get(&mut second, &entry, &pk(1)).unwrap(),
        Some(original)
    );
    env.rows
        .update(
            &mut first,
            &entry,
            &pk(1),
            &[(1, Value::Int(1))],
            None,
            None,
            &|| false,
        )
        .unwrap();
    let (queued_tx, queued_rx) = mpsc::channel();
    let rows = env.rows.clone();
    let worker_locks = env.locks.clone();
    let worker_entry = entry.clone();
    let worker = thread::spawn(move || {
        let sent = AtomicBool::new(false);
        let result = rows.update(
            &mut second,
            &worker_entry,
            &pk(1),
            &[(1, Value::Int(1))],
            None,
            None,
            &|| {
                if !sent.swap(true, Ordering::SeqCst) {
                    assert_eq!(worker_locks.waiting_count(), 1);
                    queued_tx.send(()).unwrap();
                }
                false
            },
        );
        sql_error(result, SqlError::Conflict);
        second.rollback();
    });
    queued_rx.recv().unwrap();
    first.commit().unwrap();
    worker.join().unwrap();
    assert_eq!(
        env.rows
            .get(&mut env.txn(4), &entry, &pk(1))
            .unwrap()
            .unwrap()
            .values[1],
        Value::Int(1)
    );
}

#[test]
fn rows_read_committed_locked_current_increments_exactly() {
    let env = Arc::new(Env::new());
    let entry = table(7);
    let mut seed = env.txn(1);
    insert(&env, &mut seed, &entry, 1, 0);
    seed.commit().unwrap();
    let start = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let env = env.clone();
            let entry = entry.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                for iteration in 0..50 {
                    let mut txn = env.txn(2 + worker * 50 + iteration);
                    let key = user_key(&entry, 1);
                    // Lock before computing the assignment, as an UPDATE executor's EPQ path does.
                    let current = txn
                        .lock_for_write(Keyspace::Rows, &key, None, &|| false)
                        .unwrap()
                        .current
                        .unwrap();
                    let current = codec::decode_row(&key, &current).unwrap();
                    let Value::Int(n) = current.values[1] else {
                        panic!("counter must be an integer")
                    };
                    let updated = env
                        .rows
                        .update(
                            &mut txn,
                            &entry,
                            &pk(1),
                            &[(1, Value::Int(n + 1))],
                            Some(current.row_version),
                            None,
                            &|| false,
                        )
                        .unwrap()
                        .unwrap();
                    assert_eq!(updated.values[1], Value::Int(n + 1));
                    txn.commit().unwrap();
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        env.rows
            .get(&mut env.txn(1000), &entry, &pk(1))
            .unwrap()
            .unwrap()
            .values[1],
        Value::Int(400)
    );
}

#[test]
fn rows_payload_crc_binds_user_key_and_scan_decodes_lazily() {
    let env = Env::new();
    let entry = table(7);
    let mut seed = env.txn(1);
    let good = insert(&env, &mut seed, &entry, 1, 10);
    seed.commit().unwrap();
    let mut corrupt = env.txn(2);
    let wrong_key = user_key(&entry, 2);
    corrupt
        .lock_for_write(Keyspace::Rows, &wrong_key, None, &|| false)
        .unwrap();
    corrupt
        .put(
            Keyspace::Rows,
            wrong_key.clone(),
            codec::encode_row(&user_key(&entry, 1), 123, &[Value::Int(2), Value::Int(20)]).unwrap(),
        )
        .unwrap();
    corrupt.commit().unwrap();
    let mut reader = env.txn(3);
    assert!(matches!(
        env.rows.get(&mut reader, &entry, &pk(2)),
        Err(CoreError::Corrupt(_))
    ));
    let mut lazy = env
        .rows
        .scan(
            &mut reader,
            &entry,
            Unbounded,
            Unbounded,
            Direction::Forward,
            None,
        )
        .unwrap();
    assert_eq!(lazy.next().unwrap().unwrap(), (encoded(1), good));
    assert!(matches!(lazy.next(), Some(Err(CoreError::Corrupt(_)))));
    drop(lazy);
    let limited = env
        .rows
        .scan(
            &mut reader,
            &entry,
            Unbounded,
            Unbounded,
            Direction::Forward,
            Some(1),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(limited.len(), 1);
}

#[test]
fn rows_schema_with_fewer_columns_than_record_is_corrupt() {
    let env = Env::new();
    let entry = table(7);
    let mut seed = env.txn(1);
    insert(&env, &mut seed, &entry, 1, 10);
    seed.commit().unwrap();
    let mut narrow = entry;
    narrow.def.columns.truncate(1);
    assert!(matches!(
        env.rows.get(&mut env.txn(2), &narrow, &pk(1)),
        Err(CoreError::Corrupt(_))
    ));
}
