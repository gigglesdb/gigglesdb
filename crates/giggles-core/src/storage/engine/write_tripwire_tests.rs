//! Replay tripwire for writes reachable through the v2 SQL/Admin surface.
use super::{
    kv::{Direction, Durability, Keyspace, KvBackend, KvError, KvIter, KvRead, Op, WriteBatch},
    kv_mem,
};
use crate::{Admin, Limits, relational::Value, storage::v2::DEFAULT_WRITE_SET_BYTES};
use std::{
    collections::BTreeSet,
    ops::Bound,
    sync::{Arc, Mutex},
};

struct Recording {
    inner: Arc<kv_mem::MemKv>,
    log: Mutex<Vec<WriteBatch>>,
}

impl KvRead for Recording {
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
        self.inner.range(ks, start, end, dir)
    }
}

impl KvBackend for Recording {
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        self.log.lock().unwrap().push(batch.clone());
        self.inner.write(batch, durability)
    }

    fn flush_barrier(&self) -> Result<(), KvError> {
        self.inner.flush_barrier()
    }

    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.inner.snapshot()
    }

    fn engine_name(&self) -> &'static str {
        self.inner.engine_name()
    }
}

fn touched(batches: &[WriteBatch]) -> BTreeSet<Keyspace> {
    batches
        .iter()
        .flat_map(|batch| batch.ops())
        .map(|op| match op {
            Op::Put { ks, .. } | Op::Delete { ks, .. } | Op::DeleteRange { ks, .. } => *ks,
        })
        .collect()
}

#[test]
fn every_engine_write_goes_through_kv_backend_write() {
    let rec = Arc::new(Recording {
        inner: Arc::new(kv_mem::MemKv::new()),
        log: Mutex::new(Vec::new()),
    });
    let mut domain = [0; 16];
    getrandom::fill(&mut domain).unwrap();
    super::meta::write_initial_meta(rec.as_ref(), domain, 0).unwrap();
    let admin =
        Admin::attach_v2_backend(rec.clone(), Limits::default(), DEFAULT_WRITE_SET_BYTES).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE t(id int PRIMARY KEY, v text)",
        "CREATE INDEX t_v ON t(v)",
        "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "UPDATE t SET v = 'updated' WHERE id = 2",
        "DELETE FROM t WHERE id = 1",
        "CREATE SEQUENCE s",
    ] {
        session
            .execute(sql, &[], None)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    // SQL counters write separately from SequenceAllocator's 1024-value blocks.
    // nextval requires the clocked API to install its sequence callback.
    let before_sequence = rec.log.lock().unwrap().len();
    let clock = crate::relational::StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    };
    for value in 1..=1025 {
        let result = session
            .execute_at("SELECT nextval('s')", &[], None, &clock)
            .unwrap();
        assert_eq!(result.rows[0].values, vec![Value::Int(value)]);
    }
    assert!(touched(&rec.log.lock().unwrap()[before_sequence..]).contains(&Keyspace::Seq));
    for sql in [
        "CREATE TABLE retired(id int PRIMARY KEY, v text)",
        "CREATE INDEX retired_v ON retired(v)",
        "INSERT INTO retired VALUES (1, 'retired')",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    let before_drop = rec.log.lock().unwrap().len();
    session.execute("DROP TABLE retired", &[], None).unwrap();
    let before_reclaim = rec.log.lock().unwrap().len();
    admin.reclaim_storage().unwrap();
    {
        let log = rec.log.lock().unwrap();
        // DROP's commit reclaims Rows and Index prefixes automatically; the
        // subsequent explicit pass is normally empty. Neither runs MVCC Gc.
        assert!(touched(&log[before_drop..before_reclaim]).contains(&Keyspace::Catalog));
        let reclaimed: BTreeSet<_> = log[before_drop..]
            .iter()
            .flat_map(|batch| batch.ops())
            .filter_map(|op| match op {
                Op::DeleteRange { ks, .. } => Some(*ks),
                _ => None,
            })
            .collect();
        assert_eq!(reclaimed, BTreeSet::from([Keyspace::Rows, Keyspace::Index]));
        eprintln!(
            "DROP keyspaces: {:?}; reclaim keyspaces: {:?}",
            touched(&log[before_drop..before_reclaim]),
            touched(&log[before_reclaim..])
        );
    }
    drop(session);
    {
        use crate::operations::{Bindings, Engine, Request, ValueExpr,
            once::{ONCE_CONTRACT_VERSION, OnceBody, OnceIdentity, OnceRequest, OnceTime}};
        use std::time::{Duration, Instant};
        let engine = Engine::new(admin.clone());
        let request = OnceRequest { contract_version: ONCE_CONTRACT_VERSION,
            identity: OnceIdentity { database_domain: admin.authority_metadata().unwrap().0,
                tenant: 1, namespace: "tripwire".into(), key: vec![1] }, retention_seconds: 60,
            body: OnceBody::Operation(Request::Insert { table: "t".into(), values: vec![
                ("id".into(), ValueExpr::Literal(Value::Int(4))),
                ("v".into(), ValueExpr::Literal(Value::Text("four".into())))] }) };
        let clock = |seconds| move || Ok(OnceTime { lower_unix_seconds: seconds,
            upper_unix_seconds: seconds, valid_until: Instant::now() + Duration::from_secs(10) });
        let before_once = rec.log.lock().unwrap().len();
        assert!(engine.execute_once(&request, &Bindings::new(), &clock(1000)).unwrap().outcome.is_ok());
        let before_compact = rec.log.lock().unwrap().len();
        assert_eq!(engine.compact_outcomes(&clock(2000)).unwrap(), 1);
        let log = rec.log.lock().unwrap();
        assert!(touched(&log[before_once..before_compact]).contains(&Keyspace::Meta));
        assert!(touched(&log[before_once..before_compact]).contains(&Keyspace::Kernel));
        assert!(touched(&log[before_compact..]).contains(&Keyspace::Kernel));
        assert_eq!(super::meta::open_checks(rec.as_ref()).unwrap().subversion, 2);
    }
    drop(admin);
    let reopened =
        Admin::attach_v2_backend(rec.clone(), Limits::default(), DEFAULT_WRITE_SET_BYTES).unwrap();
    let rows: Vec<_> = reopened
        .sql_session()
        .execute("SELECT id, v FROM t ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect();
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(2), Value::Text("updated".into())],
            vec![Value::Int(3), Value::Text("three".into())],
            vec![Value::Int(4), Value::Text("four".into())],
        ]
    );
    drop(reopened);

    let replay = kv_mem::MemKv::new();
    let log = rec.log.lock().unwrap();
    for batch in log.iter() {
        replay.write(batch.clone(), Durability::Sync).unwrap();
    }
    for ks in Keyspace::ALL {
        let scan = |kv: &dyn KvRead| {
            kv.range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(
            scan(rec.inner.as_ref()),
            scan(&replay),
            "unrecorded write in {ks:?}"
        );
    }
    let written = touched(&log);
    for ks in [
        Keyspace::Meta,
        Keyspace::Catalog,
        Keyspace::Rows,
        Keyspace::Index,
        Keyspace::Seq,
        Keyspace::Kernel,
    ] {
        assert!(written.contains(&ks), "workload did not write {ks:?}");
    }
}
