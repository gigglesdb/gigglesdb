use super::*;
use crate::coordinator::{self, Transition, journal::Entry};
use std::{cell::RefCell, rc::Rc};

#[derive(Default)]
struct Effects {
    intent: Option<Step>,
    events: Vec<&'static str>,
    privileges: Vec<String>,
    original: Vec<String>,
    stopped: bool,
    calls: usize,
    crash: Option<usize>,
    pending: bool,
}
type Shared = Rc<RefCell<Effects>>;
fn boundary(e: &Shared) -> cutover::Result<()> {
    let mut e = e.borrow_mut();
    let call = e.calls;
    e.calls += 1;
    if e.crash == Some(call) {
        e.crash = None;
        return Err(cutover::Error::Component("lost reply".into()));
    }
    Ok(())
}
struct Log {
    state: Materialized,
    effects: Shared,
}
impl Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, record: Record) -> cutover::Result<()> {
        boundary(&self.effects)?;
        let entry = Entry::new(
            &self.state,
            Transition::Rollback {
                record: Box::new(record.clone()),
            },
            "owner",
        )
        .unwrap();
        self.state
            .apply(&entry)
            .map_err(|e| cutover::Error::Journal(e.to_string()))?;
        if let Record::Intent { step } = record {
            self.effects.borrow_mut().intent = Some(step);
        }
        boundary(&self.effects)
    }
    fn native(&mut self, _: &cutover::Record) -> Result<(), String> {
        Ok(())
    }
}
struct Guard;
impl Authority for Guard {
    fn run<T>(&mut self, _: &Key, run: impl FnOnce() -> cutover::Result<T>) -> cutover::Result<T> {
        run()
    }
}
struct Endpoint(Shared);
impl SourceFence for Endpoint {
    fn fence(
        &mut self,
        _: &Key,
        _: &mut cutover::Journal<'_>,
    ) -> cutover::Result<cutover::DrainCounts> {
        {
            let mut e = self.0.borrow_mut();
            assert_eq!(e.intent, Some(Step::FenceDestination));
            e.events.push("save/fence/drain");
            e.privileges.clear();
        }
        boundary(&self.0)?;
        Ok(cutover::DrainCounts::default())
    }
    fn final_marker(&mut self, key: &Key) -> cutover::Result<Marker> {
        {
            let mut e = self.0.borrow_mut();
            assert_eq!(e.intent, Some(Step::FinalMarker));
            e.events.push("marker");
        }
        boundary(&self.0)?;
        Ok(Marker {
            cutover: key.cutover.clone(),
            epoch: 1,
            lsn: 42,
        })
    }
    fn unfence(&mut self, _: &Key, _: &mut cutover::Journal<'_>) -> cutover::Result<()> {
        {
            let mut e = self.0.borrow_mut();
            assert_eq!(e.intent, Some(Step::RestoreDestination));
            e.events.push("restore");
            e.privileges = e.original.clone();
        }
        boundary(&self.0)
    }
}
impl PreHook for Endpoint {
    fn run(&mut self, _: &Key) -> cutover::Result<()> {
        {
            let mut e = self.0.borrow_mut();
            assert_eq!(e.intent, Some(Step::PreHook));
            e.events.push("hook");
        }
        boundary(&self.0)
    }
}
impl Capture for Endpoint {
    fn captured(&mut self, marker: &Marker) -> cutover::Result<Option<Marker>> {
        self.0.borrow_mut().events.push("captured");
        boundary(&self.0)?;
        Ok((!self.0.borrow().pending).then(|| marker.clone()))
    }
    fn stop_at(&mut self, _: &Marker) -> cutover::Result<()> {
        {
            let mut e = self.0.borrow_mut();
            assert_eq!(e.intent, Some(Step::FinalMarker));
            e.events.push("stop");
            e.stopped = true;
        }
        boundary(&self.0)
    }
    fn restart(&mut self, _: &Key) -> cutover::Result<()> {
        {
            let mut e = self.0.borrow_mut();
            assert_eq!(e.intent, Some(Step::RestoreDestination));
            assert_eq!(e.privileges, e.original);
            e.events.push("restart");
            e.stopped = false;
        }
        boundary(&self.0)
    }
}
fn setup() -> (Key, Log, Shared) {
    let original = vec![
        "app LOGIN".into(),
        "offline NOLOGIN".into(),
        "owner:PUBLIC INSERT".into(),
        "grantor:writer UPDATE(value) WITH GRANT OPTION".into(),
    ];
    let e = Rc::new(RefCell::new(Effects {
        privileges: original.clone(),
        original,
        ..Default::default()
    }));
    let mut state = Materialized {
        epoch: 1,
        owner: "owner".into(),
        ..Default::default()
    };
    state.lifecycle.state = State::RollbackWindow;
    let mut log = Log {
        state,
        effects: e.clone(),
    };
    log.publish(Record::Admit {
        request: "request".into(),
        deadline_at_ms: u64::MAX,
    })
    .unwrap();
    e.borrow_mut().calls = 0;
    (
        Key {
            migration: "migration".into(),
            cutover: "forward".into(),
            epoch: 1,
            coordinator: "owner".into(),
        },
        log,
        e,
    )
}
fn tick(key: &Key, log: &mut Log, e: &Shared, expired: bool) -> cutover::Result<Outcome> {
    execute_next(
        key,
        if expired { u64::MAX } else { 0 },
        &mut Guard,
        &mut Endpoint(e.clone()),
        &mut Endpoint(e.clone()),
        &mut Endpoint(e.clone()),
        log,
    )
}

#[test]
fn order_intents_and_exact_marker_before_stop() {
    let (key, mut log, e) = setup();
    for _ in 0..4 {
        assert_eq!(tick(&key, &mut log, &e, false).unwrap(), Outcome::Progress);
    }
    assert_eq!(
        tick(&key, &mut log, &e, false).unwrap(),
        Outcome::ReadyForReverseApply
    );
    assert!(!log.state.rollback.as_ref().unwrap().point_of_no_return);
    assert_eq!(
        e.borrow().events,
        ["hook", "save/fence/drain", "marker", "captured", "stop"]
    );
}

#[test]
fn crash_at_every_preparation_boundary_resumes_from_durable_journal() {
    let (key, mut log, e) = setup();
    while tick(&key, &mut log, &e, false).unwrap() != Outcome::ReadyForReverseApply {}
    let count = e.borrow().calls;
    for crash in 0..count {
        let (key, mut log, e) = setup();
        e.borrow_mut().crash = Some(crash);
        for _ in 0..20 {
            if tick(&key, &mut log, &e, false) == Ok(Outcome::ReadyForReverseApply) {
                break;
            }
            log.state = serde_json::from_slice(&serde_json::to_vec(&log.state).unwrap()).unwrap();
        }
        assert!(e.borrow().crash.is_none(), "unvisited boundary {crash}");
        assert_eq!(
            log.state.rollback.as_ref().unwrap().next,
            Step::ReverseApply
        );
        assert!(e.borrow().stopped);
    }
}

#[test]
fn deadline_restoration_crashes_at_every_boundary_return_exact_privileges() {
    for prepared in 0..=4 {
        for crash in 0..8 {
            let (key, mut log, e) = setup();
            for _ in 0..prepared {
                tick(&key, &mut log, &e, false).unwrap();
            }
            log.state.rollback.as_mut().unwrap().deadline_at_ms = 1;
            e.borrow_mut().calls = 0;
            e.borrow_mut().crash = Some(crash);
            for _ in 0..10 {
                if tick(&key, &mut log, &e, true) == Ok(Outcome::Restored) {
                    break;
                }
                log.state =
                    serde_json::from_slice(&serde_json::to_vec(&log.state).unwrap()).unwrap();
            }
            assert!(e.borrow().crash.is_none());
            assert_eq!(log.state.lifecycle.state, State::RollbackWindow);
            assert_eq!(
                log.state.lifecycle.authority,
                coordinator::Authority::Destination
            );
            assert_eq!(e.borrow().privileges, e.borrow().original);
            assert!(!e.borrow().stopped);
        }
    }
}

#[test]
fn pending_capture_never_stops_and_can_expire() {
    let (key, mut log, e) = setup();
    for _ in 0..3 {
        tick(&key, &mut log, &e, false).unwrap();
    }
    e.borrow_mut().pending = true;
    assert_eq!(
        tick(&key, &mut log, &e, false),
        Err(cutover::Error::Pending)
    );
    assert!(!e.borrow().stopped);
    log.state.rollback.as_mut().unwrap().deadline_at_ms = 1;
    assert_eq!(tick(&key, &mut log, &e, true).unwrap(), Outcome::Restored);
}

#[test]
fn ponr_and_eligibility_loss_fail_closed() {
    let (key, mut log, e) = setup();
    log.state.rollback.as_mut().unwrap().point_of_no_return = true;
    assert!(tick(&key, &mut log, &e, true).is_err());
    assert!(e.borrow().events.is_empty());
    log.state.rollback.as_mut().unwrap().point_of_no_return = false;
    log.state.rollback_eligibility_ended =
        Some(coordinator::reverse::EligibilityEnd::SourceFenceBreach);
    assert!(tick(&key, &mut log, &e, true).is_err());
    assert!(e.borrow().events.is_empty());
}

#[test]
fn receipt_keys_separate_forward_and_each_rollback_request() {
    let (key, _, _) = setup();
    assert_ne!(destination_key(&key, "request").migration, key.migration);
    assert_ne!(
        destination_key(&key, "request"),
        destination_key(&key, "retry")
    );
    assert_eq!(
        destination_key(&key, "request"),
        destination_key(&key, "request")
    );
}

// Exercise the real swapped PG Source, not just the effect-level fault model.
#[test]
fn pg_source_fake_connection_replays_native_boundaries_and_restores_exactly() {
    use std::{collections::BTreeSet, time::Duration};
    #[derive(Default)]
    struct Db {
        saved: Option<fence::Fence>,
        stored: Option<fence::Fence>,
        revoked: bool,
        login: bool,
        save_durable: bool,
    }
    struct Connection(Rc<RefCell<Db>>);
    impl fence::Connection for Connection {
        fn query(&mut self, sql: &str, _: Duration) -> Result<fence::Rows, String> {
            let mut db = self.0.borrow_mut();
            if sql.starts_with("SELECT to_regclass") {
                return Ok(vec![vec!["true".into()]]);
            }
            if sql.starts_with("SELECT state FROM") {
                return Ok(db
                    .stored
                    .as_ref()
                    .map(|s| vec![vec![serde_json::to_string(s).unwrap()]])
                    .unwrap_or_default());
            }
            if sql.starts_with("INSERT INTO _skedaddle.fence_state") {
                assert!(db.save_durable);
                db.stored = db.saved.clone();
                return Ok(vec![]);
            }
            if sql.starts_with("SELECT 'role'") {
                let mut rows = vec![
                    vec![
                        "role",
                        "app",
                        if db.login { "true" } else { "false" },
                        "false",
                        "false",
                        "",
                        "",
                        "",
                        "",
                    ],
                    vec!["role", "owner", "false", "false", "false", "", "", "", ""],
                    vec!["table", "public", "items", "owner", "", "", "", "", ""],
                ];
                if !db.revoked {
                    rows.push(vec![
                        "grant",
                        "public",
                        "items",
                        "owner",
                        "value",
                        "role",
                        "app",
                        "owner",
                        "UPDATE:true",
                    ]);
                    rows.push(vec![
                        "grant",
                        "public",
                        "items",
                        "owner",
                        "",
                        "public",
                        "",
                        "owner",
                        "INSERT:false",
                    ]);
                }
                return Ok(rows
                    .into_iter()
                    .map(|r| r.into_iter().map(str::to_string).collect())
                    .collect());
            }
            if sql.starts_with("SELECT pg_current_wal_insert_lsn") {
                return Ok(vec![vec!["0/2A".into()]]);
            }
            if sql.starts_with("BEGIN;") {
                assert!(
                    db.stored.is_some(),
                    "privilege mutation before durable snapshot"
                );
                if sql.contains("NOLOGIN") {
                    db.login = false;
                }
                if sql.contains(" REVOKE ") {
                    db.revoked = true;
                }
                if sql.contains(" GRANT ") {
                    assert!(sql.contains("WITH GRANT OPTION"));
                    assert!(sql.contains("UPDATE (\"value\")"));
                    assert!(sql.contains(" TO PUBLIC"));
                    db.revoked = false;
                }
                if sql.contains("\"app\" LOGIN") {
                    db.login = true;
                }
                return Ok(vec![]);
            }
            if sql.starts_with("SELECT epoch::text")
                || sql.starts_with("SELECT 'backend'")
                || sql.starts_with("SELECT pid::text")
                || sql == crate::pg::ddl::ADMIN_INSTALLATION_SQL
            {
                return Ok(vec![]);
            }
            panic!("unexpected SQL: {sql}");
        }
    }
    fn source(
        db: Rc<RefCell<Db>>,
    ) -> cutover::pg::Source<Connection, impl FnMut() -> Result<(), String>> {
        cutover::pg::Source {
            connection: Connection(db),
            state: fence::Fence::new(fence::FenceConfig {
                epoch: 1,
                applications: BTreeSet::from(["app".into()]),
                tables: vec![fence::Table {
                    schema: "public".into(),
                    name: "items".into(),
                    owner: "owner".into(),
                }],
                acknowledged: BTreeSet::from([
                    fence::ResidualPrincipal {
                        role: "owner".into(),
                        kind: fence::ResidualKind::TableOwner,
                    },
                    fence::ResidualPrincipal {
                        role: "owner".into(),
                        kind: fence::ResidualKind::OutOfScopeWriter,
                    },
                ]),
                deadline_ms: 1000,
            })
            .unwrap(),
            drained: cutover::DrainCounts::default(),
            pre_fence: || Ok(()),
        }
    }
    let (key, _, _) = setup();
    let key = destination_key(&key, "request");
    // Source has 23 native boundaries for this fixture; first measure rather
    // than coupling the fault loop to its implementation's step count.
    let mut boundaries = 0;
    for fault in std::iter::once(None).chain((0..40).map(Some)) {
        if fault.is_some_and(|f| f >= boundaries) {
            break;
        }
        let db = Rc::new(RefCell::new(Db {
            login: true,
            ..Default::default()
        }));
        let mut src = source(db.clone());
        let mut records = Vec::new();
        let mut calls = 0;
        let mut failed = false;
        loop {
            let outcome = src.fence(&key, &mut |record| {
                let crash = fault == Some(calls) && !failed;
                calls += 1;
                if crash {
                    failed = true;
                    return Err("crash".into());
                }
                if let cutover::Record::Source(fence::Intent::Save(saved)) = record {
                    db.borrow_mut().saved = Some(*saved.clone());
                    db.borrow_mut().save_durable = true;
                }
                records.push(record.clone());
                Ok(())
            });
            if outcome.is_ok() {
                break;
            }
            assert!(failed, "{outcome:?}");
            src = source(db.clone());
            replay_native(&mut src, &key, &records).unwrap();
        }
        assert!(!db.borrow().login && db.borrow().revoked);
        assert!(
            src.unfence(&key, &mut |_| Err("journal unavailable".into()))
                .is_err()
        );
        assert!(!db.borrow().login && db.borrow().revoked);
        src.unfence(&key, &mut |_| Ok(())).unwrap();
        assert!(db.borrow().login && !db.borrow().revoked);
        if fault.is_none() {
            boundaries = calls;
        } else {
            assert!(failed);
        }
    }
}

#[test]
fn destination_pg_fence_restore_exact_privileges() {
    use fence::Connection as _;
    use std::{collections::BTreeSet, io::Write, time::Duration};
    let (Ok(url), Ok(config)) = (
        std::env::var("SKEDADDLE_ROLLBACK_PG_DESTINATION_URL"),
        std::env::var("SKEDADDLE_TEST_CONFIG"),
    ) else {
        std::io::stderr().write_all(b"SKIP destination_pg_fence_restore_exact_privileges: set SKEDADDLE_ROLLBACK_PG_DESTINATION_URL and SKEDADDLE_TEST_CONFIG (disposable PG16-18 database, superuser, logical WAL, TLS CA).\n").unwrap();
        return;
    };
    struct Pg {
        rt: tokio::runtime::Runtime,
        client: tokio_postgres::Client,
    }
    impl fence::Connection for Pg {
        fn query(&mut self, sql: &str, timeout: Duration) -> Result<fence::Rows, String> {
            self.rt.block_on(async {
                let messages = tokio::time::timeout(timeout, self.client.simple_query(sql))
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;
                Ok(messages
                    .into_iter()
                    .filter_map(|m| match m {
                        tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                            (0..r.len())
                                .map(|i| r.get(i).unwrap_or("").into())
                                .collect(),
                        ),
                        _ => None,
                    })
                    .collect())
            })
        }
    }
    let harness: serde_json::Value =
        serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let ca = harness["ca_pem"]
        .as_str()
        .or_else(|| harness["replication"]["ca_pem"].as_str())
        .expect("harness TLS CA");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tls = crate::pg::tls::Config::new(url.parse().unwrap(), ca);
    let (client, driver, _) = rt
        .block_on(crate::pg::tls::connect(
            &tls,
            &crate::security::TlsPolicy::Required,
        ))
        .unwrap();
    rt.spawn(async move {
        let _ = driver.await;
    });
    let mut db = Pg { rt, client };
    let timeout = Duration::from_secs(30);
    assert_eq!(
        db.query(
            "SELECT (to_regnamespace('_skedaddle') IS NULL)::text",
            timeout
        )
        .unwrap()[0][0],
        "true",
        "requires an empty disposable destination"
    );
    let version = db.query("SHOW server_version_num", timeout).unwrap()[0][0]
        .parse()
        .unwrap();
    let id = format!("rd{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let app = format!("{id}_app");
    let offline = format!("{id}_offline");
    db.query(&format!("CREATE ROLE {app} LOGIN; CREATE ROLE {offline} NOLOGIN; CREATE TABLE public.{id}(id int,value text); GRANT INSERT ON public.{id} TO PUBLIC; GRANT UPDATE(value) ON public.{id} TO {app} WITH GRANT OPTION;"), timeout).unwrap();
    let ddl = crate::pg::ddl::TriggerSql::new(&id, version).unwrap();
    db.query(&ddl.install, timeout).unwrap();
    let owner = db.query("SELECT current_user::text", timeout).unwrap()[0][0].clone();
    let roles = db
        .query("SELECT rolname::text FROM pg_roles", timeout)
        .unwrap();
    let mut acknowledged = BTreeSet::new();
    for row in roles {
        for kind in [
            fence::ResidualKind::Superuser,
            fence::ResidualKind::BypassRls,
            fence::ResidualKind::TableOwner,
            fence::ResidualKind::OutOfScopeWriter,
        ] {
            acknowledged.insert(fence::ResidualPrincipal {
                role: row[0].clone(),
                kind,
            });
        }
    }
    let state = fence::Fence::new(fence::FenceConfig {
        epoch: 1,
        applications: BTreeSet::from([app.clone(), offline.clone()]),
        tables: vec![fence::Table {
            schema: "public".into(),
            name: id.clone(),
            owner,
        }],
        acknowledged,
        deadline_ms: 30000,
    })
    .unwrap();
    let mut source = cutover::pg::Source {
        connection: db,
        state,
        drained: cutover::DrainCounts::default(),
        pre_fence: || Ok(()),
    };
    let (key, _, _) = setup();
    let scoped = destination_key(&key, &id);
    let mut records = Vec::new();
    source
        .fence(&scoped, &mut |r| {
            records.push(r.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(
        source
            .connection
            .query(
                &format!("SELECT rolcanlogin::text FROM pg_roles WHERE rolname='{app}'"),
                timeout
            )
            .unwrap()[0][0],
        "false"
    );
    let saved = source.state.saved.clone();
    replay_native(&mut source, &scoped, &records).unwrap();
    assert_eq!(source.state.saved, saved);
    source.unfence(&scoped, &mut |_| Ok(())).unwrap(); // verifies full ACL attributes internally
    assert_eq!(source.connection.query(&format!("SELECT rolname::text,rolcanlogin::text FROM pg_roles WHERE rolname IN ('{app}','{offline}') ORDER BY rolname"), timeout).unwrap(), vec![vec![app.clone(), "true".into()], vec![offline.clone(), "false".into()]]);
    source.connection.query(&ddl.remove, timeout).unwrap();
    source.connection.query(&format!("DROP TABLE public.{id}; DROP ROLE {app}; DROP ROLE {offline}; DROP SCHEMA _skedaddle CASCADE;"), timeout).unwrap();
}
