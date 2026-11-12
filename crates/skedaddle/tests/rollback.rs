//! Pure authority/fault models plus opt-in tests against two disposable PG DBs.
mod support;
use skedaddle::{
    cutover::{self, Destination, SourceFence},
    pg::{activate, fence, reverse},
    rollback::{self, Finalize, Inventory, Object, ObjectKind, Side},
};
use std::{cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};

fn key(id: &str) -> cutover::Key {
    cutover::Key {
        migration: id.into(),
        cutover: id.into(),
        epoch: 1,
        coordinator: "rollback-test".into(),
    }
}
fn coverage() -> cutover::Coverage {
    cutover::Coverage {
        total: 1,
        verified: 1,
        busy: 0,
        mismatched: 0,
        excluded: 0,
    }
}
fn saved() -> fence::SavedState {
    let table = fence::Table {
        schema: "public".into(),
        name: "data".into(),
        owner: "owner".into(),
    };
    fence::SavedState {
        authority: None,
        catalog: fence::Catalog {
            roles: vec![
                fence::Role {
                    name: "app".into(),
                    login: true,
                    superuser: false,
                    bypass_rls: false,
                },
                fence::Role {
                    name: "offline".into(),
                    login: false,
                    superuser: false,
                    bypass_rls: false,
                },
            ],
            tables: vec![table.clone()],
            memberships: vec![fence::Membership {
                member: "app".into(),
                role: "writer".into(),
            }],
            grants: vec![
                fence::Grant {
                    table: table.clone(),
                    column: Some("value".into()),
                    grantee: fence::Grantee::Role("writer".into()),
                    grantor: "owner".into(),
                    privilege: fence::Privilege::Update,
                    grant_option: true,
                },
                fence::Grant {
                    table,
                    column: None,
                    grantee: fence::Grantee::Public,
                    grantor: "owner".into(),
                    privilege: fence::Privilege::Insert,
                    grant_option: false,
                },
            ],
        },
    }
}

struct Guard;
impl cutover::Authority for Guard {
    fn run<T>(
        &mut self,
        _: &cutover::Key,
        f: impl FnOnce() -> cutover::Result<T>,
    ) -> cutover::Result<T> {
        f()
    }
}

// This models transfer authority, not a SQL transport. Repeated effects retain
// one receipt/marker/audit per transfer. Real PG adapters are tested below.
#[derive(Default)]
struct Model {
    live: [bool; 2],
    restored: Option<fence::SavedState>,
    receipt: Option<u64>,
    marker: Option<cutover::Marker>,
    hook: bool,
    audit: Option<cutover::Audit>,
    synced: bool,
    wrong_marker: bool,
    marker_not_applied: bool,
    bad_verification: bool,
    revoke_at_restore: Option<reverse::Eligibility>,
}
type Shared = Rc<RefCell<Model>>;
struct Fencer(Shared, usize);
impl SourceFence for Fencer {
    fn fence(
        &mut self,
        _: &cutover::Key,
        _: &mut cutover::Journal<'_>,
    ) -> cutover::Result<cutover::DrainCounts> {
        self.0.borrow_mut().live[self.1] = false;
        Ok(cutover::DrainCounts::default())
    }
    fn final_marker(&mut self, k: &cutover::Key) -> cutover::Result<cutover::Marker> {
        let mut m = self.0.borrow_mut();
        assert!(!m.live[self.1]);
        Ok(m.marker
            .get_or_insert_with(|| cutover::Marker {
                cutover: k.cutover.clone(),
                epoch: k.epoch,
                lsn: 42,
            })
            .clone())
    }
    fn unfence(&mut self, _: &cutover::Key, _: &mut cutover::Journal<'_>) -> cutover::Result<()> {
        self.0.borrow_mut().live[self.1] = true;
        Ok(())
    }
}
struct Target(Shared, usize);
impl Destination for Target {
    fn activated(&mut self, _: &cutover::Key) -> cutover::Result<Option<u64>> {
        Ok(self.0.borrow().receipt)
    }
    fn prove_fenced(
        &mut self,
        _: &cutover::Key,
        _: &mut cutover::Journal<'_>,
    ) -> cutover::Result<()> {
        assert!(!self.0.borrow().live[self.1]);
        Ok(())
    }
    fn readiness(&mut self, k: &cutover::Key, j: &mut cutover::Journal<'_>) -> cutover::Result<()> {
        self.prove_fenced(k, j)
    }
    fn activate(
        &mut self,
        _: &cutover::Key,
        marker: &cutover::Marker,
        _: &mut cutover::Journal<'_>,
        authorize: &mut dyn FnMut() -> cutover::Result<()>,
    ) -> cutover::Result<u64> {
        if let Some(receipt) = self.0.borrow().receipt {
            return Ok(receipt);
        }
        if let Some(e) = self.0.borrow_mut().revoke_at_restore.take() {
            e.end(reverse::Violation::IrreversibleMapping).unwrap();
        }
        authorize()?;
        let mut m = self.0.borrow_mut();
        assert!(!m.live[1 - self.1]);
        assert_eq!(m.marker.as_ref(), Some(marker));
        assert!(m.synced);
        m.live[self.1] = true;
        m.restored = Some(saved());
        m.receipt = Some(10);
        Ok(10)
    }
    fn post_activate(
        &mut self,
        _: &cutover::Key,
        _: &mut cutover::Journal<'_>,
    ) -> cutover::Result<()> {
        self.0.borrow_mut().hook = true;
        Ok(())
    }
}
struct Progress(Shared);
impl cutover::Progress for Progress {
    fn buffered(&mut self, _: &cutover::Marker) -> cutover::Result<Option<cutover::Marker>> {
        let m = self.0.borrow();
        let mut marker = m.marker.clone();
        if m.wrong_marker {
            marker.as_mut().unwrap().cutover = "unrelated".into();
        }
        Ok(marker)
    }
    fn applied(&mut self, _: &cutover::Marker) -> cutover::Result<Option<cutover::Marker>> {
        let m = self.0.borrow();
        Ok(if m.marker_not_applied {
            None
        } else {
            m.marker.clone()
        })
    }
}
struct Verifier(Shared);
impl cutover::Verifier for Verifier {
    fn boundary(&mut self, marker: &cutover::Marker) -> cutover::Result<cutover::VerifiedBoundary> {
        let mut c = coverage();
        if self.0.borrow().bad_verification {
            c.mismatched = 1;
        }
        Ok(cutover::VerifiedBoundary {
            marker: marker.clone(),
            coverage: c,
        })
    }
}
struct Sequences(Shared);
impl cutover::Sequences for Sequences {
    fn sync(&mut self, _: &cutover::Marker) -> cutover::Result<()> {
        self.0.borrow_mut().synced = true;
        Ok(())
    }
}
struct ReverseReady;
impl cutover::Reverse for ReverseReady {
    fn confirm(&mut self, _: &cutover::Key, _: &cutover::Marker) -> cutover::Result<()> {
        Ok(())
    }
}
struct Status(Shared);
impl cutover::Status for Status {
    fn readiness(&mut self, _: &cutover::Key) -> cutover::Result<()> {
        Ok(())
    }
    fn breached(&mut self, _: &cutover::Key) -> cutover::Result<bool> {
        Ok(false)
    }
    fn now_ms(&mut self) -> cutover::Result<u64> {
        Ok(1)
    }
    fn audit(&mut self, _: &cutover::Key, audit: &cutover::Audit) -> cutover::Result<()> {
        self.0.borrow_mut().audit = Some(audit.clone());
        Ok(())
    }
}

fn tick(
    state: &mut rollback::State,
    m: &Shared,
    e: &reverse::Eligibility,
    journal: &mut rollback::Journal<'_>,
) -> cutover::Result<()> {
    state.execute_next(
        &mut Guard,
        e,
        &mut cutover::Components {
            source: &mut Fencer(m.clone(), 1),
            destination: &mut Target(m.clone(), 0),
            progress: &mut Progress(m.clone()),
            verifier: &mut Verifier(m.clone()),
            sequences: &mut Sequences(m.clone()),
            reverse: &mut ReverseReady,
            status: &mut Status(m.clone()),
        },
        journal,
    )
}
fn model() -> Shared {
    Rc::new(RefCell::new(Model {
        live: [false, true],
        ..Default::default()
    }))
}
fn recover(initial: &rollback::State, records: &[rollback::Record]) -> rollback::State {
    let mut state = initial.clone();
    for r in records {
        match r {
            rollback::Record::Transfer(
                cutover::Record::Intent {
                    state: transfer, ..
                }
                | cutover::Record::Checkpoint(transfer),
            ) => state.transfer = transfer.clone(),
            rollback::Record::Refused { state: refused, .. } => state = refused.clone(),
            _ => (),
        }
    }
    // Exercise the durable format rather than retaining pointers to the machine.
    serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap()
}
fn finish(state: &mut rollback::State, m: &Shared, e: &reverse::Eligibility) {
    for _ in 0..20 {
        if state.transfer.next == cutover::Step::Complete {
            return;
        }
        tick(state, m, e, &mut |_| Ok(())).unwrap();
        assert!(!m.borrow().live.iter().all(|x| *x));
    }
    panic!("rollback did not complete");
}

#[test]
fn crash_at_every_rollback_intent_and_checkpoint_resumes() {
    let initial = rollback::State::new(key("faults")).unwrap();
    let e = reverse::Eligibility::new(|_| Ok(()));
    let mut baseline = initial.clone();
    let m = model();
    let mut count = 0;
    while baseline.transfer.next != cutover::Step::Complete {
        tick(&mut baseline, &m, &e, &mut |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(count, 22); // eleven steps, each with intent and checkpoint
    for crash in 0..count {
        for lost_reply in [false, true] {
            let m = model();
            let mut state = initial.clone();
            let mut records = Vec::new();
            let mut index = 0;
            loop {
                let result = tick(&mut state, &m, &e, &mut |r| {
                    let fail = index == crash;
                    index += 1;
                    if !fail || lost_reply {
                        records.push(r.clone());
                    }
                    if fail {
                        Err("simulated process loss".into())
                    } else {
                        Ok(())
                    }
                });
                if result.is_err() {
                    break;
                }
            }
            state = recover(&initial, &records);
            finish(&mut state, &m, &e);
            let m = m.borrow();
            assert_eq!(m.live, [true, false], "crash={crash}");
            assert_eq!(m.restored, Some(saved()));
            assert!(m.hook && m.audit.is_some());
        }
    }
}

#[test]
fn rollback_refused_with_durable_first_reason() {
    let e = reverse::Eligibility::new(|_| Ok(()));
    e.end(reverse::Violation::Extension {
        name: "unsupported".into(),
    })
    .unwrap();
    let mut s = rollback::State::new(key("refused")).unwrap();
    let m = model();
    let mut records = Vec::new();
    assert!(
        tick(&mut s, &m, &e, &mut |r| {
            records.push(r.clone());
            Ok(())
        })
        .is_err()
    );
    assert_eq!(m.borrow().live, [false, true]);
    assert_eq!(s.refused, e.first_violation());
    let mut resumed = recover(&s, &records);
    assert!(
        tick(
            &mut resumed,
            &m,
            &reverse::Eligibility::new(|_| Ok(())),
            &mut |_| Ok(())
        )
        .is_err()
    );
}

#[test]
fn eligibility_rechecked_immediately_before_source_access() {
    let e = reverse::Eligibility::new(|_| Ok(()));
    let m = model();
    m.borrow_mut().revoke_at_restore = Some(e.clone());
    let mut s = rollback::State::new(key("late-refusal")).unwrap();
    while s.transfer.next != cutover::Step::Activate {
        tick(&mut s, &m, &e, &mut |_| Ok(())).unwrap();
    }
    assert!(tick(&mut s, &m, &e, &mut |_| Ok(())).is_err());
    assert_eq!(m.borrow().live, [false, false]);
    assert!(s.refused.is_some());
}

#[test]
fn committed_restore_resumes_after_eligibility_ends_and_reply_is_lost() {
    let e = reverse::Eligibility::new(|_| Ok(()));
    let m = model();
    let mut s = rollback::State::new(key("ambiguous-restore")).unwrap();
    while s.transfer.next != cutover::Step::Activate {
        tick(&mut s, &m, &e, &mut |_| Ok(())).unwrap();
    }
    assert!(
        tick(&mut s, &m, &e, &mut |record| {
            if matches!(
                record,
                rollback::Record::Transfer(cutover::Record::Checkpoint(_))
            ) {
                e.end(reverse::Violation::IrreversibleMapping).unwrap();
                return Err("lost acknowledgement of committed restoration".into());
            }
            Ok(())
        })
        .is_err()
    );
    assert!(s.refused.is_none());
    assert_eq!(m.borrow().live, [true, false]);
    finish(&mut s, &m, &e);
    assert!(m.borrow().audit.is_some());
}

#[test]
fn boundary_requires_the_marker_itself_and_complete_verification() {
    let e = reverse::Eligibility::new(|_| Ok(()));
    let m = model();
    let mut s = rollback::State::new(key("marker")).unwrap();
    while s.transfer.next != cutover::Step::Boundary {
        tick(&mut s, &m, &e, &mut |_| Ok(())).unwrap();
    }
    m.borrow_mut().wrong_marker = true;
    assert_eq!(
        tick(&mut s, &m, &e, &mut |_| Ok(())),
        Err(cutover::Error::Pending)
    );
    m.borrow_mut().wrong_marker = false;
    m.borrow_mut().marker_not_applied = true;
    assert_eq!(
        tick(&mut s, &m, &e, &mut |_| Ok(())),
        Err(cutover::Error::Pending)
    );
    m.borrow_mut().marker_not_applied = false;
    m.borrow_mut().bad_verification = true;
    assert_eq!(
        tick(&mut s, &m, &e, &mut |_| Ok(())),
        Err(cutover::Error::Verification)
    );
    assert_eq!(m.borrow().live, [false, false]);
}

#[test]
fn source_privileges_restored_exactly() {
    let m = model();
    let mut s = rollback::State::new(key("acl")).unwrap();
    finish(&mut s, &m, &reverse::Eligibility::new(|_| Ok(())));
    assert_eq!(m.borrow().restored.as_ref().unwrap(), &saved());
    let sql = fence::restoration_sql(&saved(), &BTreeSet::from(["app".into(), "offline".into()]))
        .unwrap();
    assert!(sql.contains("WITH GRANT OPTION") && sql.contains("NOLOGIN") && sql.contains("PUBLIC"));
}

fn manifest() -> Inventory {
    [Side::Source, Side::Destination]
        .into_iter()
        .flat_map(|side| {
            [
                (ObjectKind::Slot, "gd49_test"),
                (ObjectKind::Publication, "gd49_test"),
                (ObjectKind::EventTrigger, "gd49_test"),
                (ObjectKind::ReplicationOrigin, "gd49_test"),
                (ObjectKind::Schema, "_skedaddle"),
            ]
            .into_iter()
            .map(move |(kind, name)| Object {
                side,
                kind,
                name: name.into(),
            })
        })
        .collect()
}
#[test]
fn finalize_without_flag_plans_no_drops() {
    let objects = manifest();
    let f = Finalize::new(key("retain"), false, objects.clone()).unwrap();
    assert!(f.planned_drops(&objects).is_empty());
    assert_eq!(objects.difference(&f.planned_drops(&objects)).count(), 10);
}
#[test]
fn finalize_with_flag_has_empty_installed_inventory_diff() {
    let objects = manifest();
    let f = Finalize::new(key("remove"), true, objects.clone()).unwrap();
    assert!(
        objects
            .difference(&f.planned_drops(&objects))
            .next()
            .is_none()
    );
    assert!(f.planned_drops(&Inventory::new()).is_empty());
}

#[test]
fn repeated_cutover_rollback_cycles() {
    let m = model();
    m.borrow_mut().live = [true, false];
    for cycle in 0..10 {
        let mut forward = cutover::State::new(key(&format!("forward-{cycle}"))).unwrap();
        while forward.next != cutover::Step::Complete {
            forward
                .execute_next(
                    &mut Guard,
                    &mut cutover::Components {
                        source: &mut Fencer(m.clone(), 0),
                        destination: &mut Target(m.clone(), 1),
                        progress: &mut Progress(m.clone()),
                        verifier: &mut Verifier(m.clone()),
                        sequences: &mut Sequences(m.clone()),
                        reverse: &mut ReverseReady,
                        status: &mut Status(m.clone()),
                    },
                    &mut |_| Ok(()),
                )
                .unwrap();
        }
        assert_eq!(m.borrow().live, [false, true]);
        reset_receipts(&m);
        let mut rollback = rollback::State::new(key(&format!("rollback-{cycle}"))).unwrap();
        finish(&mut rollback, &m, &reverse::Eligibility::new(|_| Ok(())));
        assert_eq!(m.borrow().live, [true, false]);
        assert!(serde_json::to_vec(&rollback).unwrap().len() < 1500);
        reset_receipts(&m);
    }
}
fn reset_receipts(m: &Shared) {
    let mut m = m.borrow_mut();
    m.receipt = None;
    m.marker = None;
    m.audit = None;
    m.hook = false;
    m.synced = false;
}

// Each journal callback is a crash boundary, including the checkpoint after effects.
#[test]
fn finalize_crash_at_every_journal_boundary_recovers_and_never_drops_before_intent() {
    use rollback::{FinalizeRecord, FinalizeStep};
    #[derive(Default)]
    struct Evidence {
        intent: bool,
        drops: Inventory,
        objects: Inventory,
    }
    struct Catalog(Rc<RefCell<Evidence>>, Side);
    impl fence::Connection for Catalog {
        fn query(&mut self, sql: &str, _: Duration) -> Result<fence::Rows, String> {
            let mut e = self.0.borrow_mut();
            assert!(e.intent, "catalog access before durable intent");
            let column = |kind| match kind {
                ObjectKind::Slot => "slot_name",
                ObjectKind::Publication => "pubname",
                ObjectKind::EventTrigger => "evtname",
                ObjectKind::ReplicationOrigin => "roname",
                ObjectKind::Schema => "nspname",
            };
            if sql.contains("DROP ")
                || sql.contains("pg_drop_replication_slot(slot_name)")
                || sql.contains("pg_replication_origin_drop(roname)")
            {
                let objects: Vec<_> = e
                    .objects
                    .iter()
                    .filter(|o| {
                        o.side == self.1
                            && (sql.contains(&format!("\"{}\"", o.name))
                                || sql.contains(&format!("E'{}'", o.name)))
                    })
                    .cloned()
                    .collect();
                // Kind is disambiguated by SQL as all fixture kinds use one name.
                for object in objects {
                    let matches = match object.kind {
                        ObjectKind::Slot => sql.contains("pg_drop_replication_slot"),
                        ObjectKind::Publication => sql.contains("DROP PUBLICATION"),
                        ObjectKind::EventTrigger => sql.contains("DROP EVENT TRIGGER"),
                        ObjectKind::ReplicationOrigin => sql.contains("pg_replication_origin_drop"),
                        ObjectKind::Schema => sql.contains("DROP SCHEMA"),
                    };
                    if matches {
                        assert!(e.drops.contains(&object), "drop before journal intent");
                        e.objects.remove(&object);
                    }
                }
                return Ok(vec![]);
            }
            Ok(e.objects
                .iter()
                .filter(|o| {
                    o.side == self.1
                        && sql.starts_with(&format!("SELECT {}::text", column(o.kind)))
                        && sql.contains(&format!("E'{}'", o.name))
                })
                .map(|o| vec![o.name.clone()])
                .collect())
        }
    }
    for crash in 0..18 {
        let evidence = Rc::new(RefCell::new(Evidence {
            objects: manifest(),
            ..Default::default()
        }));
        let initial = Finalize::new(key("faults"), true, manifest()).unwrap();
        let mut state = initial.clone();
        let mut durable = initial;
        let mut calls = 0;
        let mut faulted = false;
        while state.next != FinalizeStep::Complete {
            let result = state.execute_next(
                &mut Guard,
                &mut Catalog(evidence.clone(), Side::Source),
                &mut Catalog(evidence.clone(), Side::Destination),
                &mut |_| {
                    assert!(evidence.borrow().intent);
                    Ok(())
                },
                &mut |record| {
                    let fail = calls == crash && !faulted;
                    calls += 1;
                    if fail {
                        faulted = true;
                        return Err("injected crash".into());
                    }
                    match record {
                        FinalizeRecord::Intent(_) => evidence.borrow_mut().intent = true,
                        FinalizeRecord::Drop(object) => {
                            evidence.borrow_mut().drops.insert(object.clone());
                        }
                        FinalizeRecord::Checkpoint(saved) => durable = saved.clone(),
                    }
                    Ok(())
                },
            );
            if result.is_err() {
                assert!(faulted);
                state = serde_json::from_slice(&serde_json::to_vec(&durable).unwrap()).unwrap();
            }
        }
        assert!(faulted, "boundary {crash} was not exercised");
        assert!(state.leftovers().unwrap().is_empty());
        assert!(evidence.borrow().objects.is_empty());
    }
}

// These tests deliberately use two disposable databases, never Docker directly.
// The fixture consumes real pgoutput INSERT/message frames for its single table;
// it is not the general reverse capture/applier qualification (P11a owns that).
mod real_pg {
    use super::*;
    use fence::Connection;
    use std::{io::Write, sync::Mutex};

    static SERIAL: Mutex<()> = Mutex::new(());
    fn urls(test: &str) -> Option<(String, String)> {
        let source = std::env::var("SKEDADDLE_ROLLBACK_PG_SOURCE_URL").ok();
        let destination = std::env::var("SKEDADDLE_ROLLBACK_PG_DESTINATION_URL").ok();
        match (source, destination) {
            (Some(s), Some(d)) if !s.is_empty() && !d.is_empty() => Some((s, d)),
            _ => {
                // Bypass libtest capture so the skip is visible without --nocapture.
                std::io::stderr().write_all(format!("SKIP {test}: set SKEDADDLE_ROLLBACK_PG_SOURCE_URL and SKEDADDLE_ROLLBACK_PG_DESTINATION_URL to two disposable PostgreSQL 16+ databases (superuser, wal_level=logical). Docker runs belong to the coordinator.\n").as_bytes()).expect("write loud skip");
                None
            }
        }
    }
    fn lit(s: &str) -> String {
        format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
    }
    #[derive(Clone)]
    struct Pg {
        rt: Rc<tokio::runtime::Runtime>,
        client: Rc<tokio_postgres::Client>,
    }
    impl Pg {
        fn connect(rt: Rc<tokio::runtime::Runtime>, url: &str) -> Self {
            let (client, connection) = rt
                .block_on(tls_connect(url))
                .expect("connect disposable PostgreSQL");
            rt.spawn(async move {
                let _ = connection.await;
            });
            Self {
                rt,
                client: Rc::new(client),
            }
        }
        fn sql(&mut self, sql: &str) -> fence::Rows {
            self.query(sql, Duration::from_secs(30)).unwrap()
        }
    }
    impl Connection for Pg {
        fn query(&mut self, sql: &str, timeout: Duration) -> Result<fence::Rows, String> {
            self.rt.block_on(async {
                let messages = tokio::time::timeout(timeout, self.client.simple_query(sql))
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;
                Ok(messages
                    .into_iter()
                    .filter_map(|message| match message {
                        tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                            (0..row.len())
                                .map(|i| row.get(i).unwrap_or("").to_owned())
                                .collect(),
                        ),
                        _ => None,
                    })
                    .collect())
            })
        }
    }
    struct Fixture {
        source: Pg,
        destination: Pg,
        source_fence: fence::Fence,
        destination_fence: fence::Fence,
        source_role: String,
        destination_role: String,
        table: String,
        config: reverse::Config,
        id: String,
    }
    fn fence_config(role: &str, table: fence::Table) -> fence::FenceConfig {
        fence::FenceConfig {
            epoch: 1,
            applications: BTreeSet::from([role.into()]),
            tables: vec![table],
            acknowledged: BTreeSet::new(),
            deadline_ms: 30_000,
        }
    }
    fn acknowledge_admins(state: &mut fence::Fence) {
        if let Some(saved) = &state.saved {
            state.config.acknowledged = fence::classify_residuals(
                &saved.catalog,
                &state.config.applications,
                &BTreeSet::new(),
            )
            .into_iter()
            .filter(|r| r.principal.kind != fence::ResidualKind::InScopeWriter)
            .map(|r| r.principal)
            .collect();
        }
    }
    impl Fixture {
        fn new(source_url: &str, destination_url: &str) -> Self {
            let rt = Rc::new(tokio::runtime::Runtime::new().unwrap());
            let mut source = Pg::connect(rt.clone(), source_url);
            let mut destination = Pg::connect(rt, destination_url);
            // Never adopt or remove another installation's schema.
            for db in [&mut source, &mut destination] {
                assert_eq!(
                    db.sql("SELECT (to_regnamespace('_skedaddle') IS NULL)::text")[0][0],
                    "true",
                    "requires disposable databases without _skedaddle"
                );
                assert!(
                    db.sql("SHOW server_version_num")[0][0]
                        .parse::<u32>()
                        .unwrap()
                        >= 160000
                );
            }
            assert_ne!(source_url, destination_url, "use two separate databases");
            let id = format!("rb{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
            let table = format!("{id}_data");
            let source_role = format!("{id}_src");
            let destination_role = format!("{id}_dst");
            let mut tables = Vec::new();
            for (db, role) in [
                (&mut source, &source_role),
                (&mut destination, &destination_role),
            ] {
                let owner = db.sql("SELECT current_user::text")[0][0].clone();
                db.sql(&format!("CREATE ROLE {role} LOGIN; CREATE TABLE public.{table}(id bigint PRIMARY KEY,value text NOT NULL); INSERT INTO public.{table} VALUES(1,'snapshot'); GRANT SELECT,INSERT,DELETE ON public.{table} TO {role}; GRANT UPDATE(value) ON public.{table} TO {role} WITH GRANT OPTION;"));
                tables.push(fence::Table {
                    schema: "public".into(),
                    name: table.clone(),
                    owner,
                });
            }
            let mut source_fence =
                fence::Fence::new(fence_config(&source_role, tables[0].clone())).unwrap();
            source_fence
                .bind_migration(&format!("{id}_forward"))
                .unwrap();
            while source_fence.next != fence::Step::Complete {
                source_fence
                    .execute_next(&mut source, &mut |_| Ok(()), &mut || Ok(()))
                    .unwrap();
                acknowledge_admins(&mut source_fence);
            }
            // Establish reverse setup before the first destination-era write.
            let mut destination_fence =
                fence::Fence::new(fence_config(&destination_role, tables[1].clone())).unwrap();
            destination_fence
                .bind_migration(&format!("{id}_setup"))
                .unwrap();
            while destination_fence.next != fence::Step::Complete {
                destination_fence
                    .execute_next(&mut destination, &mut |_| Ok(()), &mut || Ok(()))
                    .unwrap();
                acknowledge_admins(&mut destination_fence);
            }
            let config =
                reverse::Config::new(&id, 160000, vec![("public".into(), table.clone())]).unwrap();
            config
                .setup(
                    &mut destination,
                    &mut source,
                    None,
                    Duration::from_secs(30),
                    || Ok(true),
                    |_, _| Ok(()),
                )
                .unwrap();
            destination_fence
                .unfence(&mut destination, &mut |_| Ok(()))
                .unwrap();
            // A fresh fence snapshot is required for the rollback transfer.
            let destination_fence = fence::Fence::new(destination_fence.config.clone()).unwrap();
            destination.sql(&format!("SET ROLE {destination_role}; INSERT INTO public.{table} VALUES(2,'destination-era'); RESET ROLE;"));
            Self {
                source,
                destination,
                source_fence,
                destination_fence,
                source_role,
                destination_role,
                table,
                config,
                id,
            }
        }
        fn installed(&mut self) -> Inventory {
            let mut objects = Inventory::new();
            for (side, db) in [
                (Side::Source, &mut self.source),
                (Side::Destination, &mut self.destination),
            ] {
                for (kind, catalog, column, scope) in [
                    (
                        ObjectKind::Slot,
                        "pg_replication_slots",
                        "slot_name",
                        " AND database=current_database()",
                    ),
                    (ObjectKind::Publication, "pg_publication", "pubname", ""),
                    (ObjectKind::EventTrigger, "pg_event_trigger", "evtname", ""),
                    (
                        ObjectKind::ReplicationOrigin,
                        "pg_replication_origin",
                        "roname",
                        "",
                    ),
                ] {
                    for row in db.sql(&format!(
                        "SELECT {column}::text FROM {catalog} WHERE strpos({column},{})>0{scope}",
                        lit(&self.id)
                    )) {
                        objects.insert(Object {
                            side,
                            kind,
                            name: row[0].clone(),
                        });
                    }
                }
                objects.insert(Object {
                    side,
                    kind: ObjectKind::Schema,
                    name: "_skedaddle".into(),
                });
            }
            objects
        }
        fn run(&mut self, crash: Option<cutover::Step>) {
            use skedaddle::coordinator::{Event, State, Transition};
            let migration = format!("{}_rollback", self.id);
            let journal = support::journal(
                support::fs_store(support::scratch("rollback-proof")),
                &migration,
            );
            let mut owner = support::coordinator(journal.clone(), "rollback-owner");
            let runtime = self.source.rt.clone();
            runtime.block_on(owner.acquire()).unwrap();
            let mut k = key(&migration);
            k.coordinator = owner.instance_id().into();
            let mut fencer = cutover::pg::Source {
                connection: self.destination.clone(),
                state: self.destination_fence.clone(),
                drained: cutover::DrainCounts::default(),
                pre_fence: || Ok(()),
            };
            let config = activate::Config {
                epoch: 1,
                applications: BTreeSet::from([self.source_role.clone()]),
                tables: self.source_fence.config.tables.clone(),
                timeout_ms: 30_000,
                post_activate: None,
            };
            let mut target = cutover::pg::Target {
                connection: self.source.clone(),
                state: activate::DestinationFence::with_activation_state(
                    config,
                    self.source_fence.saved.clone().unwrap(),
                )
                .unwrap(),
                acknowledged_triggers: BTreeSet::new(),
            };
            target.initialize().unwrap();
            while target.state.next != activate::Step::Fenced {
                target
                    .state
                    .execute_next(&mut target.connection, &mut |_| Ok(()))
                    .unwrap();
            }
            self.source.sql("CREATE TABLE _skedaddle.rollback_test_receipt(cutover text PRIMARY KEY,epoch bigint,lsn pg_lsn)");
            let mut progress = WireApply {
                source: self.source.clone(),
                destination: self.destination.clone(),
                slot: self.config.slot(),
                publication: self.config.publication(),
                origin: self.config.origin(),
                table: self.table.clone(),
                buffered: None,
            };
            let mut verifier = PgVerifier {
                source: self.source.clone(),
                destination: self.destination.clone(),
                table: self.table.clone(),
            };
            let mut sequences = cutover::pg::SequenceSync {
                source: self.destination.clone(),
                destination: self.source.clone(),
                scope: activate::SequenceScope {
                    tables: self.destination_fence.config.tables.clone(),
                    sequences: vec![],
                },
                timeout_ms: 30_000,
            };
            let mut status = PgStatus(self.source.clone());
            let mut state = rollback::State::new(k).unwrap();
            let initial = state.clone();
            let eligibility = reverse::Eligibility::new(|_| Ok(()));
            let mut records = Vec::new();
            let mut crashed = false;
            let mut proof_recorded = false;
            for _ in 0..30 {
                if state.transfer.next == cutover::Step::Complete {
                    break;
                }
                let at = state.transfer.next;
                if at == cutover::Step::Activate {
                    if !proof_recorded {
                        // Rollback activates the ORIGINAL source. The existing
                        // destination-era capture runs the other way. Set up
                        // this new stream after final-marker application while
                        // both application endpoints remain physically fenced.
                        assert_eq!(fencer.state.next, fence::Step::Complete);
                        assert_eq!(target.state.next, activate::Step::Fenced);
                        let fenced = target.state.clone();
                        let marker = state.transfer.marker.as_ref().unwrap();
                        let mut authorized = false;
                        assert_eq!(
                            target.activate(
                                &state.transfer.key,
                                marker,
                                &mut |_| Ok(()),
                                &mut || {
                                    authorized = true;
                                    Ok(())
                                },
                            ),
                            Err(cutover::Error::ReverseProof(
                                skedaddle::coordinator::reverse::ProofError::Missing
                            ))
                        );
                        assert!(!authorized);
                        assert_eq!(target.state.next, activate::Step::Restore);
                        assert_eq!(target.activated(&state.transfer.key).unwrap(), None);
                        target.state = fenced;
                        runtime.block_on(async {
                            for to in [State::Assessed, State::Copying, State::CatchingUp] {
                                owner
                                    .commit(Transition::Lifecycle {
                                        event: Event::Advance { to },
                                    })
                                    .await
                                    .unwrap();
                            }
                            owner
                                .commit(Transition::Lifecycle {
                                    event: Event::Readiness {
                                        facts: support::ready(),
                                    },
                                })
                                .await
                                .unwrap();
                            for to in [State::SourceFenced, State::FinalBoundaryApplied] {
                                owner
                                    .commit(Transition::Lifecycle {
                                        event: Event::Advance { to },
                                    })
                                    .await
                                    .unwrap();
                            }
                        });
                        let version = target.connection.sql("SHOW server_version_num")[0][0]
                            .parse()
                            .unwrap();
                        let config = reverse::Config::new(
                            &format!("{}_return", self.id),
                            version,
                            vec![("public".into(), self.table.clone())],
                        )
                        .unwrap();
                        let proof = owner
                            .setup_reverse(
                                &runtime,
                                &config,
                                &mut target.connection,
                                &mut fencer.connection,
                                &mut target.state,
                            )
                            .unwrap();
                        target.state.attach_reverse_proof(proof);
                        proof_recorded = true;
                    }
                    // Serialization drops the capability; replay the journal
                    // again after recovery instead of retaining local authority.
                    target.state.attach_reverse_proof(
                        runtime.block_on(journal.authenticated_reverse()).unwrap(),
                    );
                }
                let result = state.execute_next(
                    &mut Guard,
                    &eligibility,
                    &mut cutover::Components {
                        source: &mut fencer,
                        destination: &mut target,
                        progress: &mut progress,
                        verifier: &mut verifier,
                        sequences: &mut sequences,
                        reverse: &mut ReverseReady,
                        status: &mut status,
                    },
                    &mut |r| {
                        if !crashed
                            && crash == Some(at)
                            && matches!(
                                r,
                                rollback::Record::Transfer(cutover::Record::Checkpoint(_))
                            )
                        {
                            crashed = true;
                            return Err("lost process after committed effect".into());
                        }
                        records.push(r.clone());
                        Ok(())
                    },
                );
                if result.is_err() {
                    assert!(crashed, "unexpected real-PG rollback error: {result:?}");
                    state = recover(&initial, &records);
                    // Replay native journal records, discarding volatile adapters.
                    fencer.state = self.destination_fence.clone();
                    for record in &records {
                        match record {
                            rollback::Record::Transfer(cutover::Record::Source(
                                fence::Intent::Step(s)
                                | fence::Intent::Save(s)
                                | fence::Intent::Unfence(s),
                            )) => fencer.state = *s.clone(),
                            rollback::Record::Transfer(cutover::Record::Destination(i)) => {
                                target.state = i.state.clone()
                            }
                            rollback::Record::Transfer(cutover::Record::Drain(d)) => {
                                fencer.drained = d.clone()
                            }
                            _ => (),
                        }
                    }
                    fencer.connection = self.destination.clone();
                    target.connection = self.source.clone();
                }
            }
            assert!(
                proof_recorded,
                "normal Restore must succeed after P11a setup"
            );
            assert_eq!(state.transfer.next, cutover::Step::Complete);
            assert_eq!(crashed, crash.is_some());
            assert_eq!(
                self.source.sql(&format!(
                    "SELECT id::text,value FROM public.{} ORDER BY id",
                    self.table
                )),
                self.destination.sql(&format!(
                    "SELECT id::text,value FROM public.{} ORDER BY id",
                    self.table
                ))
            );
            assert_eq!(
                self.source.sql(&format!(
                    "SELECT rolcanlogin::text FROM pg_roles WHERE rolname={}",
                    lit(&self.source_role)
                ))[0][0],
                "true"
            );
            assert_eq!(
                self.destination.sql(&format!(
                    "SELECT rolcanlogin::text FROM pg_roles WHERE rolname={}",
                    lit(&self.destination_role)
                ))[0][0],
                "false"
            );
            // Native Target activation verifies the complete saved ACL inventory.
            assert_eq!(target.state.activation_state, self.source_fence.saved);
            assert!(state.transfer.audit.is_some());
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            // Remove only this fixture's manifest; an active capture prevents slot
            // deletion, which is intentionally a hard error in finalize.
            let manifest = self.installed();
            let mut f =
                Finalize::new(key(&format!("{}_cleanup", self.id)), true, manifest).unwrap();
            while f.next != rollback::FinalizeStep::Complete {
                f.execute_next(
                    &mut Guard,
                    &mut self.source,
                    &mut self.destination,
                    &mut |_| Ok(()),
                    &mut |_| Ok(()),
                )
                .unwrap();
            }
            for (db, role) in [
                (&mut self.source, &self.source_role),
                (&mut self.destination, &self.destination_role),
            ] {
                db.sql(&format!(
                    "DROP TABLE public.{}; DROP ROLE {role}",
                    self.table
                ));
            }
        }
    }

    struct WireApply {
        source: Pg,
        destination: Pg,
        slot: String,
        publication: String,
        origin: String,
        table: String,
        buffered: Option<cutover::Marker>,
    }
    impl cutover::Progress for WireApply {
        fn buffered(
            &mut self,
            marker: &cutover::Marker,
        ) -> cutover::Result<Option<cutover::Marker>> {
            let sql = format!(
                "SELECT data FROM pg_logical_slot_peek_binary_changes({},NULL,NULL,'proto_version','1','publication_names',{},'messages','true','origin','none')",
                lit(&self.slot),
                lit(&self.publication)
            );
            let rows = self
                .destination
                .rt
                .block_on(self.destination.client.query(&sql, &[]))
                .map_err(|e| cutover::Error::Component(e.to_string()))?;
            let mut inserts = Vec::new();
            let mut found = false;
            for row in rows {
                let data: Vec<u8> = row.get(0);
                match data.first() {
                    Some(b'I') => {
                        assert_eq!(data[5], b'N');
                        let columns = u16::from_be_bytes(data[6..8].try_into().unwrap());
                        assert_eq!(columns, 2);
                        let mut pos = 8;
                        let mut values = Vec::new();
                        for _ in 0..columns {
                            assert_eq!(data[pos], b't');
                            pos += 1;
                            let len =
                                u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                            pos += 4;
                            values.push(String::from_utf8(data[pos..pos + len].to_vec()).unwrap());
                            pos += len;
                        }
                        inserts.push(values);
                    }
                    Some(b'M') => {
                        let lsn = u64::from_be_bytes(data[2..10].try_into().unwrap());
                        let end = 10 + data[10..].iter().position(|b| *b == 0).unwrap();
                        let len =
                            u32::from_be_bytes(data[end + 1..end + 5].try_into().unwrap()) as usize;
                        let body = &data[end + 5..end + 5 + len];
                        if &data[10..end] == b"skedaddle"
                            && body == marker.payload().as_bytes()
                            && lsn == marker.lsn
                        {
                            found = true;
                            break;
                        }
                    }
                    _ => (),
                }
            }
            if found {
                let mut sql = format!(
                    "SELECT pg_replication_origin_session_setup({}); BEGIN;",
                    lit(&self.origin)
                );
                for values in inserts {
                    sql.push_str(&format!(
                        "INSERT INTO public.{} VALUES({},{}) ON CONFLICT(id) DO NOTHING;",
                        self.table,
                        lit(&values[0]),
                        lit(&values[1])
                    ));
                }
                sql.push_str(&format!("INSERT INTO _skedaddle.rollback_test_receipt VALUES({},{},'{}'::pg_lsn) ON CONFLICT(cutover) DO NOTHING; COMMIT; SELECT pg_replication_origin_session_reset();", lit(&marker.cutover), marker.epoch, lsn_text(marker.lsn)));
                self.source
                    .query(&sql, Duration::from_secs(30))
                    .map_err(cutover::Error::Component)?;
                self.buffered = Some(marker.clone());
            }
            Ok(self.buffered.clone())
        }
        fn applied(
            &mut self,
            marker: &cutover::Marker,
        ) -> cutover::Result<Option<cutover::Marker>> {
            let rows = self.source.sql(&format!("SELECT cutover,epoch::text,lsn::text FROM _skedaddle.rollback_test_receipt WHERE cutover={}", lit(&marker.cutover)));
            Ok(rows
                .first()
                .filter(|r| {
                    r[0] == marker.cutover
                        && r[1] == marker.epoch.to_string()
                        && fence::parse_lsn(&r[2]).unwrap() == marker.lsn
                })
                .map(|_| marker.clone()))
        }
    }
    fn lsn_text(lsn: u64) -> String {
        format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
    }
    struct PgVerifier {
        source: Pg,
        destination: Pg,
        table: String,
    }
    impl cutover::Verifier for PgVerifier {
        fn boundary(
            &mut self,
            marker: &cutover::Marker,
        ) -> cutover::Result<cutover::VerifiedBoundary> {
            let sql = format!(
                "SELECT id::text,value FROM public.{} ORDER BY id",
                self.table
            );
            let a = self.source.sql(&sql);
            let b = self.destination.sql(&sql);
            let mut c = coverage();
            if a != b {
                c.mismatched = 1;
            }
            Ok(cutover::VerifiedBoundary {
                marker: marker.clone(),
                coverage: c,
            })
        }
    }
    struct PgStatus(Pg);
    impl cutover::Status for PgStatus {
        fn readiness(&mut self, _: &cutover::Key) -> cutover::Result<()> {
            Ok(())
        }
        fn breached(&mut self, _: &cutover::Key) -> cutover::Result<bool> {
            Ok(false)
        }
        fn now_ms(&mut self) -> cutover::Result<u64> {
            Ok(self
                .0
                .sql("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint::text")[0]
                [0]
            .parse()
            .unwrap())
        }
        fn audit(&mut self, _: &cutover::Key, _: &cutover::Audit) -> cutover::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn rollback_preserves_destination_writes() {
        let Some((s, d)) = urls("rollback_preserves_destination_writes") else {
            return;
        };
        let _guard = SERIAL.lock().unwrap();
        Fixture::new(&s, &d).run(None);
    }
    #[test]
    fn rollback_restores_source_privileges_exactly() {
        let Some((s, d)) = urls("rollback_restores_source_privileges_exactly") else {
            return;
        };
        let _guard = SERIAL.lock().unwrap();
        Fixture::new(&s, &d).run(None);
    }
    #[test]
    fn crash_during_rollback_recovers() {
        let Some((s, d)) = urls("crash_during_rollback_recovers") else {
            return;
        };
        let _guard = SERIAL.lock().unwrap();
        for step in [
            cutover::Step::Fence,
            cutover::Step::FinalMarker,
            cutover::Step::Activate,
        ] {
            Fixture::new(&s, &d).run(Some(step));
        }
    }
    #[test]
    fn finalize_drops_all_installed_objects() {
        let Some((s, d)) = urls("finalize_drops_all_installed_objects") else {
            return;
        };
        let _guard = SERIAL.lock().unwrap();
        let mut fixture = Fixture::new(&s, &d);
        // Install capture objects on the other endpoint as well.
        let forward = reverse::Config::new(
            &format!("{}_f", fixture.id),
            160000,
            vec![("public".into(), fixture.table.clone())],
        )
        .unwrap();
        forward
            .setup(
                &mut fixture.source,
                &mut fixture.destination,
                None,
                Duration::from_secs(30),
                || Ok(true),
                |_, _| Ok(()),
            )
            .unwrap();
        let manifest = fixture.installed();
        let mut stopped = false;
        let mut retained = Finalize::new(key("retained"), false, manifest.clone()).unwrap();
        while retained.next != rollback::FinalizeStep::Complete {
            retained
                .execute_next(
                    &mut Guard,
                    &mut fixture.source,
                    &mut fixture.destination,
                    &mut |_| {
                        stopped = true;
                        Ok(())
                    },
                    &mut |_| Ok(()),
                )
                .unwrap();
        }
        assert!(stopped);
        assert_eq!(retained.before, retained.after);
        let mut removed = Finalize::new(key("removed"), true, manifest).unwrap();
        let mut records = Vec::new();
        let mut fault = false;
        while removed.next != rollback::FinalizeStep::Complete {
            let result = removed.execute_next(
                &mut Guard,
                &mut fixture.source,
                &mut fixture.destination,
                &mut |_| Ok(()),
                &mut |r| {
                    if matches!(r, rollback::FinalizeRecord::Drop(_)) && !fault {
                        fault = true;
                        return Err("crash before drop".into());
                    }
                    records.push(r.clone());
                    Ok(())
                },
            );
            if result.is_err() {
                assert!(fault);
            }
        }
        assert!(fault && !records.is_empty());
        assert!(!removed.before.as_ref().unwrap().is_empty());
        assert!(removed.leftovers().unwrap().is_empty());
        for db in [&mut fixture.source, &mut fixture.destination] {
            assert_eq!(
                db.sql("SELECT (to_regnamespace('_skedaddle') IS NULL)::text")[0][0],
                "true"
            );
        }
    }
}

// The harness CA is explicit; missing trust is a failure once a DB gate is set.
fn tls_config(postgres: tokio_postgres::Config) -> skedaddle::pg::tls::Config {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("TLS harness config required");
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read TLS harness config"))
            .expect("TLS harness JSON");
    let ca_pem = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .expect("harness ca_pem or replication.ca_pem required");
    skedaddle::pg::tls::Config::new(postgres, ca_pem)
}

async fn tls_connect(
    dsn: &str,
) -> std::result::Result<
    (tokio_postgres::Client, skedaddle::pg::tls::Driver),
    skedaddle::pg::tls::Error,
> {
    let config = tls_config(dsn.parse().expect("harness PostgreSQL configuration"));
    let (client, driver, transport) =
        skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required).await?;
    assert!(transport.encrypted());
    assert!(transport.warnings.is_empty());
    Ok((client, driver))
}
