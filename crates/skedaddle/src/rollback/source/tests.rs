use super::*;
use crate::coordinator::{Transition, journal::Entry};
use std::{cell::RefCell, rc::Rc};
#[derive(Default)]
struct Shared {
    intent: Option<Step>,
    calls: usize,
    crash: Option<usize>,
    events: Vec<&'static str>,
    active: bool,
}
type Memory = Rc<RefCell<Shared>>;
fn boundary(m: &Memory) -> Result<()> {
    let mut s = m.borrow_mut();
    let n = s.calls;
    s.calls += 1;
    if s.crash == Some(n) {
        s.crash = None;
        return Err(Error::Retry);
    }
    Ok(())
}
struct Log {
    state: Materialized,
    memory: Memory,
}
fn publish(state: &mut Materialized, record: Record) {
    let e = Entry::new(
        state,
        Transition::Rollback {
            record: Box::new(record),
        },
        "owner",
    )
    .unwrap();
    state.apply(&e).unwrap();
}
impl Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, record: Record) -> Result<()> {
        boundary(&self.memory)?;
        if let Record::Intent { step } = record {
            self.memory.borrow_mut().intent = Some(step);
        }
        publish(&mut self.state, record);
        boundary(&self.memory)
    }
    fn native(&mut self, _: &fence::Intent) -> std::result::Result<(), String> {
        Ok(())
    }
}
struct Fake(Memory);
impl Effects for Fake {
    fn validate(&self, _: &Key) -> Result<()> {
        Ok(())
    }
    fn sequences(&mut self, _: &str) -> Result<()> {
        assert_eq!(self.0.borrow().intent, Some(Step::Sequences));
        boundary(&self.0)?;
        self.0.borrow_mut().events.push("sequences");
        boundary(&self.0)
    }
    fn restore(&mut self, _: &mut impl Journal) -> Result<()> {
        assert!(matches!(
            self.0.borrow().intent,
            Some(Step::RestoreSource | Step::PostHook)
        ));
        boundary(&self.0)?;
        self.0.borrow_mut().active = true;
        self.0.borrow_mut().events.push("restore");
        boundary(&self.0)
    }
    fn refence(&mut self, _: &mut impl Journal) -> Result<()> {
        self.0.borrow_mut().active = false;
        Ok(())
    }
}
impl PostHook for Fake {
    fn run(&mut self, key: &Key) -> Result<()> {
        assert_eq!(key.cutover, "request");
        assert_eq!(self.0.borrow().intent, Some(Step::PostHook));
        assert!(self.0.borrow().active);
        boundary(&self.0)?;
        self.0.borrow_mut().events.push("hook");
        boundary(&self.0)
    }
}
fn setup(crash: Option<usize>) -> (Key, Memory, Log) {
    let mut state = Materialized {
        epoch: 1,
        owner: "owner".into(),
        ..Default::default()
    };
    state.lifecycle.state = State::RollbackWindow;
    publish(
        &mut state,
        Record::Admit {
            request: "request".into(),
            deadline_at_ms: u64::MAX,
        },
    );
    for step in [
        Step::PreHook,
        Step::FenceDestination,
        Step::Drain,
        Step::FinalMarker,
        Step::ReverseApply,
        Step::Verify,
    ] {
        publish(&mut state, Record::Intent { step });
        publish(
            &mut state,
            Record::Checkpoint {
                step,
                final_marker: (step == Step::FinalMarker).then_some(42),
            },
        );
    }
    let memory = Rc::new(RefCell::new(Shared {
        crash,
        ..Default::default()
    }));
    (
        Key {
            migration: "m".into(),
            cutover: "forward".into(),
            epoch: 1,
            coordinator: "owner".into(),
        },
        memory.clone(),
        Log { state, memory },
    )
}
#[test]
fn order_and_intents() {
    let (key, m, mut log) = setup(None);
    for _ in 0..3 {
        execute_next(&key, &mut Fake(m.clone()), &mut Fake(m.clone()), &mut log).unwrap();
    }
    assert_eq!(
        m.borrow().events,
        ["sequences", "restore", "restore", "hook"]
    );
    assert_eq!(log.state.lifecycle.state, State::SourceActiveAfterRollback);
    assert_eq!(
        execute_next(&key, &mut Fake(m.clone()), &mut Fake(m.clone()), &mut log).unwrap(),
        Outcome::Complete
    );
}
#[test]
fn crash_at_every_boundary_resumes() {
    // 20 boundaries: journal intent/effect/checkpoint and post-hook restore.
    for crash in 0..22 {
        let (key, m, mut log) = setup(Some(crash));
        for _ in 0..10 {
            if log.state.lifecycle.state == State::SourceActiveAfterRollback {
                break;
            }
            let result = execute_next(&key, &mut Fake(m.clone()), &mut Fake(m.clone()), &mut log);
            if result.is_err() && log.state.rollback.as_ref().unwrap().next != Step::Sequences {
                assert!(!m.borrow().active);
            }
            log.state = serde_json::from_slice(&serde_json::to_vec(&log.state).unwrap()).unwrap();
        }
        assert_eq!(log.state.lifecycle.state, State::SourceActiveAfterRollback);
    }
}
#[test]
fn missing_verification_or_authority_has_no_effect() {
    for bad in 0..4 {
        let (mut key, m, mut log) = setup(None);
        match bad {
            0 => key.epoch = 2,
            1 => log.state.rollback.as_mut().unwrap().point_of_no_return = false,
            2 => log.state.rollback.as_mut().unwrap().final_marker = None,
            _ => log.state.rollback.as_mut().unwrap().next = Step::Verify,
        }
        assert!(matches!(
            execute_next(&key, &mut Fake(m.clone()), &mut Fake(m.clone()), &mut log),
            Err(Error::OperatorAction(_))
        ));
        assert!(m.borrow().events.is_empty());
    }
}

struct Connection {
    rows: fence::Rows,
    restored: fence::Rows,
    expected_sql: String,
    writes: usize,
}
impl fence::Connection for Connection {
    fn query(&mut self, sql: &str, _: Duration) -> std::result::Result<fence::Rows, String> {
        if sql.starts_with("SELECT 'role'") {
            return Ok(self.rows.clone());
        }
        if sql.starts_with("BEGIN;") {
            assert!(sql.contains(&self.expected_sql));
            self.rows = self.restored.clone();
            self.writes += 1;
        }
        Ok(vec![])
    }
}
fn pg() -> Pg<Connection, Connection> {
    let rows: fence::Rows = [
        ["role", "app", "true", "false", "false", "", "", "", ""],
        ["role", "offline", "false", "false", "false", "", "", "", ""],
        ["member", "app", "writer", "", "", "", "", "", ""],
        ["table", "public", "data", "owner", "", "", "", "", ""],
        [
            "grant",
            "public",
            "data",
            "owner",
            "value",
            "role",
            "writer",
            "owner",
            "UPDATE:true",
        ],
        [
            "grant",
            "public",
            "data",
            "owner",
            "",
            "public",
            "",
            "owner",
            "INSERT:false",
        ],
    ]
    .into_iter()
    .map(|r| r.into_iter().map(String::from).collect())
    .collect();
    let mut conn = Connection {
        rows: rows.clone(),
        restored: rows,
        expected_sql: String::new(),
        writes: 0,
    };
    let tables = vec![fence::Table {
        schema: "public".into(),
        name: "data".into(),
        owner: "owner".into(),
    }];
    let saved = fence::SavedState {
        catalog: fence::read_catalog(&mut conn, &tables, Duration::from_secs(1)).unwrap(),
        authority: None,
    };
    let applications = ["app".into(), "offline".into()].into();
    conn.expected_sql = fence::restoration_sql(&saved, &applications).unwrap();
    conn.rows.retain(|r| r[0] != "grant");
    conn.rows[0][2] = "false".into();
    let mut original = fence::Fence::new(fence::FenceConfig {
        epoch: 1,
        applications,
        tables: tables.clone(),
        acknowledged: Default::default(),
        deadline_ms: 1000,
    })
    .unwrap();
    original.bind_migration("m").unwrap();
    original.next = fence::Step::Complete;
    original.saved = Some(saved);
    Pg {
        source: conn,
        destination: Connection {
            rows: vec![],
            restored: vec![],
            expected_sql: String::new(),
            writes: 0,
        },
        original,
        scope: activate::SequenceScope {
            tables,
            sequences: vec![],
        },
        timeout: Duration::from_secs(1),
    }
}
#[test]
fn exact_saved_roles_memberships_owners_and_grants_restored() {
    let (_, _, mut log) = setup(None);
    let mut pg = pg();
    pg.restore(&mut log).unwrap();
    let actual =
        fence::read_catalog(&mut pg.source, &pg.original.config.tables, pg.timeout).unwrap();
    let expected = &pg.original.saved.as_ref().unwrap().catalog;
    assert_eq!(actual.roles, expected.roles);
    assert_eq!(actual.memberships, expected.memberships);
    assert_eq!(actual.tables, expected.tables);
    for (a, e) in actual.grants.iter().zip(&expected.grants) {
        assert_eq!(a.table, e.table);
        assert_eq!(a.column, e.column);
        assert_eq!(a.grantee, e.grantee);
        assert_eq!(a.grantor, e.grantor);
        assert_eq!(a.privilege, e.privilege);
        assert_eq!(a.grant_option, e.grant_option);
    }
    assert_eq!(actual.grants.len(), expected.grants.len());
    assert_eq!(pg.destination.writes, 0);
    assert!(!actual.roles[1].login);
}
#[test]
fn altered_role_membership_or_owner_refuses_before_unfence() {
    for (row, column) in [(0, 3), (0, 4), (2, 2), (3, 3)] {
        let (_, _, mut log) = setup(None);
        let mut pg = pg();
        pg.source.rows[row][column] = if column == 3 && row == 0 || column == 4 {
            "true"
        } else {
            "other"
        }
        .into();
        assert!(matches!(
            pg.restore(&mut log),
            Err(Error::OperatorAction(_))
        ));
        assert_eq!(pg.source.writes, 0);
    }
}
#[test]
fn absent_or_foreign_original_snapshot_is_rejected() {
    let (key, _, _) = setup(None);
    let mut pg = pg();
    pg.original.saved = None;
    assert!(pg.validate(&key).is_err());
    pg.original = super::tests::pg().original;
    pg.original.migration = "foreign".into();
    assert!(pg.validate(&key).is_err());
}

struct SequenceConnection {
    value: i64,
    called: bool,
    writes: usize,
}
impl fence::Connection for SequenceConnection {
    fn query(&mut self, sql: &str, _: Duration) -> std::result::Result<fence::Rows, String> {
        if sql.contains("FROM pg_class") {
            return Ok(vec![
                ["public", "seq", "-1", "1", "1000", "true", "", "", ""]
                    .into_iter()
                    .map(String::from)
                    .collect(),
            ]);
        }
        if sql.starts_with("SELECT last_value") {
            return Ok(vec![vec![self.value.to_string(), self.called.to_string()]]);
        }
        if sql.starts_with("SELECT setval") {
            assert!(sql.ends_with(",700,false);"));
            self.value = 700;
            self.called = false;
            self.writes += 1;
            return Ok(vec![]);
        }
        panic!("unexpected SQL {sql}");
    }
}
#[test]
fn sequence_roles_swapped_exact_setval_without_advancing_destination() {
    let original = pg().original;
    let mut pg = Pg {
        source: SequenceConnection {
            value: 10,
            called: true,
            writes: 0,
        },
        destination: SequenceConnection {
            value: 700,
            called: false,
            writes: 0,
        },
        original,
        scope: activate::SequenceScope {
            tables: vec![],
            sequences: vec![activate::SequenceName {
                schema: "public".into(),
                name: "seq".into(),
            }],
        },
        timeout: Duration::from_secs(1),
    };
    for _ in 0..2 {
        pg.sequences("request:42").unwrap();
    }
    assert_eq!(
        (pg.source.value, pg.source.called, pg.source.writes),
        (700, false, 2)
    );
    assert_eq!(
        (
            pg.destination.value,
            pg.destination.called,
            pg.destination.writes
        ),
        (700, false, 0)
    );
}

struct Uncertain;
impl Effects for Uncertain {
    fn validate(&self, _: &Key) -> Result<()> {
        Ok(())
    }
    fn sequences(&mut self, _: &str) -> Result<()> {
        Ok(())
    }
    fn restore(&mut self, _: &mut impl Journal) -> Result<()> {
        Err(Error::Retry)
    }
    fn refence(&mut self, _: &mut impl Journal) -> Result<()> {
        Err(Error::Retry)
    }
}
#[test]
fn uncertain_refence_requires_operator_and_does_not_advance() {
    let (key, m, mut log) = setup(None);
    execute_next(&key, &mut Uncertain, &mut Fake(m.clone()), &mut log).unwrap();
    let error = execute_next(&key, &mut Uncertain, &mut Fake(m), &mut log).unwrap_err();
    assert!(matches!(error, Error::OperatorAction(_)));
    assert_eq!(error.exit_code(), 5);
    assert_eq!(log.state.lifecycle.state, State::RollingBack);
    assert_eq!(
        log.state.rollback.as_ref().unwrap().next,
        Step::RestoreSource
    );
}

#[test]
fn native_unfence_intent_failure_prevents_privilege_effect() {
    struct Reject;
    impl Journal for Reject {
        fn state(&self) -> Materialized {
            Default::default()
        }
        fn publish(&mut self, _: Record) -> Result<()> {
            unreachable!()
        }
        fn native(&mut self, _: &fence::Intent) -> std::result::Result<(), String> {
            Err("durability unavailable".into())
        }
    }
    let mut pg = pg();
    assert!(matches!(pg.restore(&mut Reject), Err(Error::Retry)));
    assert_eq!(pg.source.writes, 0);
    assert_eq!(pg.destination.writes, 0);
    assert_eq!(pg.source.rows[0][2], "false");
}
