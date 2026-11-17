use super::*;
use skedaddle::{
    coordinator::{self, Materialized, Transition, journal::Entry, rollback as rb},
    cutover::{self as c, SourceFence},
    rollback::destination as rd,
};
use std::{cell::Cell, rc::Rc};

// Only non-GigglesDB components are doubles; every fence query hits the server.
struct Other;
impl c::Authority for Other {
    fn run<T>(&mut self, _: &c::Key, run: impl FnOnce() -> c::Result<T>) -> c::Result<T> {
        run()
    }
}
impl c::Destination for Other {
    fn activated(&mut self, _: &c::Key) -> c::Result<Option<u64>> {
        Ok(None)
    }
    fn prove_fenced(&mut self, _: &c::Key, _: &mut c::Journal<'_>) -> c::Result<()> {
        Ok(())
    }
    fn readiness(&mut self, _: &c::Key, _: &mut c::Journal<'_>) -> c::Result<()> {
        panic!("past marker refusal")
    }
    fn activate(
        &mut self,
        _: &c::Key,
        _: &c::Marker,
        _: &mut c::Journal<'_>,
        _: &mut dyn FnMut() -> c::Result<()>,
    ) -> c::Result<u64> {
        panic!("past marker refusal")
    }
    fn post_activate(&mut self, _: &c::Key, _: &mut c::Journal<'_>) -> c::Result<()> {
        panic!("past marker refusal")
    }
}
impl c::Status for Other {
    fn readiness(&mut self, _: &c::Key) -> c::Result<()> {
        Ok(())
    }
    fn breached(&mut self, _: &c::Key) -> c::Result<bool> {
        Ok(false)
    }
    fn now_ms(&mut self) -> c::Result<u64> {
        Ok(1)
    }
    fn audit(&mut self, _: &c::Key, _: &c::Audit) -> c::Result<()> {
        panic!("past marker refusal")
    }
}
impl c::Progress for Other {
    fn buffered(&mut self, _: &c::Marker) -> c::Result<Option<c::Marker>> {
        panic!("past marker refusal")
    }
    fn applied(&mut self, _: &c::Marker) -> c::Result<Option<c::Marker>> {
        panic!("past marker refusal")
    }
}
impl c::Verifier for Other {
    fn boundary(&mut self, _: &c::Marker) -> c::Result<c::VerifiedBoundary> {
        panic!("past marker refusal")
    }
}
impl c::Sequences for Other {
    fn sync(&mut self, _: &c::Marker) -> c::Result<()> {
        panic!("past marker refusal")
    }
}
impl c::Reverse for Other {
    fn confirm(&mut self, _: &c::Key, _: &c::Marker) -> c::Result<()> {
        panic!("past marker refusal")
    }
}
#[derive(Default)]
struct Capture {
    restarts: usize,
}
impl rd::Capture for Capture {
    fn captured(&mut self, _: &c::Marker) -> c::Result<Option<c::Marker>> {
        panic!("past marker refusal")
    }
    fn stop_at(&mut self, _: &c::Marker) -> c::Result<()> {
        panic!("past marker refusal")
    }
    fn restart(&mut self, _: &c::Key) -> c::Result<()> {
        self.restarts += 1;
        Ok(())
    }
}
impl rd::PreHook for Other {
    fn run(&mut self, _: &c::Key) -> c::Result<()> {
        Ok(())
    }
}
struct Log {
    state: Materialized,
    native: Vec<c::Record>,
}
impl rd::Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, record: rb::Record) -> c::Result<()> {
        let entry = Entry::new(
            &self.state,
            Transition::Rollback {
                record: Box::new(record),
            },
            "owner",
        )
        .map_err(|e| c::Error::Journal(e.to_string()))?;
        self.state
            .apply(&entry)
            .map_err(|e| c::Error::Journal(e.to_string()))
    }
    fn native(&mut self, record: &c::Record) -> Result<(), String> {
        self.native.push(record.clone());
        Ok(())
    }
}
fn key() -> c::Key {
    c::Key {
        migration: "n7-live".into(),
        cutover: "forward".into(),
        epoch: 1,
        coordinator: "owner".into(),
    }
}
fn source(
    db: Db,
    state: Fence,
    calls: Rc<Cell<usize>>,
) -> c::gigglesdb::Source<Db, impl FnMut() -> Result<(), String>> {
    c::gigglesdb::Source {
        connection: db,
        state,
        drained: c::DrainCounts::default(),
        pre_fence: move || {
            calls.set(calls.get() + 1);
            Ok(())
        },
    }
}
fn logins(db: &mut Db) -> Rows {
    db.sql("SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname IN ('app', 'asleep') ORDER BY rolname")
}
fn assert_fenced(db: &mut Db) {
    assert_eq!(logins(db), vec![vec!["app", "f"], vec!["asleep", "f"]]);
    for role in ["app", "asleep"] {
        for privilege in ["INSERT", "UPDATE", "DELETE", "TRUNCATE"] {
            assert_eq!(
                db.sql(&format!(
                    "SELECT has_table_privilege('{role}', 'public.n7', '{privilege}')"
                ))[0][0],
                "f"
            );
        }
    }
    assert!(Db::connect(db.port, "app").is_err());
}
fn marker_error() -> c::Error {
    c::Error::Invalid("GigglesDB final marker needs GD-0077 capture")
}

#[test]
fn cutover_driver_refuses_marker_then_aborts_with_exact_restore() {
    let Some((_server, db, fence)) = setup() else {
        return;
    };
    let calls = Rc::new(Cell::new(0));
    let mut source = source(db, fence, calls.clone());
    let before = (acl(&mut source.connection), logins(&mut source.connection));
    let mut state = c::State::new(key()).unwrap();
    let mut records = vec![];
    {
        let mut components = c::Components {
            source: &mut source,
            destination: &mut Other,
            progress: &mut Other,
            verifier: &mut Other,
            sequences: &mut Other,
            reverse: &mut Other,
            status: &mut Other,
        };
        for _ in 0..3 {
            state
                .execute_next(&mut Other, &mut components, &mut |r| {
                    records.push(r.clone());
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(state.next, c::Step::FinalMarker);
        assert_fenced(&mut components.source.connection);
        assert_eq!(
            state.execute_next(&mut Other, &mut components, &mut |r| {
                records.push(r.clone());
                Ok(())
            }),
            Err(marker_error())
        );
        assert_eq!(state.next, c::Step::FinalMarker);
    }
    state
        .abort(&mut Other, &mut source, &mut Other, &mut |r| {
            records.push(r.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(state.next, c::Step::Aborted);
    assert_eq!(
        (acl(&mut source.connection), logins(&mut source.connection)),
        before
    );
    let _app = Db::connect(source.connection.port, "app").unwrap();
    assert_eq!(calls.get(), 1);
    assert!(
        records
            .iter()
            .any(|r| matches!(r, c::Record::GigglesdbSource(Intent::Unfence(_))))
    );
}

#[test]
fn rollback_driver_restart_replays_then_restores_on_deadline() {
    use rd::Journal;
    let Some((server, db, fence)) = setup() else {
        return;
    };
    // setup() binds the forward identity; rollback must bind its own namespace.
    let fence = Fence::new(fence.config).unwrap();
    let fresh = fence.clone();
    let calls = Rc::new(Cell::new(0));
    let mut original = source(db, fence, calls.clone());
    let before = (
        acl(&mut original.connection),
        logins(&mut original.connection),
    );
    let mut state = Materialized {
        epoch: 1,
        owner: "owner".into(),
        ..Default::default()
    };
    state.lifecycle.state = coordinator::State::RollbackWindow;
    let mut log = Log {
        state,
        native: vec![],
    };
    log.publish(rb::Record::Admit {
        request: "rollback-live".into(),
        deadline_at_ms: u64::MAX,
    })
    .unwrap();
    let mut capture = Capture::default();
    for _ in 0..2 {
        assert_eq!(
            rd::execute_next(
                &key(),
                0,
                &mut Other,
                &mut original,
                &mut capture,
                &mut Other,
                &mut log
            )
            .unwrap(),
            rd::Outcome::Progress
        );
    }
    assert_eq!(log.state.rollback.as_ref().unwrap().next, rb::Step::Drain);
    assert_fenced(&mut original.connection);
    assert_eq!(calls.get(), 1);
    // Crash boundary: discard the adapter and rebuild solely from durable records.
    drop(original);
    let mut recovered = source(
        Db::connect(server.port, "postgres").unwrap(),
        fresh,
        calls.clone(),
    );
    for record in &log.native {
        let durable: c::Record =
            serde_json::from_str(&serde_json::to_string(record).unwrap()).unwrap();
        recovered.replay(&durable);
    }
    assert_eq!(recovered.state.next, Step::Complete);
    assert!(recovered.state.saved.is_some());
    // Retrying the fence after replay must not run pre_fence again.
    let native_key = rd::destination_key(&key(), "rollback-live");
    recovered
        .fence(&native_key, &mut |r| log.native(r))
        .unwrap();
    assert_eq!(calls.get(), 1);
    rd::execute_next(
        &key(),
        0,
        &mut Other,
        &mut recovered,
        &mut capture,
        &mut Other,
        &mut log,
    )
    .unwrap();
    assert_eq!(
        rd::execute_next(
            &key(),
            0,
            &mut Other,
            &mut recovered,
            &mut capture,
            &mut Other,
            &mut log
        ),
        Err(marker_error())
    );
    assert_eq!(
        log.state.rollback.as_ref().unwrap().next,
        rb::Step::FinalMarker
    );
    assert_eq!(capture.restarts, 0);
    assert_fenced(&mut recovered.connection);
    // Expire the test coordinator clock, as in the driver's destination tests.
    log.state.rollback.as_mut().unwrap().deadline_at_ms = 1;
    assert_eq!(
        rd::execute_next(
            &key(),
            u64::MAX,
            &mut Other,
            &mut recovered,
            &mut capture,
            &mut Other,
            &mut log
        )
        .unwrap(),
        rd::Outcome::Restored
    );
    assert_eq!(log.state.rollback.as_ref().unwrap().next, rb::Step::Aborted);
    assert_eq!(
        (
            acl(&mut recovered.connection),
            logins(&mut recovered.connection)
        ),
        before
    );
    let _app = Db::connect(server.port, "app").unwrap();
    assert_eq!(capture.restarts, 1);
    assert_eq!(calls.get(), 1);
    assert!(
        log.native
            .iter()
            .any(|r| matches!(r, c::Record::GigglesdbSource(Intent::Unfence(_))))
    );
}
