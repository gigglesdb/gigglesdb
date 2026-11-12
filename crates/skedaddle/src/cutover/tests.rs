use super::*;
use std::{cell::RefCell, rc::Rc};

#[derive(Default)]
struct World {
    source_fenced: bool,
    destination_fenced: bool,
    activated: Option<u64>,
    activation_count: u64,
    marker: Option<Marker>,
    buffered: bool,
    applied: bool,
    breach: bool,
    complete_coverage: bool,
    sequence_breach: bool,
    late_breach: bool,
    verifier_wrong_marker: bool,
    applied_identity: Option<Marker>,
    proof_count: u64,
    clock: u64,
    audit: Option<Audit>,
    hook_count: u64,
}
#[derive(Clone)]
struct Fake(Rc<RefCell<World>>);
impl SourceFence for Fake {
    fn fence(&mut self, _: &Key, _: &mut Journal<'_>) -> Result<DrainCounts> {
        self.0.borrow_mut().source_fenced = true;
        Ok(DrainCounts {
            terminated: 3,
            termination_outcome_unknown: false,
            remaining_sessions: 0,
            prepared_transactions: 0,
        })
    }
    fn final_marker(&mut self, key: &Key) -> Result<Marker> {
        Ok(self
            .0
            .borrow_mut()
            .marker
            .get_or_insert_with(|| Marker {
                cutover: key.cutover.clone(),
                epoch: key.epoch,
                lsn: 42,
            })
            .clone())
    }
    fn unfence(&mut self, _: &Key, _: &mut Journal<'_>) -> Result<()> {
        let mut w = self.0.borrow_mut();
        assert!(w.destination_fenced && w.proof_count > 0);
        assert!(w.activated.is_none());
        w.source_fenced = false;
        Ok(())
    }
}
impl Destination for Fake {
    fn activated(&mut self, _: &Key) -> Result<Option<u64>> {
        Ok(self.0.borrow().activated)
    }
    fn prove_fenced(&mut self, _: &Key, _: &mut Journal<'_>) -> Result<()> {
        let mut w = self.0.borrow_mut();
        if !w.destination_fenced {
            return Err(Error::Component("destination fence missing".into()));
        }
        w.proof_count += 1;
        Ok(())
    }
    fn readiness(&mut self, key: &Key, j: &mut Journal<'_>) -> Result<()> {
        self.prove_fenced(key, j)
    }
    fn activate(
        &mut self,
        _: &Key,
        _: &Marker,
        _: &mut Journal<'_>,
        authorize: &mut dyn FnMut() -> Result<()>,
    ) -> Result<u64> {
        if let Some(receipt) = self.0.borrow().activated {
            return Ok(receipt);
        }
        if self.0.borrow().late_breach {
            self.0.borrow_mut().breach = true;
        }
        authorize()?;
        let mut w = self.0.borrow_mut();
        assert!(w.source_fenced);
        if w.activated.is_none() {
            w.activation_count += 1;
            w.activated = Some(100);
            w.destination_fenced = false;
        }
        Ok(w.activated.unwrap())
    }
    fn post_activate(&mut self, _: &Key, _: &mut Journal<'_>) -> Result<()> {
        self.0.borrow_mut().hook_count = 1; // Idempotent hook sink.
        Ok(())
    }
}
impl Progress for Fake {
    fn buffered(&mut self, _: &Marker) -> Result<Option<Marker>> {
        let w = self.0.borrow();
        Ok(if w.buffered { w.marker.clone() } else { None })
    }
    fn applied(&mut self, _: &Marker) -> Result<Option<Marker>> {
        let w = self.0.borrow();
        Ok(if w.applied {
            w.applied_identity.clone().or_else(|| w.marker.clone())
        } else {
            None
        })
    }
}
impl Verifier for Fake {
    fn boundary(&mut self, marker: &Marker) -> Result<VerifiedBoundary> {
        let complete = self.0.borrow().complete_coverage;
        let mut verified_marker = marker.clone();
        if self.0.borrow().verifier_wrong_marker {
            verified_marker.lsn += 1;
        }
        Ok(VerifiedBoundary {
            marker: verified_marker,
            coverage: Coverage {
                total: 9,
                verified: if complete { 9 } else { 8 },
                busy: u64::from(!complete),
                mismatched: 0,
                excluded: 0,
            },
        })
    }
}
impl Sequences for Fake {
    fn sync(&mut self, _: &Marker) -> Result<()> {
        if self.0.borrow().sequence_breach {
            return Err(Error::FenceBreached("sequence changed".into()));
        }
        Ok(())
    }
}
impl Reverse for Fake {
    fn confirm(&mut self, _: &Key, _: &Marker) -> Result<()> {
        Ok(())
    }
}
impl Status for Fake {
    fn readiness(&mut self, _: &Key) -> Result<()> {
        Ok(())
    }
    fn breached(&mut self, _: &Key) -> Result<bool> {
        Ok(self.0.borrow().breach)
    }
    fn now_ms(&mut self) -> Result<u64> {
        let mut w = self.0.borrow_mut();
        w.clock += 10;
        Ok(w.clock)
    }
    fn audit(&mut self, _: &Key, audit: &Audit) -> Result<()> {
        self.0.borrow_mut().audit = Some(audit.clone());
        Ok(())
    }
}
struct Owner {
    epoch: i64,
    coordinator: String,
    busy: bool,
}
impl Authority for Owner {
    fn run<T>(&mut self, key: &Key, run: impl FnOnce() -> Result<T>) -> Result<T> {
        if key.epoch != self.epoch || key.coordinator != self.coordinator || self.busy {
            return Err(Error::LostOwnership);
        }
        self.busy = true;
        let result = run();
        self.busy = false;
        result
    }
}
struct Harness {
    state: State,
    owner: Owner,
    fake: Fake,
}
impl Harness {
    fn new() -> Self {
        let key = Key {
            migration: "migration".into(),
            cutover: "final-1".into(),
            epoch: 1,
            coordinator: "one".into(),
        };
        Self {
            state: State::new(key).unwrap(),
            owner: Owner {
                epoch: 1,
                coordinator: "one".into(),
                busy: false,
            },
            fake: Fake(Rc::new(RefCell::new(World {
                destination_fenced: true,
                buffered: true,
                applied: true,
                complete_coverage: true,
                ..World::default()
            }))),
        }
    }
    fn step(&mut self, j: &mut Journal<'_>) -> Result<()> {
        self.state.execute_next(
            &mut self.owner,
            &mut Components {
                source: &mut self.fake.clone(),
                destination: &mut self.fake.clone(),
                progress: &mut self.fake.clone(),
                verifier: &mut self.fake.clone(),
                sequences: &mut self.fake.clone(),
                reverse: &mut self.fake.clone(),
                status: &mut self.fake.clone(),
            },
            j,
        )
    }
    fn until(&mut self, step: Step) {
        for _ in 0..20 {
            if self.state.next == step {
                return;
            }
            self.step(&mut |_| Ok(())).unwrap();
        }
        panic!("did not reach {step:?}");
    }
    fn abort(&mut self, j: &mut Journal<'_>) -> Result<()> {
        self.state.abort(
            &mut self.owner,
            &mut self.fake.clone(),
            &mut self.fake.clone(),
            j,
        )
    }
}
const STEPS: [Step; 11] = [
    Step::Readiness,
    Step::Intent,
    Step::Fence,
    Step::FinalMarker,
    Step::Boundary,
    Step::Sequences,
    Step::DestinationReadiness,
    Step::Reverse,
    Step::Activate,
    Step::PostActivate,
    Step::Audit,
];

#[test]
fn crash_at_each_cutover_step_recovers_consistent_authority() {
    for step in STEPS {
        for after_effect in [false, true] {
            let mut h = Harness::new();
            h.until(step);
            let mut durable = h.state.clone();
            assert!(matches!(
                h.step(&mut |r| {
                    match r {
                        Record::Intent { state, .. } => {
                            durable = state.clone();
                            if !after_effect {
                                return Err("crash after durable intent".into());
                            }
                        }
                        Record::Checkpoint(_) => {
                            return Err("crash after effect, before acknowledgement".into());
                        }
                        _ => {}
                    }
                    Ok(())
                }),
                Err(Error::Journal(_))
            ));
            // A fresh machine, recovered only from durable callback state.
            h.state = serde_json::from_str(&serde_json::to_string(&durable).unwrap()).unwrap();
            h.until(Step::Complete);
            let w = h.fake.0.borrow();
            assert!(w.source_fenced && !w.destination_fenced, "{step:?}");
            assert_eq!(w.activation_count, 1, "{step:?}");
            assert_eq!(w.hook_count, 1);
            assert_eq!(h.state.audit, w.audit);
        }
    }
}
#[test]
fn abort_before_activation_requires_proof_and_restores_source() {
    let mut h = Harness::new();
    h.until(Step::Activate);
    h.fake.0.borrow_mut().destination_fenced = false;
    assert!(matches!(h.abort(&mut |_| Ok(())), Err(Error::Component(_))));
    assert!(h.fake.0.borrow().source_fenced);
    assert_eq!(h.state.next, Step::Abort);
    h.fake.0.borrow_mut().destination_fenced = true;
    h.step(&mut |_| Ok(())).unwrap();
    assert_eq!(h.state.next, Step::Aborted);
    assert!(!h.fake.0.borrow().source_fenced);
    h.abort(&mut |_| Ok(())).unwrap();
}
#[test]
fn abort_after_activation_refused_even_after_lost_reply() {
    let mut h = Harness::new();
    h.until(Step::Activate);
    let pending = h.state.clone();
    h.step(&mut |_| Ok(())).unwrap();
    h.state = pending;
    assert_eq!(h.abort(&mut |_| Ok(())), Err(Error::UseRollback));
    assert!(h.fake.0.borrow().source_fenced);
}
#[test]
fn breach_before_activation_aborts_cutover() {
    for step in STEPS.into_iter().skip(3).take(6) {
        let mut h = Harness::new();
        h.until(step);
        h.fake.0.borrow_mut().breach = true;
        assert_eq!(h.step(&mut |_| Ok(())), Err(Error::BreachAborted));
        assert_eq!(h.state.next, Step::Aborted);
        assert!(!h.fake.0.borrow().source_fenced);
        assert_eq!(h.fake.0.borrow().activation_count, 0);
    }
}
#[test]
fn competing_epoch_cannot_double_activate() {
    let mut h = Harness::new();
    h.until(Step::Activate);
    let stale = h.state.clone();
    h.owner.epoch = 2;
    h.owner.coordinator = "two".into();
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::LostOwnership));
    h.state.key.epoch = 2;
    h.state.key.coordinator = "two".into();
    h.step(&mut |_| Ok(())).unwrap();
    h.state = stale;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::LostOwnership));
    h.state.key.coordinator = "other".into();
    h.state.key.epoch = 2;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::LostOwnership));
    h.state.key.coordinator = "two".into();
    h.step(&mut |_| Ok(())).unwrap();
    assert_eq!(h.fake.0.borrow().activation_count, 1);
}
#[test]
fn waits_require_marker_itself_buffered_and_applied() {
    let mut h = Harness::new();
    h.until(Step::Boundary);
    for (buffered, applied) in [(false, false), (false, true), (true, false)] {
        h.fake.0.borrow_mut().buffered = buffered;
        h.fake.0.borrow_mut().applied = applied;
        // An arbitrarily high numeric watermark cannot satisfy either receipt.
        assert_eq!(h.step(&mut |_| Ok(())), Err(Error::Pending));
        assert_eq!(h.state.next, Step::Boundary);
    }
    h.fake.0.borrow_mut().buffered = true;
    h.fake.0.borrow_mut().applied = true;
    h.fake.0.borrow_mut().marker.as_mut().unwrap().lsn += 1;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::Pending));
    h.fake.0.borrow_mut().marker.as_mut().unwrap().lsn -= 1;
    let mut wrong_applied = h.state.marker.clone().unwrap();
    wrong_applied.lsn += 1;
    h.fake.0.borrow_mut().applied_identity = Some(wrong_applied);
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::Pending));
    h.fake.0.borrow_mut().applied_identity = None;
    h.state.marker.as_mut().unwrap().epoch += 1;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::Pending));
    h.state.marker.as_mut().unwrap().epoch -= 1;
    h.step(&mut |_| Ok(())).unwrap();
    assert_eq!(h.state.next, Step::Sequences);
}
#[test]
fn verification_must_cover_the_complete_boundary() {
    let mut h = Harness::new();
    h.until(Step::Boundary);
    h.fake.0.borrow_mut().complete_coverage = false;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::Verification));
    assert_eq!(h.state.next, Step::Boundary);
}
#[test]
fn audit_has_all_fields_and_measured_pause() {
    let mut h = Harness::new();
    h.until(Step::Complete);
    assert_eq!(
        h.state.audit.unwrap(),
        Audit {
            fence_start_ms: 10,
            fence_end_ms: 20,
            drained: DrainCounts {
                terminated: 3,
                termination_outcome_unknown: false,
                remaining_sessions: 0,
                prepared_transactions: 0
            },
            final_lsn: 42,
            coverage: Coverage {
                total: 9,
                verified: 9,
                busy: 0,
                mismatched: 0,
                excluded: 0
            },
            activated_at_ms: 100,
            write_pause_ms: 90
        }
    );
}
#[test]
fn failed_journal_prevents_effect_and_abort() {
    let mut h = Harness::new();
    h.until(Step::Fence);
    assert!(matches!(
        h.step(&mut |_| Err("unavailable".into())),
        Err(Error::Journal(_))
    ));
    assert!(!h.fake.0.borrow().source_fenced);
    h.until(Step::Activate);
    assert!(matches!(
        h.abort(&mut |_| Err("unavailable".into())),
        Err(Error::Journal(_))
    ));
    assert!(h.fake.0.borrow().source_fenced);
}
#[test]
fn abort_crash_retries_without_reactivating_source_early() {
    let mut h = Harness::new();
    h.until(Step::Activate);
    let mut durable = h.state.clone();
    assert!(
        h.abort(&mut |r| match r {
            Record::Intent { state, .. } => {
                durable = state.clone();
                Ok(())
            }
            Record::Checkpoint(_) => Err("crash".into()),
            _ => Ok(()),
        })
        .is_err()
    );
    h.state = durable;
    h.step(&mut |_| Ok(())).unwrap();
    assert_eq!(h.state.next, Step::Aborted);
    assert_eq!(h.fake.0.borrow().activation_count, 0);
}

#[test]
fn component_reported_sequence_breach_aborts() {
    let mut h = Harness::new();
    h.until(Step::Sequences);
    h.fake.0.borrow_mut().sequence_breach = true;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::BreachAborted));
    assert_eq!(h.state.next, Step::Aborted);
    assert!(!h.fake.0.borrow().source_fenced);
}
#[test]
fn verifier_receipt_must_name_the_exact_boundary() {
    let mut h = Harness::new();
    h.until(Step::Boundary);
    h.fake.0.borrow_mut().verifier_wrong_marker = true;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::Verification));
}

#[test]
fn breach_during_activation_checks_is_seen_before_restoring_access() {
    let mut h = Harness::new();
    h.until(Step::Activate);
    h.fake.0.borrow_mut().late_breach = true;
    assert_eq!(h.step(&mut |_| Ok(())), Err(Error::BreachAborted));
    assert_eq!(h.state.next, Step::Aborted);
    assert_eq!(h.fake.0.borrow().activation_count, 0);
}
