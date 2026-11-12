//! Durable rollback protocol. This is a journal contract, not an effect driver.
//! Admission is deliberately not wired into the owner until all adapters exist.
use super::{Error, Materialized, Result, State};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Step {
    PreHook,
    FenceDestination,
    Drain,
    FinalMarker,
    ReverseApply,
    Verify,
    Sequences,
    RestoreSource,
    PostHook,
    Complete,
    RestoreDestination,
    Aborted,
}
impl Step {
    fn next(self) -> Option<Self> {
        use Step::*;
        Some(match self {
            PreHook => FenceDestination,
            FenceDestination => Drain,
            Drain => FinalMarker,
            FinalMarker => ReverseApply,
            ReverseApply => Verify,
            Verify => Sequences,
            Sequences => RestoreSource,
            RestoreSource => PostHook,
            PostHook => Complete,
            RestoreDestination => Aborted,
            Complete | Aborted => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub step: Step,
    pub epoch: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    /// Stable identity across owner changes. Native receipts must use this key.
    pub request: String,
    pub deadline_at_ms: u64,
    pub next: Step,
    pub intent: Option<Intent>,
    pub final_marker: Option<u64>,
    /// Conservative point of no return: publication of ReverseApply intent,
    /// before any source transaction can commit. Remains true after recovery.
    pub point_of_no_return: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Record {
    Admit {
        request: String,
        deadline_at_ms: u64,
    },
    Intent {
        step: Step,
    },
    Checkpoint {
        step: Step,
        final_marker: Option<u64>,
    },
    /// Must be followed by RestoreDestination intent and its completion.
    /// The lifecycle remains RollingBack until restoration is acknowledged.
    DeadlineAbort,
}

fn rejected() -> Error {
    Error::Rejected(
        "rollback journal prerequisite differs; keep fences and inspect recovery evidence".into(),
    )
}

pub(super) fn apply(state: &mut Materialized, record: &Record, seq: u64, at: u64) -> Result<()> {
    if let Record::Admit {
        request,
        deadline_at_ms,
    } = record
    {
        if state.lifecycle.state != State::RollbackWindow
            || state.rollback_eligibility_ended.is_some()
            || state
                .rollback
                .as_ref()
                .is_some_and(|cp| cp.next != Step::Aborted)
            || state
                .rollback
                .as_ref()
                .is_some_and(|cp| cp.request == *request)
            || request.is_empty()
            || *deadline_at_ms <= at
        {
            return Err(rejected());
        }
        state.rollback = Some(Checkpoint {
            request: request.clone(),
            deadline_at_ms: *deadline_at_ms,
            next: Step::PreHook,
            intent: None,
            final_marker: None,
            point_of_no_return: false,
        });
        state.lifecycle.state = State::RollingBack;
        state.lifecycle.authority = super::Authority::Neither;
        state.lifecycle.since_seq = seq;
        return Ok(());
    }
    if state.lifecycle.state != State::RollingBack {
        return Err(rejected());
    }
    let epoch = state.epoch;
    let ineligible = state.rollback_eligibility_ended.is_some();
    let cp = state.rollback.as_mut().ok_or_else(rejected)?;
    match record {
        Record::Admit { .. } => unreachable!(),
        Record::Intent { step } => {
            if *step != cp.next
                || step.next().is_none()
                || (ineligible && *step != Step::PostHook)
                || (at >= cp.deadline_at_ms
                    && !cp.point_of_no_return
                    && *step != Step::RestoreDestination)
            {
                return Err(rejected());
            }
            // Re-publication under a new epoch adopts the same effect identity.
            cp.intent = Some(Intent {
                step: *step,
                epoch,
                seq,
            });
            if *step == Step::ReverseApply {
                cp.point_of_no_return = true;
            }
        }
        Record::Checkpoint { step, final_marker } => {
            let intent = cp.intent.as_ref().ok_or_else(rejected)?;
            if *step != cp.next
                || intent.step != *step
                || intent.epoch != epoch
                || intent.seq >= seq
                || (*step == Step::FinalMarker) != final_marker.is_some()
                || final_marker == &Some(0)
            {
                return Err(rejected());
            }
            cp.next = step.next().ok_or_else(rejected)?;
            cp.intent = None;
            if let Some(marker) = final_marker {
                cp.final_marker = Some(*marker);
            }
            let terminal = match cp.next {
                Step::Complete => Some(State::SourceActiveAfterRollback),
                Step::Aborted => Some(State::RollbackWindow),
                _ => None,
            };
            if let Some(terminal) = terminal {
                state.lifecycle.state = terminal;
                state.lifecycle.authority = if terminal == State::RollbackWindow {
                    super::Authority::Destination
                } else {
                    super::Authority::Source
                };
                state.lifecycle.since_seq = seq;
            }
        }
        Record::DeadlineAbort => {
            if ineligible
                || cp.point_of_no_return
                || at < cp.deadline_at_ms
                || matches!(
                    cp.next,
                    Step::Complete | Step::Aborted | Step::RestoreDestination
                )
            {
                return Err(rejected());
            }
            cp.next = Step::RestoreDestination;
            cp.intent = None;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{Transition, journal::Entry};

    fn window() -> Materialized {
        let mut state = Materialized {
            epoch: 1,
            owner: "owner".into(),
            ..Default::default()
        };
        state.lifecycle.state = State::RollbackWindow;
        state
    }
    fn record(state: &mut Materialized, value: Record) -> Result<()> {
        let entry = Entry::new(
            state,
            Transition::Rollback {
                record: Box::new(value),
            },
            &state.owner,
        )?;
        state.apply(&entry)
    }
    fn admit(state: &mut Materialized) {
        record(
            state,
            Record::Admit {
                request: "rollback-1".into(),
                deadline_at_ms: u64::MAX,
            },
        )
        .unwrap();
    }
    fn checkpoint(step: Step) -> Record {
        Record::Checkpoint {
            step,
            final_marker: (step == Step::FinalMarker).then_some(42),
        }
    }

    #[test]
    fn generic_lifecycle_cannot_bypass_restore_checkpoint() {
        let mut state = window();
        admit(&mut state);
        let before = state.clone();
        let entry = Entry::new(
            &state,
            Transition::Lifecycle {
                event: super::super::Event::Advance {
                    to: State::SourceActiveAfterRollback,
                },
            },
            &state.owner,
        )
        .unwrap();
        assert!(state.apply(&entry).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn admission_is_atomic_and_refuses_invalid_inputs() {
        for lifecycle in State::ALL {
            let mut state = window();
            state.lifecycle.state = lifecycle;
            let before = state.clone();
            let result = record(
                &mut state,
                Record::Admit {
                    request: "r".into(),
                    deadline_at_ms: u64::MAX,
                },
            );
            assert_eq!(result.is_ok(), lifecycle == State::RollbackWindow);
            if result.is_err() {
                assert_eq!(state, before);
            }
        }
        for (request, deadline) in [("", u64::MAX), ("r", 0)] {
            assert!(
                record(
                    &mut window(),
                    Record::Admit {
                        request: request.into(),
                        deadline_at_ms: deadline
                    }
                )
                .is_err()
            );
        }
    }

    #[test]
    fn all_boundaries_roundtrip_and_require_current_epoch_intent() {
        let mut state = window();
        admit(&mut state);
        while state.rollback.as_ref().unwrap().next != Step::Complete {
            let step = state.rollback.as_ref().unwrap().next;
            assert!(record(&mut state, checkpoint(step)).is_err());
            record(&mut state, Record::Intent { step }).unwrap();
            // Simulate recovery from a checkpoint at the intent/effect boundary.
            state = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
            let entry = Entry::new(
                &state,
                Transition::AcquireOwnership {
                    epoch: state.epoch + 1,
                },
                "recovered",
            )
            .unwrap();
            state.apply(&entry).unwrap();
            assert!(record(&mut state, checkpoint(step)).is_err());
            record(&mut state, Record::Intent { step }).unwrap();
            record(&mut state, checkpoint(step)).unwrap();
            state = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        }
        assert_eq!(state.lifecycle.state, State::SourceActiveAfterRollback);
        assert_eq!(state.lifecycle.authority, super::super::Authority::Source);
        let cp = state.rollback.as_ref().unwrap();
        assert_eq!(cp.final_marker, Some(42));
        assert!(cp.point_of_no_return);
    }

    #[test]
    fn deadline_abort_needs_restoration_checkpoint() {
        let mut state = window();
        admit(&mut state);
        assert!(record(&mut state, Record::DeadlineAbort).is_err());
        state.rollback.as_mut().unwrap().deadline_at_ms = 1;
        record(&mut state, Record::DeadlineAbort).unwrap();
        assert_eq!(state.lifecycle.state, State::RollingBack);
        assert_eq!(state.lifecycle.authority, super::super::Authority::Neither);
        assert!(record(&mut state, checkpoint(Step::RestoreDestination)).is_err());
        record(
            &mut state,
            Record::Intent {
                step: Step::RestoreDestination,
            },
        )
        .unwrap();
        record(&mut state, checkpoint(Step::RestoreDestination)).unwrap();
        assert_eq!(state.lifecycle.state, State::RollbackWindow);
        assert_eq!(
            state.lifecycle.authority,
            super::super::Authority::Destination
        );
        assert!(
            record(
                &mut state,
                Record::Admit {
                    request: "rollback-1".into(),
                    deadline_at_ms: u64::MAX
                }
            )
            .is_err()
        );
        record(
            &mut state,
            Record::Admit {
                request: "rollback-2".into(),
                deadline_at_ms: u64::MAX,
            },
        )
        .unwrap();
    }

    #[test]
    fn apply_intent_permanently_closes_abort_even_before_effect_reply() {
        let mut state = window();
        admit(&mut state);
        for step in [
            Step::PreHook,
            Step::FenceDestination,
            Step::Drain,
            Step::FinalMarker,
        ] {
            record(&mut state, Record::Intent { step }).unwrap();
            record(&mut state, checkpoint(step)).unwrap();
        }
        record(
            &mut state,
            Record::Intent {
                step: Step::ReverseApply,
            },
        )
        .unwrap();
        state.rollback.as_mut().unwrap().deadline_at_ms = 1;
        assert!(record(&mut state, Record::DeadlineAbort).is_err());
        record(&mut state, checkpoint(Step::ReverseApply)).unwrap();
        record(&mut state, Record::Intent { step: Step::Verify }).unwrap();
    }

    #[test]
    fn eligibility_loss_holds_rolling_back_without_releasing_either_side() {
        let mut state = window();
        admit(&mut state);
        for step in [Step::PreHook, Step::FenceDestination] {
            record(&mut state, Record::Intent { step }).unwrap();
            record(&mut state, checkpoint(step)).unwrap();
        }
        let entry = Entry::new(
            &state,
            Transition::RollbackEligibilityEnded {
                reason: super::super::reverse::EligibilityEnd::SourceFenceBreach,
            },
            &state.owner,
        )
        .unwrap();
        state.apply(&entry).unwrap();
        state.rollback.as_mut().unwrap().deadline_at_ms = 1;
        let before = state.clone();
        assert!(record(&mut state, Record::DeadlineAbort).is_err());
        assert!(record(&mut state, Record::Intent { step: Step::Drain }).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn out_of_order_steps_and_marker_substitution_are_rejected() {
        let mut state = window();
        admit(&mut state);
        assert!(
            record(
                &mut state,
                Record::Intent {
                    step: Step::RestoreSource
                }
            )
            .is_err()
        );
        record(
            &mut state,
            Record::Intent {
                step: Step::PreHook,
            },
        )
        .unwrap();
        assert!(
            record(
                &mut state,
                Record::Checkpoint {
                    step: Step::PreHook,
                    final_marker: Some(42)
                }
            )
            .is_err()
        );
        record(&mut state, checkpoint(Step::PreHook)).unwrap();
        assert!(record(&mut state, checkpoint(Step::PreHook)).is_err());
    }
}
