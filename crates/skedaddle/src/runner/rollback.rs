//! Owner-only rollback admission, resumable execution and outcome mapping.
//! Observers use the generation-scoped request transport and never acquire authority.
mod live;
use super::verification::VerifyResponse;
use crate::{
    coordinator::{self, Materialized, State, Transition, rollback::Record},
    engine::Runtime,
};
use serde_json::json;
use std::{path::Path, time::Duration};

// Enable only with both deadline restoration before reverse apply and completion
// after reverse apply implemented and tested. Never admit into a partial driver.
const ROLLBACK_DRIVER_AVAILABLE: bool = true;
const OPERATOR: &str = "runner-rollback-operator-v1";

fn refused(kind: &str, exit_code: u8) -> VerifyResponse {
    VerifyResponse {
        exit_code,
        data: json!({"complete":false,"error":{"kind":kind}}),
    }
}

/// Generation-scoped transport shared with finalize. This process never opens
/// either endpoint, acquires an epoch, or changes the migration lifecycle.
pub async fn request_rollback(root: &Path, id: &str, deadline: Duration) -> VerifyResponse {
    let Ok(deadline_ms) = u64::try_from(deadline.as_millis()) else {
        return refused("invalid_deadline", 3);
    };
    if deadline_ms == 0 {
        return refused("invalid_deadline", 3);
    }
    super::gc::request_with_deadline(root, id, "rollback", false, Some(deadline_ms)).await
}

pub(super) fn admission(state: &Materialized, deadline_ms: Option<u64>) -> Option<VerifyResponse> {
    if deadline_ms.is_none_or(|value| value == 0) {
        return Some(refused("invalid_deadline", 3));
    }
    if state.lifecycle.state != State::RollbackWindow {
        return Some(refused("invalid_state", 3));
    }
    if state.rollback_eligibility_ended.is_some() {
        return Some(refused("rollback_ineligible", 3));
    }
    if !ROLLBACK_DRIVER_AVAILABLE {
        return Some(refused("rollback_driver_unavailable", 2));
    }
    None
}

/// Admission is one authenticated journal entry, never a control-file marker.
/// The deadline is fixed once; takeover must not extend it.
pub(super) async fn admit(
    runtime: &Runtime,
    request: &str,
    deadline_ms: Option<u64>,
) -> coordinator::Result<Option<VerifyResponse>> {
    runtime.refresh_owner().await?;
    let state = runtime.state().await;
    if let Some(response) = admission(&state, deadline_ms) {
        return Ok(Some(response));
    }
    let now: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| coordinator::Error::Rejected("rollback clock unavailable".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| coordinator::Error::Rejected("rollback clock overflow".into()))?;
    let Some(deadline_at_ms) = now.checked_add(deadline_ms.unwrap_or(0)) else {
        return Ok(Some(refused("invalid_deadline", 3)));
    };
    runtime
        .commit(Transition::Rollback {
            record: Box::new(Record::Admit {
                request: request.into(),
                deadline_at_ms,
            }),
        })
        .await?;
    Ok(None)
}

/// A single owner slice; None keeps the request pending while status remains live.
pub(super) trait OwnerDriver {
    #[cfg(test)]
    fn simulated_healthy(&self) -> bool {
        false
    }
    async fn slice(
        &mut self,
        runtime: &Runtime,
        root: &Path,
        id: &str,
        context: Option<&(coordinator::journal::Journal, crate::MigrationDefinition)>,
    ) -> coordinator::Result<crate::cutover::Result<crate::rollback::driver::Outcome>>;
}
#[derive(Default)]
pub(super) struct Live(live::Thread);
impl OwnerDriver for Live {
    async fn slice(
        &mut self,
        runtime: &Runtime,
        root: &Path,
        id: &str,
        context: Option<&(coordinator::journal::Journal, crate::MigrationDefinition)>,
    ) -> coordinator::Result<crate::cutover::Result<crate::rollback::driver::Outcome>> {
        let Some((journal, definition)) = context else {
            return Ok(Err(crate::cutover::Error::Invalid(
                "rollback endpoints unavailable",
            )));
        };
        self.0.slice(runtime, id, journal, definition, root).await
    }
}
pub(super) async fn run_with_driver(
    runtime: &Runtime,
    root: &Path,
    id: &str,
    context: Option<&(coordinator::journal::Journal, crate::MigrationDefinition)>,
    driver: &mut impl OwnerDriver,
    workers: &mut Option<Box<super::reverse_workers::Workers>>,
) -> coordinator::Result<Option<VerifyResponse>> {
    runtime.refresh_owner().await?;
    let state = runtime.state().await;
    if state.lifecycle.state != State::RollingBack || state.rollback.is_none() {
        return Err(coordinator::Error::RecoveryRequired(
            "rollback admission evidence unavailable".into(),
        ));
    }
    if state.rollback_eligibility_ended.is_some()
        || state.progress.get(OPERATOR).is_some_and(|(_, request)| {
            state
                .rollback
                .as_ref()
                .is_some_and(|cp| &cp.request == request)
        })
    {
        return Ok(Some(refused("rollback_operator_action_required", 5)));
    }
    // Workers contain thread-local capture state. Retire sockets before the
    // blocking slice opens a fresh adapter from the retained durable boundary.
    *workers = None;
    let result = driver.slice(runtime, root, id, context).await?;
    match result {
        Ok(crate::rollback::driver::Outcome::Progress) | Err(crate::cutover::Error::Pending) => {
            runtime.refresh_owner().await?;
            Ok(None)
        }
        Ok(outcome) => {
            let state = runtime.state().await;
            Ok(Some(VerifyResponse {
                exit_code: outcome.exit_code().expect("terminal outcome"),
                data: json!({"complete":outcome==crate::rollback::driver::Outcome::Complete,"admitted":true,"final_state":state.lifecycle.state,"rollback":status(&state)}),
            }))
        }
        Err(crate::cutover::Error::LostOwnership) => Err(coordinator::Error::LostOwnership),
        Err(crate::cutover::Error::Component(_) | crate::cutover::Error::Journal(_)) => {
            Err(coordinator::Error::RecoveryRequired(
                "rollback infrastructure unavailable; retrying durable admission".into(),
            ))
        }
        Err(_) => {
            runtime.refresh_owner().await?;
            runtime
                .marker(
                    OPERATOR,
                    "operator-action-required".into(),
                    state.rollback.as_ref().expect("admitted").request.clone(),
                )
                .await?;
            Ok(Some(refused("rollback_operator_action_required", 5)))
        }
    }
}

/// Observer-only view of authenticated rollback progress. A terminal outcome
/// requires both the terminal checkpoint and its matching lifecycle state.
pub fn status(state: &Materialized) -> serde_json::Value {
    let Some(checkpoint) = &state.rollback else {
        return serde_json::Value::Null;
    };
    let outcome = match (state.lifecycle.state, checkpoint.next) {
        (State::SourceActiveAfterRollback, coordinator::rollback::Step::Complete) => "completed",
        (State::RollbackWindow, coordinator::rollback::Step::Aborted) => "deadline-aborted",
        _ if state.rollback_eligibility_ended.is_some()
            || state
                .progress
                .get(OPERATOR)
                .is_some_and(|(_, request)| request == &checkpoint.request) =>
        {
            "operator-action-required"
        }
        _ => "in-progress",
    };
    json!({"request":checkpoint.request,"step":checkpoint.next,
        "intent":checkpoint.intent,"deadline_at_ms":checkpoint.deadline_at_ms,
        "point_of_no_return":checkpoint.point_of_no_return,
        "final_marker":checkpoint.final_marker,"outcome":outcome})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::reverse::EligibilityEnd;

    #[test]
    fn status_requires_matching_checkpoint_and_lifecycle_for_terminal_outcome() {
        use coordinator::rollback::{Checkpoint, Step};
        let mut state = Materialized::default();
        assert!(status(&state).is_null());
        state.rollback = Some(Checkpoint {
            request: "request".into(),
            deadline_at_ms: 100,
            next: Step::ReverseApply,
            intent: None,
            final_marker: Some(50),
            capture_boundary: None,
            point_of_no_return: true,
        });
        state.lifecycle.state = State::RollingBack;
        assert_eq!(status(&state)["step"], "reverse-apply");
        assert_eq!(status(&state)["outcome"], "in-progress");
        state.rollback.as_mut().unwrap().next = Step::Complete;
        assert_eq!(status(&state)["outcome"], "in-progress");
        state.lifecycle.state = State::SourceActiveAfterRollback;
        assert_eq!(status(&state)["outcome"], "completed");
        state.rollback.as_mut().unwrap().next = Step::Aborted;
        state.lifecycle.state = State::RollingBack;
        assert_eq!(status(&state)["outcome"], "in-progress");
        state.lifecycle.state = State::RollbackWindow;
        assert_eq!(status(&state)["outcome"], "deadline-aborted");
    }
    #[test]
    fn refuses_every_non_window_state_without_mutating_materialized_state() {
        let mut state = Materialized::default();
        for lifecycle in State::ALL {
            state.lifecycle.state = lifecycle;
            let before = serde_json::to_vec(&state).unwrap();
            let response = admission(&state, Some(30_000));
            if lifecycle == State::RollbackWindow {
                assert!(response.is_none());
            } else {
                assert_eq!(response.unwrap().exit_code, 3);
            }
            assert_eq!(before, serde_json::to_vec(&state).unwrap());
        }
    }

    #[test]
    fn ended_eligibility_and_invalid_deadlines_are_refused() {
        let mut state = Materialized::default();
        state.lifecycle.state = State::RollbackWindow;
        state.rollback_eligibility_ended = Some(EligibilityEnd::SourceFenceBreach);
        assert_eq!(
            admission(&state, Some(1)).unwrap().data["error"]["kind"],
            "rollback_ineligible"
        );
        for deadline in [None, Some(0)] {
            assert_eq!(
                admission(&state, deadline).unwrap().data["error"]["kind"],
                "invalid_deadline"
            );
        }
    }

    #[tokio::test]
    async fn invalid_deadline_and_id_are_rejected_without_creating_control_files() {
        let root = std::path::PathBuf::from(
            std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set SKEDADDLE_TEST_ROOT"),
        )
        .join(format!(
            "nonexistent-rollback-request-{}",
            uuid::Uuid::new_v4()
        ));
        assert_eq!(
            request_rollback(&root, "migration", Duration::ZERO)
                .await
                .exit_code,
            3
        );
        assert_eq!(
            request_rollback(&root, "../bad", Duration::from_secs(1))
                .await
                .exit_code,
            3
        );
        assert_eq!(
            request_rollback(&root, "migration", Duration::from_secs(1))
                .await
                .exit_code,
            6
        );
        assert!(!root.exists());
    }
}
