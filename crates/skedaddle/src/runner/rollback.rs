//! Rollback request boundary. No observer acquires database authority.
//! Only the acquired post-activation owner handles requests. Admission stays
//! disabled until the complete transfer and deadline-restoration driver is qualified.
//! Previously admitted journals retain their source fence for explicit recovery.
use super::verification::VerifyResponse;
use crate::{
    coordinator::{self, Materialized, State, Transition, rollback::Record},
    engine::Runtime,
};
use serde_json::json;
use std::{path::Path, time::Duration};

// Enable only with both deadline restoration before reverse apply and completion
// after reverse apply implemented and tested. Never admit into a partial driver.
const ROLLBACK_DRIVER_AVAILABLE: bool = false;

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

/// Recovery enters here before health probes, finalize or reverse-worker startup.
/// No checkpoint or effect intent may be fabricated when an adapter is missing.
/// Keep the admitted identity and deadline intact for the real transfer driver.
pub(super) async fn run(runtime: &Runtime) -> coordinator::Result<VerifyResponse> {
    runtime.refresh_owner().await?;
    let state = runtime.state().await;
    if state.lifecycle.state != State::RollingBack || state.rollback.is_none() {
        return Err(coordinator::Error::RecoveryRequired(
            "rollback admission evidence unavailable; operator action required".into(),
        ));
    }
    let mut response = refused(
        if state.rollback_eligibility_ended.is_some() {
            "rollback_operator_action_required"
        } else {
            "rollback_driver_unavailable"
        },
        5,
    );
    response.data["admitted"] = json!(true);
    response.data["final_state"] = json!(State::RollingBack);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::reverse::EligibilityEnd;

    #[test]
    fn refuses_every_non_window_state_without_mutating_materialized_state() {
        let mut state = Materialized::default();
        for lifecycle in State::ALL {
            state.lifecycle.state = lifecycle;
            let before = serde_json::to_vec(&state).unwrap();
            let response = admission(&state, Some(30_000));
            if lifecycle == State::RollbackWindow {
                let response = response.unwrap();
                assert_eq!(response.exit_code, 2);
                assert_eq!(
                    response.data["error"]["kind"],
                    "rollback_driver_unavailable"
                );
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
