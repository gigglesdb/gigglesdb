//! Rollback request boundary. No observer acquires database authority.
//! The transfer driver must be connected before admission may journal an intent
//! or advance RollingBack. Until then, an eligible request fails closed.
use super::verification::VerifyResponse;
use crate::coordinator::{Materialized, State};
use serde_json::json;
use std::{path::Path, time::Duration};

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

pub(super) fn admission(state: &Materialized, deadline_ms: Option<u64>) -> VerifyResponse {
    if deadline_ms.is_none_or(|value| value == 0) {
        return refused("invalid_deadline", 3);
    }
    if state.lifecycle.state != State::RollbackWindow {
        return refused("invalid_state", 3);
    }
    if state.rollback_eligibility_ended.is_some() {
        return refused("rollback_ineligible", 3);
    }
    // Do not record a durable admission that recovery cannot yet execute.
    refused("rollback_driver_unavailable", 2)
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
            assert_eq!(
                response.exit_code,
                if lifecycle == State::RollbackWindow {
                    2
                } else {
                    3
                }
            );
            assert_eq!(before, serde_json::to_vec(&state).unwrap());
        }
    }

    #[test]
    fn ended_eligibility_and_invalid_deadlines_are_refused() {
        let mut state = Materialized::default();
        state.lifecycle.state = State::RollbackWindow;
        state.rollback_eligibility_ended = Some(EligibilityEnd::SourceFenceBreach);
        assert_eq!(
            admission(&state, Some(1)).data["error"]["kind"],
            "rollback_ineligible"
        );
        for deadline in [None, Some(0)] {
            assert_eq!(
                admission(&state, deadline).data["error"]["kind"],
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
