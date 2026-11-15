//! Resumable, operator-started voter replacement.
use crate::{
    membership::{self, AdminCommand, CATCH_UP_DISTANCE, Next, PeerRecord},
    sink::{MembershipStatus, ReplicaHandle, ReplicateError},
};
use std::{
    fmt, thread,
    time::{Duration, Instant},
};

pub struct ReplaceRequest {
    pub lost: u64,
    pub new: PeerRecord,
}
#[derive(Debug)]
pub enum ReplaceError {
    NotLeader { leader: u64 },
    Refused(String),
    Timeout(String),
    Replica(ReplicateError),
}
impl fmt::Display for ReplaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader } => write!(f, "not leader; leader is {leader}"),
            Self::Refused(reason) => write!(f, "refused: {reason}"),
            Self::Timeout(step) => write!(f, "replacement timed out at {step}"),
            Self::Replica(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ReplaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Replica(e) => Some(e),
            _ => None,
        }
    }
}
fn error(handle: &ReplicaHandle, e: ReplicateError) -> ReplaceError {
    match e {
        ReplicateError::NotLeader => ReplaceError::NotLeader {
            leader: handle.role().leader,
        },
        ReplicateError::Refused(s) => ReplaceError::Refused(s),
        e => ReplaceError::Replica(e),
    }
}
pub fn status(handle: &ReplicaHandle) -> Result<MembershipStatus, ReplaceError> {
    handle.membership_status().map_err(|e| error(handle, e))
}
fn at_step(handle: &ReplicaHandle, e: ReplicateError, step: &str) -> ReplaceError {
    match error(handle, e) {
        ReplaceError::Replica(e) => {
            ReplaceError::Replica(ReplicateError::Unknown(format!("{e} at {step}")))
        }
        e => e,
    }
}
pub fn run(
    handle: &ReplicaHandle,
    req: &ReplaceRequest,
    timeout: Duration,
) -> Result<MembershipStatus, ReplaceError> {
    let start = Instant::now();
    let mut last_step = "read barrier".to_owned();
    loop {
        if start.elapsed() >= timeout {
            return Err(ReplaceError::Timeout(last_step));
        }
        handle
            .read_barrier()
            .map_err(|e| at_step(handle, e, &last_step))?;
        let s = handle
            .membership_status()
            .map_err(|e| at_step(handle, e, &last_step))?;
        if s.membership.active.is_none()
            && s.membership.is_retired(req.lost)
            && s.conf.voters.contains(&req.new.id)
        {
            return Ok(s);
        }
        if let Some(active) = &s.membership.active
            && (active.lost != req.lost || active.new != req.new)
        {
            return Err(ReplaceError::Refused(format!(
                "replacement of {} by {} is already running",
                active.lost, active.new.id
            )));
        }
        let next = s.membership.next();
        last_step = format!("{next:?}");
        let command = match next {
            Next::Idle => Some(AdminCommand::Enrol {
                lost: req.lost,
                peer: req.new.clone(),
            }),
            Next::CatchUp { node, added_at } => {
                if s.snapshot_index < added_at {
                    handle
                        .build_snapshot()
                        .map_err(|e| at_step(handle, e, &last_step))?;
                }
                s.learner_matched
                    .filter(|matched| {
                        *matched >= added_at
                            && s.last_index.saturating_sub(*matched) <= CATCH_UP_DISTANCE
                    })
                    .map(|matched| AdminCommand::CaughtUp { node, matched })
            }
            Next::Retire { node } => Some(AdminCommand::Retire { node }),
            _ => None,
        };
        let conf_step = matches!(next, Next::AddLearner | Next::EnterJoint | Next::LeaveJoint);
        if command.is_some() || conf_step {
            membership::check_safe(&s.conf, req.lost, s.role.leader, &s.reachable)
                .map_err(|e| ReplaceError::Refused(e.to_string()))?;
            let result = if let Some(cmd) = command {
                handle.propose_admin(cmd)
            } else {
                handle.propose_conf_change(
                    s.membership
                        .expected_conf_change()
                        .expect("configuration step"),
                )
            };
            match result {
                Ok(()) => {}
                Err(ReplicateError::Refused(reason)) => return Err(ReplaceError::Refused(reason)),
                Err(_) => {
                    let _ = handle.membership_status();
                }
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
}
