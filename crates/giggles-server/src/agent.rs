//! Agent sessions (GD-0061 R3-204, design-2 sections 2 and 3): the agent binding
//! map, task selection at connect and per-call revalidation. The session
//! constructor and the `check_binding` branch live in `service.rs` next to the
//! ordinary ones; this module holds the parts that are agent-only.

pub mod bindings;
pub mod listener;
pub(crate) mod preview;
pub mod statement;

pub use bindings::AgentBinding;
pub use listener::{AgentConfig, serve_agent_pg_bound};

use giggles_core::{
    task::{
        context::Standing,
        definition::WallClockMs,
        StatementClass, TaskError,
    },
    CoreError,
};

/// D5: operation-ID outcomes stay readable 30 days after a task expires, and an
/// expired task's credential connects until then.
pub const DEFAULT_OUTCOME_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// Unix milliseconds of 2000-01-01 00:00:00 UTC, the epoch of `Clock::sql_micros`.
const PG_EPOCH_UNIX_MS: i64 = 946_684_800_000;

/// The wall clock as task code reads it. An unavailable clock is an error, and
/// every caller fails closed on it.
pub(crate) fn wall_now(clock: &crate::clock::Clock) -> Result<WallClockMs, CoreError> {
    let micros = clock.sql_micros()?;
    Ok(WallClockMs(micros.div_euclid(1000).saturating_add(PG_EPOCH_UNIX_MS)))
}

/// Step 3 of per-call revalidation: an expired task may still look up outcomes,
/// show and set; everything else is `Expired`.
pub(crate) fn standing_admits(standing: Standing, class: StatementClass) -> Result<(), TaskError> {
    match standing {
        Standing::Live => Ok(()),
        Standing::Expired
            if matches!(
                class,
                StatementClass::OutcomeLookup | StatementClass::Show | StatementClass::Set
            ) =>
        {
            Ok(())
        }
        Standing::Expired => Err(TaskError::Expired),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_admits_only_outcome_show_set() {
        for class in StatementClass::ALL {
            assert!(standing_admits(Standing::Live, class).is_ok(), "{class:?}");
            let got = standing_admits(Standing::Expired, class);
            if matches!(
                class,
                StatementClass::OutcomeLookup | StatementClass::Show | StatementClass::Set
            ) {
                assert!(got.is_ok(), "{class:?}");
            } else {
                assert_eq!(got, Err(TaskError::Expired), "{class:?}");
            }
        }
    }

    /// The catalog has no definition change short of REVOKE, so the session's
    /// bound version is moved behind the stored one instead: same comparison.
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn definition_change_gives_42501() {
        use giggles_core::{
            roles::{NewRole, TablePrivilege},
            task::{
                definition::{DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText},
                DenyRule,
            },
            Admin, Engine, Limits,
        };
        let path = crate::test_support::fixture_root().join(format!(
            "agent-defchange-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin =
            Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
                &[],
                None,
            )
            .unwrap();
        let roles = admin.roles();
        for name in ["incident_controller", "agent_exec"] {
            roles
                .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
                .unwrap();
        }
        roles.set_task_only("agent_exec", true).unwrap();
        roles
            .grant_table("shipments", "incident_controller", &[TablePrivilege::Select])
            .unwrap();
        let issuer = roles.get("incident_controller").unwrap().unwrap();
        let clock = crate::clock::Clock::new(0, 3600).unwrap();
        let now = wall_now(&clock).unwrap();
        admin
            .tasks()
            .create(
                &issuer,
                NewTask {
                    name: "inc-436".into(),
                    executor: "agent_exec".into(),
                    approver: None,
                    schema: None,
                    table: "shipments".into(),
                    visible_rows: PredicateText("merchant_id = 42".into()),
                    readable: vec!["id".into(), "status".into()],
                    masked: Vec::new(),
                    untrusted_text: Vec::new(),
                    allowed_write: None,
                    mutation_caps: MutationCaps::default(),
                    disclosure_caps: DisclosureCaps { rows_read: 10, rows_returned: 10, bytes_returned: 4096 },
                    limits: LoadLimits::default(),
                    approval_threshold: 0,
                    expires: "2999-01-01 00:00:00+00".into(),
                },
                now,
            )
            .unwrap();
        let fp = "aa".repeat(32);
        let service = crate::Service::with_agent(
            admin.clone(),
            vec![],
            vec![AgentBinding { fingerprint: fp.clone(), executor: "agent_exec".into(), tasks: vec!["inc-436".into()] }],
            clock,
            crate::admission::ServiceLimits::default(),
        )
        .unwrap();
        let mut session = service.agent_session(&fp, None).unwrap();
        session.admit(StatementClass::Show).unwrap();
        let mut stale = session.task().unwrap().clone();
        stale.bound_def_version = 0;
        let mut sql = admin.sql_session();
        sql.bind_task(stale).unwrap();
        session.sql = sql;
        let error = session.admit(StatementClass::Show).unwrap_err();
        assert_eq!(error, CoreError::Task(TaskError::Denied(DenyRule::DefinitionChanged)));
        assert_eq!(giggles_core::sqlstate(&error), "42501");
    }
}
