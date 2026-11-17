//! R3-204 (GD-0061 design-2 sections 2 and 3): the agent binding map,
//! `Service::agent_session` and per-call revalidation, in-process.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::{
    task::{
        definition::{DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, TaskName},
        DenyRule, StatementClass, TaskError,
    },
    Admin, CoreError, Engine, Error, Limits,
    roles::{NewRole, RoleChanges, TablePrivilege},
};
use giggles_server::{
    admission::ServiceLimits, clock::Clock, sqlstate, AgentBinding, Binding, Request, Role,
    Service, Session,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static SERIAL: AtomicU64 = AtomicU64::new(0);
const FP_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FP_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FP_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Unix milliseconds as a PostgreSQL timestamptz literal in UTC.
fn timestamptz(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:03}+00",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

/// The worked example: `shipments` (`deliveries` is reserved), an issuer, two
/// task-only executors, a task-only role without LOGIN and an ordinary login.
fn admin() -> Admin {
    let root = std::path::PathBuf::from(
        std::env::var_os("TEMP").expect("set task TEMP under J:/gigglesdb-work"),
    );
    let path = root.join(format!(
        "agent-bindings-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
             failed_at timestamptz, attempt_count bigint, customer_email text, card_token text)",
            &[],
            None,
        )
        .unwrap();
    let roles = admin.roles();
    for (name, login, task_only) in [
        ("incident_controller", true, false),
        ("oncall_lead", true, false),
        ("agent_exec", true, true),
        ("other_exec", true, true),
        ("parked_exec", false, true),
    ] {
        roles
            .create(NewRole {
                name: name.into(),
                login,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    roles
        .grant_table("shipments", "incident_controller", &[TablePrivilege::Select])
        .unwrap();
    admin
}

fn create_task(admin: &Admin, name: &str, executor: &str, expires_ms: i64) {
    let issuer = admin.roles().get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: name.into(),
                executor: executor.into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into(), "status".into(), "failed_at".into()],
                masked: vec![("customer_email".into(), None)],
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 100,
                    rows_returned: 100,
                    bytes_returned: 65_536,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: timestamptz(expires_ms),
            },
            giggles_core::task::definition::WallClockMs(now_ms()),
        )
        .unwrap();
}

fn revoke(admin: &Admin, name: &str) {
    let issuer = admin.roles().get("incident_controller").unwrap().unwrap();
    admin.tasks().revoke(&issuer, name).unwrap();
}

fn agent(fp: &str, executor: &str, tasks: &[&str]) -> AgentBinding {
    AgentBinding {
        fingerprint: fp.into(),
        executor: executor.into(),
        tasks: tasks.iter().map(|t| t.to_string()).collect(),
    }
}

fn service(admin: &Admin, ordinary: Vec<Binding>, agents: Vec<AgentBinding>) -> Result<Arc<Service>, CoreError> {
    Service::with_agent(
        admin.clone(),
        ordinary,
        agents,
        Clock::new(0, 3600).unwrap(),
        ServiceLimits::default(),
    )
}

fn refused<T>(result: Result<T, CoreError>) {
    assert!(
        matches!(result, Err(CoreError::Policy(Error::Authentication))),
        "expected an authentication refusal"
    );
}

/// Per-call revalidation, step 1: "ok" while the binding holds, 28000 once cut.
fn ping(session: &mut Session) -> String {
    session.handle(Request::Ping).code
}

const DAY_MS: i64 = 86_400_000;

#[test]
fn fingerprint_in_both_maps_rejected() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec", now_ms() + DAY_MS);
    let ordinary = vec![Binding {
        fingerprint: FP_A.into(),
        role: Role::Database {
            name: "oncall_lead".into(),
        },
    }];
    refused(service(&admin, ordinary.clone(), vec![agent(FP_A, "agent_exec", &["inc-436"])]));
    // The same fingerprint twice in the agent map is refused as well.
    refused(service(
        &admin,
        vec![],
        vec![agent(FP_B, "agent_exec", &["inc-436"]), agent(FP_B, "agent_exec", &["inc-436"])],
    ));
    let svc = service(&admin, ordinary.clone(), vec![agent(FP_B, "agent_exec", &["inc-436"])]).unwrap();
    let mut live = svc.agent_session(FP_B, None).unwrap();
    // A reload that would put one certificate in both maps fails as a whole.
    let mut both = ordinary.clone();
    both.push(Binding {
        fingerprint: FP_B.into(),
        role: Role::Database {
            name: "oncall_lead".into(),
        },
    });
    refused(svc.reload_bindings(both.clone()));
    refused(svc.reload_all_bindings(ordinary, vec![agent(FP_A, "agent_exec", &["inc-436"])]));
    assert_eq!(ping(&mut live), "ok");
    refused(svc.session(FP_B));
}

#[test]
fn executor_must_be_login_task_only() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec", now_ms() + DAY_MS);
    for executor in ["oncall_lead", "parked_exec", "no_such_role"] {
        refused(service(&admin, vec![], vec![agent(FP_A, executor, &["inc-436"])]));
    }
    // Malformed entries are limits: bad fingerprint, no tasks, a bad task name.
    for bad in [
        agent("AAAA", "agent_exec", &["inc-436"]),
        agent(FP_A, "agent_exec", &[]),
        agent(FP_A, "agent_exec", &["inc 436"]),
    ] {
        assert!(matches!(service(&admin, vec![], vec![bad]), Err(CoreError::Limit)));
    }
    let svc = service(&admin, vec![], vec![agent(FP_A, "agent_exec", &["inc-436"])]).unwrap();
    let session = svc.agent_session(FP_A, None).unwrap();
    assert_eq!(session.task().unwrap().executor, "agent_exec");
    assert_eq!(session.task().unwrap().credential_fingerprint, [0xaa; 32]);
    // An ordinary binding to a task-only role is refused (R3-203), so the
    // executor cannot be reached through the ordinary map either.
    refused(service(
        &admin,
        vec![Binding {
            fingerprint: FP_B.into(),
            role: Role::Database {
                name: "agent_exec".into(),
            },
        }],
        vec![],
    ));
}

#[test]
fn connect_refused_unknown_fp_unlisted_task_revoked_wrong_executor() {
    let admin = admin();
    let later = now_ms() + DAY_MS;
    create_task(&admin, "inc-436", "agent_exec", later);
    create_task(&admin, "inc-437", "agent_exec", later);
    create_task(&admin, "inc-revoked", "agent_exec", later);
    revoke(&admin, "inc-revoked");
    let svc = service(
        &admin,
        vec![],
        vec![
            agent(FP_A, "agent_exec", &["inc-436", "inc-revoked", "inc-missing"]),
            agent(FP_B, "other_exec", &["inc-436"]),
        ],
    )
    .unwrap();
    svc.agent_session(FP_A, Some("inc-436")).unwrap();
    refused(svc.agent_session(FP_C, Some("inc-436")));
    refused(svc.agent_session(FP_A, Some("inc-437")));
    refused(svc.agent_session(FP_A, Some("not a name")));
    refused(svc.agent_session(FP_A, Some("inc-revoked")));
    refused(svc.agent_session(FP_A, Some("inc-missing")));
    refused(svc.agent_session(FP_B, Some("inc-436")));
    refused(svc.agent_session(FP_B, None));
    // The agent map never yields an ordinary session.
    refused(svc.session(FP_A));
}

#[test]
fn single_task_binding_needs_no_selection_and_multi_requires_it() {
    let admin = admin();
    let later = now_ms() + DAY_MS;
    create_task(&admin, "inc-436", "agent_exec", later);
    create_task(&admin, "inc-437", "agent_exec", later);
    let svc = service(
        &admin,
        vec![],
        vec![
            agent(FP_A, "agent_exec", &["inc-436"]),
            agent(FP_B, "agent_exec", &["inc-436", "inc-437"]),
        ],
    )
    .unwrap();
    let single = svc.agent_session(FP_A, None).unwrap();
    assert_eq!(single.task().unwrap().name.as_str(), "inc-436");
    refused(svc.agent_session(FP_B, None));
    let chosen = svc.agent_session(FP_B, Some("inc-437")).unwrap();
    assert_eq!(chosen.task().unwrap().name.as_str(), "inc-437");
    assert_eq!(chosen.task().unwrap().bound_def_version, 1);
    let mut exposed = svc.agent_bindings_for(&TaskName::new("inc-436").unwrap());
    exposed.sort();
    assert_eq!(
        exposed,
        vec![
            ("aaaaaaaa".to_string(), "agent_exec".to_string()),
            ("bbbbbbbb".to_string(), "agent_exec".to_string())
        ]
    );
}

/// Sleeps until the wall clock is past `ms`.
fn sleep_past(ms: i64) {
    let wait = ms + 50 - now_ms();
    if wait > 0 {
        std::thread::sleep(Duration::from_millis(wait as u64));
    }
}

/// A task that expires about two seconds from now, on whole-second boundaries
/// so the EXPIRES literal is exact.
fn short_task(admin: &Admin, name: &str) -> i64 {
    let expires = (now_ms() / 1000 + 3) * 1000;
    create_task(admin, name, "agent_exec", expires);
    expires
}

#[test]
fn expired_task_connects_until_retention_then_refused() {
    let admin = admin();
    let expires = short_task(&admin, "inc-short");
    let svc = service(&admin, vec![], vec![agent(FP_A, "agent_exec", &["inc-short"])]).unwrap();
    svc.set_outcome_retention(Duration::from_millis(1500));
    let mut live = svc.agent_session(FP_A, None).unwrap();
    live.admit(StatementClass::Show).unwrap();
    sleep_past(expires);
    // Expired but inside outcome retention: connects, and only lookups pass.
    let mut expired = svc.agent_session(FP_A, None).unwrap();
    expired.admit(StatementClass::Show).unwrap();
    live.admit(StatementClass::Set).unwrap();
    sleep_past(expires + 1500);
    refused(svc.agent_session(FP_A, None));
}

#[test]
fn reload_changed_entry_cuts_session_and_unchanged_entry_survives() {
    let admin = admin();
    let later = now_ms() + DAY_MS;
    create_task(&admin, "inc-436", "agent_exec", later);
    create_task(&admin, "inc-437", "agent_exec", later);
    let a = agent(FP_A, "agent_exec", &["inc-436"]);
    let b = agent(FP_B, "agent_exec", &["inc-436"]);
    let svc = service(&admin, vec![], vec![a.clone(), b.clone()]).unwrap();
    let mut sa = svc.agent_session(FP_A, None).unwrap();
    let mut sb = svc.agent_session(FP_B, None).unwrap();
    // Reloading the ordinary map alone keeps every agent entry.
    svc.reload_bindings(vec![]).unwrap();
    assert_eq!(ping(&mut sa), "ok");
    assert_eq!(ping(&mut sb), "ok");
    // B gains a task: a changed entry gets a new epoch and its session is cut.
    let b2 = agent(FP_B, "agent_exec", &["inc-436", "inc-437"]);
    svc.reload_all_bindings(vec![], vec![a.clone(), b2.clone()]).unwrap();
    assert_eq!(ping(&mut sa), "ok");
    assert_eq!(ping(&mut sb), "28000");
    let mut sb2 = svc.agent_session(FP_B, Some("inc-436")).unwrap();
    assert_eq!(ping(&mut sb2), "ok");
    // Changing back is still a change.
    svc.reload_all_bindings(vec![], vec![a, b]).unwrap();
    assert_eq!(ping(&mut sb2), "28000");
    // Removing A cuts A.
    svc.reload_all_bindings(vec![], vec![]).unwrap();
    assert_eq!(ping(&mut sa), "28000");
    refused(svc.agent_session(FP_A, None));
}

#[test]
fn alter_role_cuts_agent_session() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec", now_ms() + DAY_MS);
    let svc = service(&admin, vec![], vec![agent(FP_A, "agent_exec", &["inc-436"])]).unwrap();
    let mut session = svc.agent_session(FP_A, None).unwrap();
    assert_eq!(ping(&mut session), "ok");
    admin
        .roles()
        .alter(
            "agent_exec",
            RoleChanges {
                password: Some(None),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    assert_eq!(ping(&mut session), "28000");
    let mut fresh = svc.agent_session(FP_A, None).unwrap();
    assert_eq!(ping(&mut fresh), "ok");
    // Losing LOGIN cuts the session and refuses the next connect.
    admin
        .roles()
        .alter(
            "agent_exec",
            RoleChanges {
                login: Some(false),
                ..RoleChanges::default()
            },
        )
        .unwrap();
    assert_eq!(ping(&mut fresh), "28000");
    refused(svc.agent_session(FP_A, None));
}

#[test]
#[allow(non_snake_case)]
fn revoke_gives_28T01_and_definition_change_gives_42501() {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec", now_ms() + DAY_MS);
    let svc = service(&admin, vec![], vec![agent(FP_A, "agent_exec", &["inc-436"])]).unwrap();
    let mut session = svc.agent_session(FP_A, None).unwrap();
    session.admit(StatementClass::Show).unwrap();
    revoke(&admin, "inc-436");
    for class in [StatementClass::Show, StatementClass::Set] {
        let error = session.admit(class).unwrap_err();
        assert_eq!(error, CoreError::Task(TaskError::Revoked));
        assert_eq!(sqlstate(&error), "28T01");
    }
    refused(svc.agent_session(FP_A, None));
    // The catalog has no definition change short of REVOKE (which bumps
    // def_version but reads as revoked), so the 42501 half is the mapping;
    // the session path is the lib test `agent::tests::definition_change_gives_42501`.
    assert_eq!(
        sqlstate(&CoreError::Task(TaskError::Denied(DenyRule::DefinitionChanged))),
        "42501"
    );
}

#[test]
fn expired_allows_only_show_set_outcome() {
    let admin = admin();
    let expires = short_task(&admin, "inc-short");
    let svc = service(&admin, vec![], vec![agent(FP_A, "agent_exec", &["inc-short"])]).unwrap();
    let mut session = svc.agent_session(FP_A, None).unwrap();
    sleep_past(expires);
    for class in StatementClass::ALL {
        let got = session.admit(class);
        match class {
            // Enabled, and standing lets them through after expiry.
            StatementClass::Show | StatementClass::Set | StatementClass::OutcomeLookup => {
                got.unwrap()
            }
            // Enabled, but refused as expired.
            StatementClass::Update => {
                assert_eq!(got, Err(CoreError::Task(TaskError::Expired)))
            }
            _ => assert!(
                matches!(
                    got,
                    Err(CoreError::Task(
                        TaskError::Expired | TaskError::NotAllowedInTaskSession(_)
                    ))
                ),
                "{class:?}: {got:?}"
            ),
        }
    }
}
