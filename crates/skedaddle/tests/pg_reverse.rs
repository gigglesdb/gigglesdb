//! Real-PG qualification is opt-in and never starts Docker. Set both URLs to
//! disposable PG16+ databases with superuser access and wal_level=logical.
mod support;
use std::{
    cell::{Cell, RefCell},
    io::Write,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use skedaddle::pg::fence::Connection;
use skedaddle::{
    mapping::{Classification, Mapping, MappingError, PgType, Value},
    pg::{
        ddl, fence,
        pgoutput::{Decoder, Message},
        reverse::{
            Config, Eligibility, Endpoint, Error, Intent, InverseMapping, MappingFailure, Violation,
        },
        source::{self, ColumnMapping, RelationMapper},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::Runtime,
};
use tokio_postgres::{Client, SimpleQueryMessage};

fn config() -> Config {
    Config::new("p11a", 160000, vec![("public".into(), "rows".into())]).unwrap()
}

struct OnlyNull(Rc<Cell<usize>>);
impl Mapping for OnlyNull {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        Ok(value.clone())
    }
    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.forward(value)
    }
    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        self.0.set(self.0.get() + 1);
        if *value == Value::Null {
            Ok(())
        } else {
            Err(MappingError::OutOfDomain("test domain".into()))
        }
    }
    fn is_reversible(&self) -> bool {
        true
    }
    fn classification(&self) -> Classification {
        Classification::Supported
    }
}

fn non_null() -> Value {
    Value::Bool(true)
}

#[test]
fn every_value_checks_inverse_domain_and_first_violation_is_permanent() {
    let recorded = Rc::new(RefCell::new(Vec::new()));
    let journal = recorded.clone();
    let eligibility = Eligibility::new(move |reason| {
        journal.borrow_mut().push(reason.clone());
        Ok(())
    });
    let checks = Rc::new(Cell::new(0));
    let mapping = InverseMapping::new(Box::new(OnlyNull(checks.clone())), eligibility.clone());
    assert_eq!(mapping.forward(&Value::Null).unwrap(), Value::Null);
    assert!(mapping.forward(&non_null()).is_err());
    assert!(!eligibility.eligible());
    assert!(mapping.forward(&Value::Null).is_err());
    eligibility
        .end(Violation::UnsupportedDdl {
            reason: "later".into(),
        })
        .unwrap();
    assert_eq!(checks.get(), 3); // domain check + inverse's own guard, then rejected value
    assert_eq!(
        *recorded.borrow(),
        vec![Violation::Value {
            reason: MappingFailure::OutOfDomain
        }]
    );
    let restored = Eligibility::restore(eligibility.first_violation(), |_| {
        panic!("already journaled")
    });
    assert!(restored.ensure().is_err());
}

#[test]
fn journal_failure_closes_latch_and_retries_exact_first_reason() {
    let calls = Rc::new(Cell::new(0));
    let seen = calls.clone();
    let e = Eligibility::new(move |_| {
        seen.set(seen.get() + 1);
        if seen.get() == 1 {
            Err("fault".into())
        } else {
            Ok(())
        }
    });
    let first = Violation::Extension {
        name: "extension".into(),
    };
    assert!(e.end(first.clone()).is_err());
    assert!(!e.eligible());
    e.end(Violation::IrreversibleMapping).unwrap();
    assert_eq!(e.first_violation(), Some(first));
    assert_eq!(calls.get(), 2);
}

fn ddl_record(tag: &str) -> ddl::DdlRecord {
    ddl::DdlRecord {
        origin: None,
        format: 1,
        installation: "p11a".into(),
        server_major: 16,
        version: 1,
        tag: tag.into(),
        xid: "1".into(),
        query: tag.into(),
        commands: vec![],
    }
}

#[test]
fn unsupported_ddl_new_types_and_extensions_end_eligibility_without_rows() {
    for tag in [
        "DROP TABLE",
        "CREATE TYPE",
        "CREATE EXTENSION",
        "ALTER TABLE",
    ] {
        let e = Eligibility::new(|_| Ok(()));
        let mut schemas = ddl::SchemaVersions::new("p11a".into(), 0, []).unwrap();
        assert!(e.check_ddl(&mut schemas, &[ddl_record(tag)]).is_err());
        assert_eq!(e.first_violation(), Some(Violation::DestinationDdl));
        assert!(e.check_ddl(&mut schemas, &[]).is_err());
    }
}

struct NoDatabase;
impl fence::Connection for NoDatabase {
    fn query(&mut self, _: &str, _: Duration) -> Result<fence::Rows, String> {
        panic!("fault must precede database execution")
    }
}

#[test]
fn setup_rejects_unfenced_destination_and_journal_fault_before_mutation() {
    assert_eq!(
        config()
            .setup(
                &mut NoDatabase,
                &mut NoDatabase,
                None,
                Duration::from_secs(1),
                || Ok(false),
                |_, _| panic!()
            )
            .unwrap_err(),
        Error::NotFenced
    );
    assert!(matches!(
        config().setup(
            &mut NoDatabase,
            &mut NoDatabase,
            None,
            Duration::from_secs(1),
            || Ok(true),
            |endpoint, intent| {
                assert_eq!(endpoint, Endpoint::Destination);
                assert_eq!(*intent, Intent::Publication);
                Err(Error::Journal("injected failure".into()))
            }
        ),
        Err(Error::Journal(_))
    ));
}

#[test]
fn configuration_rejects_internal_tables_and_unsafe_ids() {
    assert!(Config::new("bad'", 160000, vec![("public".into(), "t".into())]).is_err());
    assert!(
        Config::new(
            "ok",
            160000,
            vec![("_skedaddle".into(), "apply_progress".into())]
        )
        .is_err()
    );
    assert!(Config::new("ok", 150000, vec![("public".into(), "t".into())]).is_err());
    assert_eq!(config().apply_context(3).origin, "skedaddle_p11a_rev");
    assert_eq!(config().apply_context(3).stream, "p11a_rev");
}

#[test]
fn journaled_readiness_requires_strict_destination_lsn_order_and_matching_installation() {
    let ready: skedaddle::pg::reverse::Readiness = serde_json::from_value(serde_json::json!({
        "installation":"p11a", "slot":{"slot":"skedaddle_p11a_rev","consistent_point":32}
    }))
    .unwrap();
    for lsn in [0, 16, 32] {
        assert!(ready.check_activation(&config(), lsn).is_err());
    }
    ready.check_activation(&config(), 33).unwrap();
    let other = Config::new("other", 160000, vec![("public".into(), "rows".into())]).unwrap();
    assert!(ready.check_activation(&other, 64).is_err());
}

#[test]
fn ddl_batch_retains_the_first_violation_in_statement_order() {
    let e = Eligibility::new(|_| Ok(()));
    let mut schemas = ddl::SchemaVersions::new("p11a".into(), 0, []).unwrap();
    let first = ddl_record("DROP TABLE");
    let mut later = ddl_record("CREATE TYPE");
    later.version = 2;
    assert!(e.check_ddl(&mut schemas, &[first, later]).is_err());
    assert!(matches!(
        e.first_violation(),
        Some(Violation::DestinationDdl)
    ));
}

#[tokio::test]
async fn duplex_both_capture_commands_filter_origins_and_decode_origin_frames() {
    // A byte transport test, not a simulated database. Both production command
    // builders must request PG's filter; Origin frames remain decodable for
    // the separately configured all-origin fence audit stream.
    let reverse = config().start_command(16).unwrap();
    let forward =
        skedaddle::pg::replication::start_command("forward", 16, &["forward".into()]).unwrap();
    let (mut tx, mut rx) = tokio::io::duplex(4096);
    for command in [forward, reverse] {
        tx.write_all(&(command.len() as u32).to_be_bytes())
            .await
            .unwrap();
        tx.write_all(command.as_bytes()).await.unwrap();
        let len = rx.read_u32().await.unwrap();
        let mut received = vec![0; len as usize];
        rx.read_exact(&mut received).await.unwrap();
        assert!(
            String::from_utf8(received)
                .unwrap()
                .contains("origin 'none'")
        );
    }
    for origin in ["skedaddle_p11a", "skedaddle_p11a_rev"] {
        let mut frame = vec![b'O'];
        frame.extend_from_slice(&16u64.to_be_bytes());
        frame.extend_from_slice(origin.as_bytes());
        frame.push(0);
        tx.write_all(&frame).await.unwrap();
        let mut received = vec![0; frame.len()];
        rx.read_exact(&mut received).await.unwrap();
        assert!(
            matches!(Decoder::default().decode(&received).unwrap(), Message::Origin { name, .. } if name == origin)
        );
    }
}

fn boolean_relation() -> Vec<u8> {
    let mut frame = vec![b'R'];
    frame.extend_from_slice(&7u32.to_be_bytes());
    frame.extend_from_slice(b"public\0rows\0d");
    frame.extend_from_slice(&1u16.to_be_bytes());
    frame.extend_from_slice(b"\x01value\0");
    frame.extend_from_slice(&16u32.to_be_bytes());
    frame.extend_from_slice(&(-1i32).to_be_bytes());
    frame
}
fn boolean_insert() -> Vec<u8> {
    let mut frame = vec![b'I'];
    frame.extend_from_slice(&7u32.to_be_bytes());
    frame.push(b'N');
    frame.extend_from_slice(&1u16.to_be_bytes());
    frame.push(b't');
    frame.extend_from_slice(&1u32.to_be_bytes());
    frame.push(b't');
    frame
}

#[tokio::test]
async fn duplex_reverse_value_passes_through_existing_mapper_and_inverse_domain() {
    let e = Eligibility::new(|_| Ok(()));
    let resolver = |_: &skedaddle::pg::pgoutput::Relation<'_>,
                    _: &skedaddle::pg::pgoutput::RelationColumn<'_>|
     -> source::Result<ColumnMapping> {
        Ok(ColumnMapping {
            pg_type: PgType::Bool,
            mapping: Box::new(OnlyNull(Rc::new(Cell::new(0)))),
        })
    };
    let mut mapper = RelationMapper::new(
        skedaddle::pg::reverse::ReverseResolver::new(resolver, e.clone()),
        0,
    );
    let (mut tx, mut rx) = tokio::io::duplex(4096);
    let decoder = Decoder::default();
    for bytes in [boolean_relation(), boolean_insert()] {
        tx.write_all(&bytes).await.unwrap();
        let mut received = vec![0; bytes.len()];
        rx.read_exact(&mut received).await.unwrap();
        let message = decoder.decode(&received).unwrap();
        if let Message::Relation(relation) = message {
            mapper.relation(&relation).unwrap();
        } else {
            assert!(mapper.map(&message).is_err());
        }
    }
    assert_eq!(
        e.first_violation(),
        Some(Violation::Value {
            reason: MappingFailure::OutOfDomain
        })
    );
    assert!(e.ensure().is_err());
}

#[test]
fn new_unqualified_relation_type_ends_eligibility_before_any_row() {
    let e = Eligibility::new(|_| Ok(()));
    let resolver = |_: &skedaddle::pg::pgoutput::Relation<'_>,
                    _: &skedaddle::pg::pgoutput::RelationColumn<'_>|
     -> source::Result<ColumnMapping> {
        Err(source::Error::Mapping(MappingError::Unsupported(
            "new destination type".into(),
        )))
    };
    let mut mapper = RelationMapper::new(
        skedaddle::pg::reverse::ReverseResolver::new(resolver, e.clone()),
        0,
    );
    let bytes = boolean_relation();
    let Message::Relation(relation) = Decoder::default().decode(&bytes).unwrap() else {
        panic!()
    };
    assert!(mapper.relation(&relation).is_err());
    assert!(matches!(
        e.first_violation(),
        Some(Violation::NewType { .. })
    ));
}

static REAL_PG: Mutex<()> = Mutex::new(());
struct Pg {
    runtime: Arc<Runtime>,
    client: Client,
}
impl Pg {
    fn connect(runtime: Arc<Runtime>, url: &str) -> Self {
        let client = runtime.block_on(async {
            let (client, connection) = tls_connect(url).await.expect("real PG connect failed");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        });
        Self { runtime, client }
    }
    fn sql(&mut self, sql: &str) -> fence::Rows {
        self.query(sql, Duration::from_secs(30))
            .expect("real PG SQL failed")
    }
}
impl fence::Connection for Pg {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<fence::Rows, String> {
        self.runtime.block_on(async {
            let rows = match tokio::time::timeout(timeout, self.client.simple_query(sql)).await {
                Ok(Ok(rows)) => rows,
                result => {
                    let _ = self.client.simple_query("ROLLBACK").await;
                    return Err(match result {
                        Err(_) => "timeout".into(),
                        Ok(Err(e)) => e.to_string(),
                        Ok(Ok(_)) => unreachable!(),
                    });
                }
            };
            Ok(rows
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(row) => Some(
                        (0..row.len())
                            .map(|i| row.get(i).unwrap_or("").to_owned())
                            .collect(),
                    ),
                    _ => None,
                })
                .collect())
        })
    }
}

struct Fixture {
    source: Pg,
    destination: Pg,
    config: Config,
    id: String,
}
impl Fixture {
    fn gated(test: &str) -> Option<Self> {
        let (Ok(source), Ok(destination)) = (
            std::env::var("SKEDADDLE_PG_REVERSE_SOURCE_URL"),
            std::env::var("SKEDADDLE_PG_REVERSE_DESTINATION_URL"),
        ) else {
            let _ = writeln!(
                std::io::stderr(),
                "SKIP {test}: set SKEDADDLE_PG_REVERSE_SOURCE_URL and SKEDADDLE_PG_REVERSE_DESTINATION_URL to disposable PG16+ superuser databases (wal_level=logical); Docker is not started"
            );
            return None;
        };
        let runtime = Arc::new(Runtime::new().unwrap());
        let mut source = Pg::connect(runtime.clone(), &source);
        let mut destination = Pg::connect(runtime, &destination);
        let id = format!(
            "p11a_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        );
        for pg in [&mut source, &mut destination] {
            pg.sql(&format!(
                "CREATE SCHEMA {id}; CREATE TABLE {id}.rows (id int PRIMARY KEY, value boolean)"
            ));
        }
        let version = destination.sql("SHOW server_version_num")[0][0]
            .parse()
            .unwrap();
        let config = Config::new(&id, version, vec![(id.clone(), "rows".into())]).unwrap();
        let mut fixture = Self {
            source,
            destination,
            config,
            id,
        };
        fixture.destination.sql(&format!("CREATE ROLE {}_app NOLOGIN; GRANT USAGE ON SCHEMA {} TO {}_app; GRANT INSERT ON {}.rows TO {}_app", fixture.id, fixture.id, fixture.id, fixture.id, fixture.id));
        Some(fixture)
    }
    fn setup(&mut self) -> skedaddle::pg::reverse::Readiness {
        // This fixture exclusively owns the role; no test can release its
        // NOLOGIN fence until readiness has been recorded and checked.
        let fenced = self.destination.sql(&format!(
            "SELECT (NOT rolcanlogin)::text FROM pg_roles WHERE rolname='{}_app'",
            self.id
        ))[0][0]
            == "true";
        self.config
            .setup(
                &mut self.destination,
                &mut self.source,
                None,
                Duration::from_secs(30),
                || Ok(fenced),
                |_, _| Ok(()),
            )
            .unwrap()
    }
    fn changes(&mut self, origin: &str) -> Vec<Vec<u8>> {
        let sql = format!(
            "SELECT data FROM pg_logical_slot_get_binary_changes('{}', NULL, NULL, 'proto_version','1','publication_names','{}','messages','true','origin','{origin}')",
            self.config.slot(),
            self.config.publication()
        );
        self.destination.runtime.block_on(async {
            self.destination
                .client
                .query(&sql, &[])
                .await
                .unwrap()
                .iter()
                .map(|r| r.get(0))
                .collect()
        })
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Only this fixture's named objects. Errors are loud, never broad cleanup.
        let version = self.destination.sql("SHOW server_version_num")[0][0]
            .parse()
            .unwrap();
        let trigger = ddl::TriggerSql::new(&self.id, version).unwrap();
        let dest = format!(
            "{} SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='{}'; DROP PUBLICATION IF EXISTS {}; DROP SCHEMA IF EXISTS {} CASCADE; DROP ROLE IF EXISTS {}_app;",
            trigger.remove,
            self.config.slot(),
            self.config.publication(),
            self.id,
            self.id
        );
        if let Err(e) = self.destination.query(&dest, Duration::from_secs(30)) {
            eprintln!("CLEANUP FAILED destination {}: {e}", self.id);
        }
        let src = format!(
            "SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname='{}'; DROP SCHEMA IF EXISTS {} CASCADE;",
            self.config.origin(),
            self.id
        );
        if let Err(e) = self.source.query(&src, Duration::from_secs(30)) {
            eprintln!("CLEANUP FAILED source {}: {e}", self.id);
        }
    }
}

fn coordinated_owner(
    fixture: &mut Fixture,
) -> (
    skedaddle::coordinator::epoch::Coordinator,
    skedaddle::coordinator::journal::Journal,
    skedaddle::pg::activate::DestinationFence,
) {
    use skedaddle::{coordinator::*, pg::activate};
    let journal = support::journal(
        support::fs_store(support::scratch("p11a-objects")),
        &fixture.id,
    );
    let runtime = fixture.destination.runtime.clone();
    let mut owner = support::coordinator(journal.clone(), "p11a-owner");
    runtime.block_on(async {
        owner.acquire().await.unwrap();
        for to in [State::Assessed, State::Copying, State::CatchingUp] {
            owner
                .commit(Transition::Lifecycle {
                    event: Event::Advance { to },
                })
                .await
                .unwrap();
        }
        owner
            .commit(Transition::Lifecycle {
                event: Event::Readiness {
                    facts: support::ready(),
                },
            })
            .await
            .unwrap();
        for to in [State::SourceFenced, State::FinalBoundaryApplied] {
            owner
                .commit(Transition::Lifecycle {
                    event: Event::Advance { to },
                })
                .await
                .unwrap();
        }
    });
    let table_owner = fixture.destination.sql("SELECT current_user")[0][0].clone();
    let mut destination_fence = activate::DestinationFence::new(activate::Config {
        epoch: 1,
        applications: std::collections::BTreeSet::from([format!("{}_app", fixture.id)]),
        tables: vec![fence::Table {
            schema: fixture.id.clone(),
            name: "rows".into(),
            owner: table_owner,
        }],
        timeout_ms: 30_000,
        post_activate: None,
    })
    .unwrap();
    let path = support::scratch("p11a-native").join("fence.jsonl");
    let mut log = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    while destination_fence.next != activate::Step::Fenced {
        destination_fence
            .execute_next(&mut fixture.destination, &mut |intent| {
                serde_json::to_writer(&mut log, intent).map_err(|e| e.to_string())?;
                writeln!(log).map_err(|e| e.to_string())?;
                log.sync_data().map_err(|e| e.to_string())
            })
            .unwrap();
    }
    (owner, journal, destination_fence)
}

#[test]
fn coordinated_setup_journals_proof_and_restart_authenticates_it() {
    let _guard = REAL_PG.lock().unwrap();
    let Some(mut fixture) =
        Fixture::gated("coordinated_setup_journals_proof_and_restart_authenticates_it")
    else {
        return;
    };
    let (mut owner, journal, mut destination_fence) = coordinated_owner(&mut fixture);
    let runtime = fixture.destination.runtime.clone();
    owner
        .setup_reverse(
            &runtime,
            &fixture.config,
            &mut fixture.destination,
            &mut fixture.source,
            &mut destination_fence,
        )
        .unwrap();
    runtime.block_on(owner.checkpoint()).unwrap();
    let token = runtime.block_on(journal.authenticated_reverse()).unwrap();
    let state = runtime.block_on(journal.load(true)).unwrap();
    assert!(state.verification.reverse.as_ref().unwrap().proof.is_some());
    let restored = runtime
        .block_on(skedaddle::coordinator::local::Local::recover(
            support::scratch("p11a-restart").join("db"),
            &journal,
        ))
        .unwrap();
    assert_eq!(restored.state(), &state);
    let key = skedaddle::cutover::Key {
        migration: fixture.id.clone(),
        cutover: "restart".into(),
        epoch: 1,
        coordinator: owner.instance_id().into(),
    };
    destination_fence.next = skedaddle::pg::activate::Step::Restore;
    let (identity, barrier) = skedaddle::coordinator::reverse::destination_barrier(
        &mut fixture.destination,
        Duration::from_secs(30),
    )
    .unwrap();
    token
        .check(&key, &destination_fence, &identity, barrier)
        .unwrap();
    // Repeating setup adopts the same logical slot and publishes a new proof.
    destination_fence.next = skedaddle::pg::activate::Step::Fenced;
    let slot = state.verification.reverse.as_ref().unwrap().slot.clone();
    owner
        .setup_reverse(
            &runtime,
            &fixture.config,
            &mut fixture.destination,
            &mut fixture.source,
            &mut destination_fence,
        )
        .unwrap();
    assert_eq!(
        owner.state().verification.reverse.as_ref().unwrap().slot,
        slot
    );
}

#[cfg(feature = "test-crash")]
#[test]
fn coordinated_setup_crash_before_proof_refuses_and_retry_is_idempotent() {
    use skedaddle::coordinator::{self, epoch::Coordinator, journal::Journal, local::Local};
    let _guard = REAL_PG.lock().unwrap();
    if let Some(root) = std::env::var_os("SKEDADDLE_REVERSE_CRASH_ROOT") {
        let root = std::path::PathBuf::from(root);
        let (id, mut destination_fence): (String, skedaddle::pg::activate::DestinationFence) =
            serde_json::from_slice(&std::fs::read(root.join("fence.json")).unwrap()).unwrap();
        let runtime = Arc::new(Runtime::new().unwrap());
        let mut source = Pg::connect(
            runtime.clone(),
            &std::env::var("SKEDADDLE_PG_REVERSE_SOURCE_URL").unwrap(),
        );
        let mut destination = Pg::connect(
            runtime.clone(),
            &std::env::var("SKEDADDLE_PG_REVERSE_DESTINATION_URL").unwrap(),
        );
        let journal = Journal::new(
            support::fs_store(root.join("objects")),
            "p2",
            &id,
            Duration::from_secs(3),
        )
        .unwrap();
        let local = runtime
            .block_on(Local::recover(root.join("child-db"), &journal))
            .unwrap();
        let mut owner = Coordinator::observer(journal, local, "crash-child").unwrap();
        let epoch = runtime.block_on(owner.acquire()).unwrap();
        destination_fence.config.epoch = epoch as i64;
        owner.set_crash_seam(coordinator::CrashSeam::AfterReverseSetup);
        let version = destination.sql("SHOW server_version_num")[0][0]
            .parse()
            .unwrap();
        let config = Config::new(&id, version, vec![(id.clone(), "rows".into())]).unwrap();
        owner
            .setup_reverse(
                &runtime,
                &config,
                &mut destination,
                &mut source,
                &mut destination_fence,
            )
            .unwrap();
        panic!("crash seam did not terminate child");
    }
    let Some(mut fixture) =
        Fixture::gated("coordinated_setup_crash_before_proof_refuses_and_retry_is_idempotent")
    else {
        return;
    };
    let (owner, original, destination_fence) = coordinated_owner(&mut fixture);
    let runtime = fixture.destination.runtime.clone();
    let root = support::scratch("p11a-crash");
    std::fs::create_dir(root.join("objects")).unwrap();
    let journal = Journal::new(
        support::fs_store(root.join("objects")),
        "p2",
        &fixture.id,
        Duration::from_secs(3),
    )
    .unwrap();
    runtime
        .block_on(original.copy_entries_for_rebuild(&journal))
        .unwrap();
    std::fs::write(
        root.join("fence.json"),
        serde_json::to_vec(&(&fixture.id, &destination_fence)).unwrap(),
    )
    .unwrap();
    drop(owner);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "coordinated_setup_crash_before_proof_refuses_and_retry_is_idempotent",
            "--nocapture",
        ])
        .env("SKEDADDLE_REVERSE_CRASH_ROOT", &root)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(matches!(
        runtime.block_on(journal.authenticated_reverse()),
        Err(coordinator::Error::ReverseProof(
            coordinator::reverse::ProofError::Missing
        ))
    ));
    let state = runtime.block_on(journal.load(false)).unwrap();
    let reverse = state.verification.reverse.as_ref().unwrap();
    assert!(reverse.readiness.is_some());
    assert!(reverse.proof.is_none());
    let slot = reverse.slot.clone();
    let mut destination_fence = (*reverse.binding.as_ref().unwrap().fence).clone();
    let local = runtime
        .block_on(Local::recover(root.join("retry-db"), &journal))
        .unwrap();
    let mut owner = Coordinator::observer(journal.clone(), local, "retry-owner").unwrap();
    destination_fence.config.epoch = runtime.block_on(owner.acquire()).unwrap() as i64;
    owner
        .setup_reverse(
            &runtime,
            &fixture.config,
            &mut fixture.destination,
            &mut fixture.source,
            &mut destination_fence,
        )
        .unwrap();
    assert_eq!(
        owner.state().verification.reverse.as_ref().unwrap().slot,
        slot
    );
    runtime.block_on(journal.authenticated_reverse()).unwrap();
}

#[test]
fn reverse_slot_covers_first_destination_write() {
    let _guard = REAL_PG.lock().unwrap();
    let Some(mut f) = Fixture::gated("reverse_slot_covers_first_destination_write") else {
        return;
    };
    let ready = f.setup();
    assert!(
        ready
            .check_activation(&f.config, ready.slot().consistent_point)
            .is_err()
    );
    let resumed = f
        .config
        .setup(
            &mut f.destination,
            &mut f.source,
            Some(ready.slot()),
            Duration::from_secs(30),
            || Ok(true),
            |_, _| Ok(()),
        )
        .unwrap();
    assert_eq!(resumed, ready);
    // Journal faults at every boundary must leave a plan that can be replayed
    // against the real databases. No fake catalog state is used.
    for cut in 1..=6 {
        let mut calls = 0;
        assert!(
            f.config
                .setup(
                    &mut f.destination,
                    &mut f.source,
                    Some(ready.slot()),
                    Duration::from_secs(30),
                    || Ok(true),
                    |_, _| {
                        calls += 1;
                        if calls == cut {
                            Err(Error::Journal("injected boundary failure".into()))
                        } else {
                            Ok(())
                        }
                    }
                )
                .is_err()
        );
        assert_eq!(
            f.config
                .setup(
                    &mut f.destination,
                    &mut f.source,
                    Some(ready.slot()),
                    Duration::from_secs(30),
                    || Ok(true),
                    |_, _| Ok(())
                )
                .unwrap(),
            ready
        );
    }
    let barrier = fence::parse_lsn(
        &f.destination
            .sql("SELECT pg_current_wal_insert_lsn()::text")[0][0],
    )
    .unwrap();
    ready.check_activation(&f.config, barrier).unwrap();
    f.destination.sql(&format!(
        "ALTER ROLE {}_app LOGIN; SET ROLE {}_app; INSERT INTO {}.rows VALUES(1,true); RESET ROLE",
        f.id, f.id, f.id
    ));
    let changes = f.changes("none");
    assert!(changes.iter().any(|b| b.first() == Some(&b'I')));
    assert!(
        f.source
            .sql("SELECT to_regclass('_skedaddle.apply_receipts')::text")[0][0]
            .contains("apply_receipts")
    );
}

#[test]
fn no_replication_loop_either_direction() {
    let _guard = REAL_PG.lock().unwrap();
    let Some(mut f) = Fixture::gated("no_replication_loop_either_direction") else {
        return;
    };
    f.setup();
    // Qualify PG's origin filter on both endpoints using real WAL; the same
    // production option is asserted on both START_REPLICATION commands above.
    for reverse in [false, true] {
        let (pg, slot, origin) = if reverse {
            (
                &mut f.source,
                format!("{}_fwd_filter", f.id),
                f.config.origin(),
            )
        } else {
            (
                &mut f.destination,
                format!("{}_rev_filter", f.id),
                format!("skedaddle_{}", f.id),
            )
        };
        let publication = format!("{}_filter", f.id);
        pg.sql(&format!(
            "CREATE PUBLICATION {publication} FOR TABLE {}.rows",
            f.id
        ));
        // The filter publication must precede this probe slot's historic snapshot.
        pg.sql(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}','pgoutput')"
        ));
        if !reverse {
            pg.sql(&format!("SELECT pg_replication_origin_create('{origin}')"));
        }
        pg.sql(&format!("SELECT pg_replication_origin_session_setup('{origin}'); BEGIN; INSERT INTO {}.rows VALUES(1,true); COMMIT; SELECT pg_replication_origin_session_reset(); INSERT INTO {}.rows VALUES(2,false)", f.id, f.id));
        let rows = pg.sql(&format!("SELECT count(*)::text FROM pg_logical_slot_get_binary_changes('{slot}',NULL,NULL,'proto_version','1','publication_names','{publication}','origin','none') WHERE get_byte(data,0)=73"));
        assert_eq!(rows[0][0], "1");
        pg.sql(&format!("DROP PUBLICATION {publication}"));
        pg.sql(&format!("SELECT pg_drop_replication_slot('{slot}')"));
        if !reverse {
            pg.sql(&format!("SELECT pg_replication_origin_drop('{origin}')"));
        }
    }
}

#[test]
fn non_reversible_value_ends_eligibility() {
    let _guard = REAL_PG.lock().unwrap();
    let Some(mut f) = Fixture::gated("non_reversible_value_ends_eligibility") else {
        return;
    };
    f.setup();
    f.destination
        .sql(&format!("INSERT INTO {}.rows VALUES(1,true)", f.id));
    let changes = f.changes("none");
    let decoder = Decoder::default();
    let e = Eligibility::new(|_| Ok(()));
    let resolver = |_: &skedaddle::pg::pgoutput::Relation<'_>,
                    column: &skedaddle::pg::pgoutput::RelationColumn<'_>|
     -> source::Result<ColumnMapping> {
        // Preflight's integer key uses its identity mapping; the boolean
        // destination column deliberately has a narrower inverse domain.
        if column.name == "value" {
            Ok(ColumnMapping {
                pg_type: PgType::Bool,
                mapping: Box::new(OnlyNull(Rc::new(Cell::new(0)))),
            })
        } else {
            Ok(ColumnMapping::identity(PgType::Int4))
        }
    };
    let mut mapper = RelationMapper::new(
        skedaddle::pg::reverse::ReverseResolver::new(resolver, e.clone()),
        0,
    );
    for bytes in changes {
        let message = decoder.decode(&bytes).unwrap();
        if let Message::Relation(relation) = message {
            mapper.relation(&relation).unwrap();
        } else if matches!(message, Message::Insert { .. }) {
            assert!(mapper.map(&message).is_err());
        }
    }
    assert!(!e.eligible());
    assert!(e.ensure().is_err());
}

#[test]
fn unsupported_destination_ddl_ends_eligibility() {
    let _guard = REAL_PG.lock().unwrap();
    let Some(mut f) = Fixture::gated("unsupported_destination_ddl_ends_eligibility") else {
        return;
    };
    f.setup();
    f.destination.sql(&format!(
        "ALTER TABLE {}.rows ADD COLUMN unsupported bigint",
        f.id
    ));
    let changes = f.changes("none");
    let e = Eligibility::new(|_| Ok(()));
    let mut schemas = ddl::SchemaVersions::new(f.id.clone(), 0, []).unwrap();
    let decoder = Decoder::default();
    for bytes in changes {
        let message = decoder.decode(&bytes).unwrap();
        if let Message::LogicalMessage {
            prefix: ddl::MESSAGE_PREFIX,
            ..
        } = &message
        {
            let record = ddl::decode(&message, &f.id).unwrap();
            assert!(e.check_ddl(&mut schemas, &[record]).is_err());
        }
    }
    assert!(!e.eligible());
    assert!(e.ensure().is_err());
}

// The harness CA is explicit; missing trust is a failure once a DB gate is set.
fn tls_config(postgres: tokio_postgres::Config) -> skedaddle::pg::tls::Config {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("TLS harness config required");
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read TLS harness config"))
            .expect("TLS harness JSON");
    let ca_pem = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .expect("harness ca_pem or replication.ca_pem required");
    skedaddle::pg::tls::Config::new(postgres, ca_pem)
}

async fn tls_connect(
    dsn: &str,
) -> std::result::Result<
    (tokio_postgres::Client, skedaddle::pg::tls::Driver),
    skedaddle::pg::tls::Error,
> {
    let config = tls_config(dsn.parse().expect("harness PostgreSQL configuration"));
    let (client, driver, transport) =
        skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required).await?;
    assert!(transport.encrypted());
    assert!(transport.warnings.is_empty());
    Ok((client, driver))
}

#[test]
fn live_post_activation_health_is_read_only_and_detects_missing_publication() {
    let _guard = REAL_PG.lock().unwrap();
    let Some(mut fixture) = Fixture::gated("post_activation_health") else {
        return;
    };
    let ready = fixture.setup();
    let barrier = ready.slot().consistent_point + 1;
    let sql = fixture.config.health_sql(&ready, barrier).unwrap();
    assert_eq!(
        fixture
            .destination
            .sql(&format!("SELECT result::text FROM ({sql}) q(result)"))[0][0],
        "true"
    );
    fixture.destination.sql(&format!(
        "DROP PUBLICATION {}",
        fixture.config.publication()
    ));
    assert_eq!(
        fixture
            .destination
            .sql(&format!("SELECT result::text FROM ({sql}) q(result)"))[0][0],
        "false"
    );
}

#[test]
fn live_reverse_capture_buffers_and_resumes_the_durable_manifest() {
    use skedaddle::{
        buffer::{
            backends::ObjectBufferStore,
            reader::{BufferReader, ReaderConfig},
            writer::{BufferWriter, WriterConfig},
        },
        event::{EventError, PositionOrder, SourceIdentity},
        mapping::PgType,
    };
    let _guard = REAL_PG.lock().unwrap();
    let Some(mut f) = Fixture::gated("live_reverse_capture_buffers_and_resumes") else {
        return;
    };
    let ready = f.setup();
    // Activation restores access with a tagged administrative GRANT. It must
    // preserve eligibility and the classifier version before application DML.
    f.destination.sql(&format!("BEGIN; SET LOCAL skedaddle.admin_origin = '{}'; GRANT SELECT ON {}.rows TO {}_app; COMMIT;", f.id, f.id, f.id));
    f.destination
        .sql(&format!("INSERT INTO {}.rows VALUES(1,true)", f.id));
    let messages = f.changes("none");
    struct Order(String);
    impl PositionOrder for Order {
        fn domain(&self) -> &str {
            &self.0
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Result<std::cmp::Ordering, EventError> {
            Ok(a.cmp(b))
        }
    }
    struct Transport(std::collections::VecDeque<Vec<u8>>);
    impl source::Transport for Transport {
        async fn next(&mut self, _: u64) -> source::Result<Vec<u8>> {
            Ok(self.0.pop_front().expect("finite PG fixture stream"))
        }
    }
    let root =
        std::path::PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task test root"))
            .join(format!("reverse-live-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("store")).unwrap();
    let store =
        Arc::new(ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap());
    let config = WriterConfig {
        prefix: "reverse-runner-v1".into(),
        migration_id: f.id.clone(),
        stream: f.config.stream(),
        epoch: 1,
        segment_bytes: 1000000,
        segment_transactions: 100,
        segment_age: Duration::from_secs(1),
        max_transaction_bytes: 1000000,
    };
    let order = Arc::new(Order(f.config.stream()));
    let runtime = f.destination.runtime.clone();
    std::fs::create_dir_all(root.join("scratch")).unwrap();
    runtime.block_on(async {
        let (send, _) = std::sync::mpsc::channel();
        let writer = BufferWriter::open_from_checkpoint(
            store.clone(),
            config.clone(),
            Default::default(),
            order.clone(),
            send,
            root.join("scratch"),
        )
        .await
        .unwrap();
        let identity = SourceIdentity {
            engine: "postgres".into(),
            system_id: "live-fixture".into(),
            lineage: "1".into(),
            db_name: "destination".into(),
            stream: f.config.slot(),
        };
        let resolver = |_: &skedaddle::pg::pgoutput::Relation<'_>,
                        c: &skedaddle::pg::pgoutput::RelationColumn<'_>|
         -> source::Result<ColumnMapping> {
            Ok(ColumnMapping::identity(match c.type_oid {
                23 => PgType::Int4,
                16 => PgType::Bool,
                _ => panic!("unexpected PG fixture type"),
            }))
        };
        let eligibility = Eligibility::new(|_| Ok(()));
        let mut capture = skedaddle::pg::reverse::Capture::new(
            &f.config,
            identity,
            ready.slot().consistent_point,
            writer,
            resolver,
            ddl::SchemaVersions::new(f.id.clone(), 0, []).unwrap(),
            eligibility.clone(),
        )
        .unwrap();
        let mut transport = Transport(
            messages
                .into_iter()
                .map(|m| {
                    let mut bytes = vec![b'w'];
                    bytes.extend_from_slice(&[0; 24]);
                    bytes.extend(m);
                    bytes
                })
                .collect(),
        );
        let (_send, mut pause) = tokio::sync::watch::channel(false);
        let mut sink = |_: source::Watermark| Ok(());
        let mut durable = false;
        while !transport.0.is_empty() {
            durable |= matches!(
                capture
                    .step(&mut transport, &mut sink, &mut pause)
                    .await
                    .unwrap(),
                source::Step::Durable { .. }
            );
        }
        assert!(durable);
        assert!(eligibility.eligible());
        let saved = capture.writer().checkpoint().unwrap();
        drop(capture);
        let (send, _) = std::sync::mpsc::channel();
        let resumed = BufferWriter::open_from_checkpoint(
            store.clone(),
            config.clone(),
            Default::default(),
            order.clone(),
            send,
            root.join("scratch"),
        )
        .await
        .unwrap();
        assert_eq!(resumed.checkpoint().unwrap(), saved);
        let mut reader = BufferReader::new(
            store.clone(),
            ReaderConfig {
                prefix: config.prefix,
                migration_id: config.migration_id,
                stream: config.stream,
                limits: Default::default(),
                required_next_mseq: saved.next_mseq,
            },
            Default::default(),
            order,
        )
        .unwrap();
        let mut insert = false;
        while let Some(txn) = reader.next_txn().await.unwrap() {
            insert |= txn
                .events
                .iter()
                .any(|e| e.kind == skedaddle::event::EventKind::Insert);
        }
        assert!(insert);
    });
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
