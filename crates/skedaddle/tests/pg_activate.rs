//! Opt-in disposable real-PG tests; no Docker or server is started here.
//! Set SKEDADDLE_PG_ACTIVATE_DSN to a harness superuser DSN (verified TLS)
//! with CREATEDB permission. All fixture databases/roles are uniquely named.

use skedaddle::pg::{
    activate::{
        BlockerKind, Config, DestinationFence, Error, SequenceName, SequenceScope, Step,
        discover_sequences, inspect_validity, read_sequence_states, sync_sequences,
    },
    fence::{Connection, Grant, Grantee, Privilege, Rows, Table},
};
use std::{
    collections::BTreeSet,
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, SimpleQueryMessage};

static SERIAL: Mutex<()> = Mutex::new(());
const TIMEOUT: Duration = Duration::from_secs(15);

struct Pg {
    runtime: Arc<tokio::runtime::Runtime>,
    client: Client,
}

impl Pg {
    fn connect(runtime: Arc<tokio::runtime::Runtime>, config: &tokio_postgres::Config) -> Self {
        let (client, connection) = runtime
            .block_on(async {
                let (client, driver, transport) = skedaddle::pg::tls::connect(
                    &tls_config(config.clone()),
                    &skedaddle::security::TlsPolicy::Required,
                )
                .await?;
                assert!(transport.encrypted());
                Ok::<_, skedaddle::pg::tls::Error>((client, driver))
            })
            .expect("connect disposable harness PG");
        runtime.spawn(async move {
            let _ = connection.await;
        });
        Self { runtime, client }
    }
    fn sql(&mut self, sql: &str) -> Rows {
        self.query(sql, TIMEOUT).unwrap()
    }
}

impl Connection for Pg {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<Rows, String> {
        self.runtime.block_on(async {
            match tokio::time::timeout(timeout, self.client.simple_query(sql)).await {
                Ok(Ok(messages)) => Ok(messages
                    .into_iter()
                    .filter_map(|message| match message {
                        SimpleQueryMessage::Row(row) => Some(
                            (0..row.len())
                                .map(|i| row.get(i).unwrap_or("").to_owned())
                                .collect(),
                        ),
                        _ => None,
                    })
                    .collect()),
                Ok(Err(error)) => {
                    let _ = self.client.batch_execute("ROLLBACK").await;
                    Err(format!("{error:?}"))
                }
                Err(_) => {
                    let _ = self
                        .client
                        .cancel_token()
                        .cancel_query(
                            skedaddle::pg::tls::MakeRustlsConnect::from_ca_pem(
                                tls_config(tokio_postgres::Config::new()).ca_pem,
                            )
                            .expect("harness CA"),
                        )
                        .await;
                    let _ = self.client.batch_execute("ROLLBACK").await;
                    Err("real PG query timed out".into())
                }
            }
        })
    }
}

struct Fixture {
    admin: Pg,
    source: Pg,
    destination: Pg,
    source_db: String,
    destination_db: String,
    app: String,
    epoch: i64,
}

impl Fixture {
    fn new(test: &str) -> Option<Self> {
        let dsn = std::env::var("SKEDADDLE_PG_ACTIVATE_DSN").ok().or_else(|| {
            let path = std::env::var_os("SKEDADDLE_TEST_CONFIG")?;
            let config: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).expect("read PG harness config"))
                    .expect("PG harness JSON");
            config["env"]["SKEDADDLE_PG_ACTIVATE_DSN"]
                .as_str()
                .or_else(|| config["pg"].as_str())
                .map(str::to_owned)
        });
        let Some(dsn) = dsn else {
            // Write directly so the reason remains visible under cargo's capture.
            let _ = writeln!(
                std::io::stderr(),
                "LOUD SKIP {test}: SKEDADDLE_PG_ACTIVATE_DSN unset; requires a disposable harness PostgreSQL superuser with CREATEDB; no Docker started"
            );
            return None;
        };
        let root = std::env::var("SKEDADDLE_TEST_ROOT")
            .expect("set task-specific SKEDADDLE_TEST_ROOT under J:/gigglesdb-work");
        assert!(
            std::path::Path::new(&root).is_dir(),
            "test root must already exist"
        );
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let source_db = format!("p6c_s_{epoch}");
        let destination_db = format!("p6c_d_{epoch}");
        let app = format!("p6c_app_{epoch}");
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        );
        let config: tokio_postgres::Config = dsn.parse().expect("harness DSN");
        let mut admin = Pg::connect(runtime.clone(), &config);
        assert_eq!(
            admin.sql("SELECT rolsuper::text FROM pg_roles WHERE rolname=current_user")[0][0],
            "true"
        );
        admin.sql(&format!("CREATE DATABASE {source_db}"));
        admin.sql(&format!("CREATE DATABASE {destination_db}"));
        admin.sql(&format!("CREATE ROLE {app} NOLOGIN"));
        let mut source_config = config.clone();
        source_config.dbname(&source_db);
        let mut destination_config = config;
        destination_config.dbname(&destination_db);
        let mut source = Pg::connect(runtime.clone(), &source_config);
        let mut destination = Pg::connect(runtime, &destination_config);
        let schema = "CREATE SCHEMA p6c; CREATE TABLE p6c.items(id bigint GENERATED ALWAYS AS IDENTITY, value integer);";
        source.sql(schema);
        destination.sql(schema);
        destination.sql(&format!("GRANT USAGE ON SCHEMA p6c TO {app};"));
        Some(Self {
            admin,
            source,
            destination,
            source_db,
            destination_db,
            app,
            epoch,
        })
    }

    fn tables(&mut self) -> Vec<Table> {
        let owner = self.destination.sql("SELECT current_user")[0][0].clone();
        vec![Table {
            schema: "p6c".into(),
            name: "items".into(),
            owner,
        }]
    }

    fn fence(&mut self) -> DestinationFence {
        let config = Config {
            epoch: self.epoch,
            applications: BTreeSet::from([self.app.clone()]),
            tables: self.tables(),
            timeout_ms: 15000,
            post_activate: None,
        };
        let mut initial = DestinationFence::new(config.clone()).unwrap();
        initial
            .execute_next(&mut self.destination, &mut |_| Ok(()))
            .unwrap();
        let mut desired = initial.saved.unwrap();
        desired
            .catalog
            .roles
            .iter_mut()
            .find(|r| r.name == self.app)
            .unwrap()
            .login = true;
        for privilege in [
            Privilege::Insert,
            Privilege::Update,
            Privilege::Delete,
            Privilege::Truncate,
        ] {
            desired.catalog.grants.push(Grant {
                table: config.tables[0].clone(),
                column: None,
                grantee: Grantee::Role(self.app.clone()),
                grantor: config.tables[0].owner.clone(),
                privilege,
                grant_option: false,
            });
        }
        DestinationFence::with_activation_state(config, desired).unwrap()
    }

    /// Execute, then recover the pre-execution intent and replay each step.
    fn run_until(&mut self, fence: &mut DestinationFence, end: Step) {
        while fence.next != end {
            let before = serde_json::to_string(fence).unwrap();
            let mut intents = vec![];
            fence
                .execute_next(&mut self.destination, &mut |i| {
                    intents.push(i.clone());
                    Ok(())
                })
                .unwrap();
            let expected = fence.next;
            let mut replay: DestinationFence = serde_json::from_str(&before).unwrap();
            // Save's snapshot intent, rather than the earlier unsaved intent,
            // is the latest durable recovery point before any mutation.
            if replay.next == Step::Save {
                replay = intents.last().unwrap().state.clone();
            }
            replay
                .execute_next(&mut self.destination, &mut |_| Ok(()))
                .unwrap();
            assert_eq!(replay.next, expected);
            *fence = replay;
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for db in [&self.source_db, &self.destination_db] {
            let _ = self
                .admin
                .query(&format!("DROP DATABASE {db} WITH (FORCE)"), TIMEOUT);
        }
        let _ = self
            .admin
            .query(&format!("DROP ROLE {}", self.app), TIMEOUT);
    }
}

#[test]
fn destination_app_roles_cannot_write_before_activation() {
    let _lock = SERIAL.lock().unwrap();
    let Some(mut fixture) = Fixture::new("destination_app_roles_cannot_write_before_activation")
    else {
        return;
    };
    let mut fence = fixture.fence();
    fixture.run_until(&mut fence, Step::Fenced);
    assert!(!fence.evidence.is_empty());
    assert_eq!(
        fixture.destination.sql(&format!(
            "SELECT rolcanlogin::text FROM pg_roles WHERE rolname='{}'",
            fixture.app
        ))[0][0],
        "false"
    );
    fixture
        .destination
        .sql(&format!("SET ROLE {}", fixture.app));
    assert!(
        fixture
            .destination
            .query("INSERT INTO p6c.items(value) VALUES(1)", TIMEOUT)
            .is_err()
    );
    fixture.destination.sql("RESET ROLE");
    fence
        .observe(&mut fixture.destination, &mut |_| Ok(()))
        .unwrap();
    fence
        .begin_activation("final-marker-49".into(), BTreeSet::new(), &mut |_| Ok(()))
        .unwrap();
    fixture.run_until(&mut fence, Step::Complete);
    let before = fence.clone();
    fence
        .execute_next(&mut fixture.destination, &mut |_| {
            panic!("complete must be a no-op")
        })
        .unwrap();
    assert_eq!(fence, before);
    fixture.destination.sql(&format!(
        "SET ROLE {}; INSERT INTO p6c.items(value) VALUES(2); RESET ROLE",
        fixture.app
    ));
    assert_eq!(
        fixture.destination.sql("SELECT value::text FROM p6c.items")[0][0],
        "2"
    );
}

#[test]
fn sequence_sync_exact() {
    let _lock = SERIAL.lock().unwrap();
    let Some(mut fixture) = Fixture::new("sequence_sync_exact") else {
        return;
    };
    let ddl = "CREATE SEQUENCE p6c.descending_seq INCREMENT -3 MINVALUE -100 MAXVALUE -1 START -1 CYCLE; CREATE SEQUENCE p6c.uncalled START 23; CREATE TABLE p6c.serials(id serial);";
    fixture.source.sql(ddl);
    fixture.destination.sql(ddl);
    fixture.source.sql("SELECT setval('p6c.items_id_seq',81,true); SELECT setval('p6c.descending_seq',-97,true); SELECT setval('p6c.uncalled',41,false); SELECT setval('p6c.serials_id_seq',99,false);");
    let mut tables = fixture.tables();
    tables.push(Table {
        schema: "p6c".into(),
        name: "serials".into(),
        owner: tables[0].owner.clone(),
    });
    let scope = SequenceScope {
        tables,
        sequences: vec![
            SequenceName {
                schema: "p6c".into(),
                name: "descending_seq".into(),
            },
            SequenceName {
                schema: "p6c".into(),
                name: "uncalled".into(),
            },
        ],
    };
    let mut fence = fixture.fence();
    fixture.run_until(&mut fence, Step::Fenced);
    let states = sync_sequences(
        &mut fixture.source,
        &mut fixture.destination,
        &scope,
        "final-marker-49",
        TIMEOUT,
    )
    .unwrap();
    assert_eq!(states.len(), 4);
    assert!(states.iter().any(|s| s.sequence.identity_column.is_some()));
    assert!(
        states
            .iter()
            .any(|s| s.sequence.increment == -3 && s.sequence.cycle)
    );
    assert!(states.iter().any(|s| !s.is_called && s.last_value == 41));
    assert_eq!(
        states,
        sync_sequences(
            &mut fixture.source,
            &mut fixture.destination,
            &scope,
            "final-marker-49",
            TIMEOUT
        )
        .unwrap()
    );
    let sequences = discover_sequences(&mut fixture.destination, &scope, TIMEOUT).unwrap();
    assert_eq!(
        states,
        read_sequence_states(&mut fixture.destination, &sequences, TIMEOUT).unwrap()
    );
    assert_eq!(fixture.destination.sql("SELECT nextval('p6c.items_id_seq')::text,nextval('p6c.descending_seq')::text,nextval('p6c.uncalled')::text,nextval('p6c.serials_id_seq')::text")[0], ["82", "-100", "41", "99"]);
}

#[test]
fn default_only_sequence_catalog_discovery_and_sync() {
    let _lock = SERIAL.lock().unwrap();
    let Some(mut fixture) = Fixture::new("default_only_sequence_catalog_discovery_and_sync") else {
        return;
    };
    // Hand-built PostgreSQL catalog: no OWNED BY edge, only pg_attrdef dependencies.
    // Two defaults sharing one sequence must discover and synchronize it once.
    let ddl = "CREATE SEQUENCE p6c.default_only START 17; CREATE SEQUENCE p6c.unselected; ALTER TABLE p6c.items ADD COLUMN default_id bigint DEFAULT nextval('p6c.default_only'::regclass), ADD COLUMN other_default bigint DEFAULT nextval('p6c.default_only'::regclass);";
    fixture.source.sql(ddl);
    fixture.destination.sql(ddl);
    assert_eq!(fixture.source.sql("SELECT count(*)::text FROM pg_depend WHERE classid='pg_class'::regclass AND objid='p6c.default_only'::regclass AND deptype IN ('a','i')")[0][0], "0");
    fixture
        .source
        .sql("SELECT setval('p6c.default_only',731,false)");
    let scope = SequenceScope {
        tables: fixture.tables(),
        sequences: vec![],
    };
    let sequences = discover_sequences(&mut fixture.source, &scope, TIMEOUT).unwrap();
    assert_eq!(sequences.len(), 2); // identity + default-only; unrelated excluded
    let default = sequences
        .iter()
        .find(|s| s.name.name == "default_only")
        .unwrap();
    assert!(default.identity_column.is_none());
    assert!(sequences.iter().any(|s| s.identity_column.is_some()));
    let mut fence = fixture.fence();
    fixture.run_until(&mut fence, Step::Fenced);
    let states = sync_sequences(
        &mut fixture.source,
        &mut fixture.destination,
        &scope,
        "default-only-final-boundary",
        TIMEOUT,
    )
    .unwrap();
    assert_eq!(states.len(), 2);
    assert_eq!(
        states,
        read_sequence_states(&mut fixture.destination, &sequences, TIMEOUT).unwrap()
    );
    assert_eq!(
        fixture
            .destination
            .sql("SELECT last_value::text,is_called::text FROM p6c.default_only")[0],
        ["731", "false"]
    );
    assert_eq!(
        fixture
            .destination
            .sql("SELECT nextval('p6c.default_only')::text")[0][0],
        "731"
    );
}

#[test]
fn invalid_index_blocks() {
    let _lock = SERIAL.lock().unwrap();
    let Some(mut fixture) = Fixture::new("invalid_index_blocks") else {
        return;
    };
    fixture
        .destination
        .sql("INSERT INTO p6c.items(value) VALUES(1),(1)");
    assert!(
        fixture
            .destination
            .query(
                "CREATE UNIQUE INDEX CONCURRENTLY broken ON p6c.items(value)",
                TIMEOUT
            )
            .is_err()
    );
    fixture.destination.sql("ALTER TABLE p6c.items ADD CONSTRAINT unchecked CHECK(value>0) NOT VALID; CREATE FUNCTION p6c.trigger_fn() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'; CREATE TRIGGER external_effect BEFORE INSERT ON p6c.items FOR EACH ROW EXECUTE FUNCTION p6c.trigger_fn(); ALTER TABLE p6c.items ENABLE ALWAYS TRIGGER external_effect;");
    let tables = fixture.tables();
    let blockers =
        inspect_validity(&mut fixture.destination, &tables, &BTreeSet::new(), TIMEOUT).unwrap();
    assert_eq!(blockers.len(), 3);
    assert!(blockers.iter().all(|b| b.blocks()));
    let acknowledged = BTreeSet::from([blockers
        .iter()
        .find(|b| b.kind == BlockerKind::ExternalSideEffect)
        .unwrap()
        .acknowledgement_key()]);
    let mut fence = fixture.fence();
    fixture.run_until(&mut fence, Step::Fenced);
    fence
        .begin_activation("final-marker-49".into(), acknowledged.clone(), &mut |_| {
            Ok(())
        })
        .unwrap();
    assert!(
        matches!(fence.execute_next(&mut fixture.destination, &mut |_| Ok(())), Err(Error::Blocked(items)) if items.len()==2 && items.iter().all(|i| i.kind != BlockerKind::ExternalSideEffect))
    );
    assert_eq!(fence.next, Step::CheckActivation);
    fixture
        .destination
        .sql("DROP INDEX p6c.broken; ALTER TABLE p6c.items VALIDATE CONSTRAINT unchecked;");
    let remaining =
        inspect_validity(&mut fixture.destination, &tables, &acknowledged, TIMEOUT).unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(!remaining[0].blocks());
    fixture.run_until(&mut fence, Step::Complete);
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

#[test]
fn fence_and_restore_with_live_ddl_capture_do_not_stop_at_boundary() {
    use skedaddle::pg::{
        ddl::{self, Classification, SchemaVersions, TriggerSql},
        pgoutput::{Decoder, Message},
    };
    let _lock = SERIAL.lock().unwrap();
    let Some(mut fixture) =
        Fixture::new("fence_and_restore_with_live_ddl_capture_do_not_stop_at_boundary")
    else {
        return;
    };
    let config = fixture.fence().config;
    fixture.destination.sql(&format!(
        "ALTER ROLE {} LOGIN; GRANT INSERT,UPDATE,DELETE,TRUNCATE ON p6c.items TO {};",
        fixture.app, fixture.app
    ));
    let mut fence = DestinationFence::new(config).unwrap();
    let id = format!("d9a_{:x}", fixture.epoch);
    fixture
        .destination
        .sql(&format!("CREATE PUBLICATION {id} FOR TABLE p6c.items"));
    fixture.destination.sql(&format!(
        "SELECT slot_name FROM pg_create_logical_replication_slot('{id}','pgoutput')"
    ));
    let version: u32 = fixture.destination.sql("SHOW server_version_num")[0][0]
        .parse()
        .unwrap();
    let trigger = TriggerSql::new(&id, version).unwrap();
    fixture.destination.sql(&trigger.install);
    let baseline: u64 = fixture.destination.sql(&format!(
        "SELECT version::text FROM _skedaddle.skedaddle_ddl_{id}_version"
    ))[0][0]
        .parse()
        .unwrap();
    let changes = format!(
        "SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes('{id}',NULL,NULL,'proto_version','1','publication_names','{id}','messages','true')"
    );
    fixture.destination.sql(&changes); // Discard only the pre-baseline installation.
    let mut versions = SchemaVersions::new(id.clone(), baseline, []).unwrap();
    fixture.run_until(&mut fence, Step::Fenced);
    let mut saw_revoke = false;
    let mut saw_grant = false;
    for restore in [false, true] {
        if restore {
            fence
                .begin_activation("d9-final-marker".into(), BTreeSet::new(), &mut |_| Ok(()))
                .unwrap();
            fixture.run_until(&mut fence, Step::Complete);
        }
        let mut records = vec![];
        for row in fixture.destination.sql(&changes) {
            let bytes: Vec<u8> = row[0]
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let message = Decoder::default().decode(&bytes).unwrap();
            if let Message::LogicalMessage {
                prefix: ddl::MESSAGE_PREFIX,
                ..
            } = message
            {
                records.push(ddl::decode(&message, &id).unwrap());
            }
        }
        assert!(!records.is_empty(), "must exercise captured GRANT/REVOKE");
        // Each replay is a distinct committed transaction, even if idempotent.
        let mut start = 0;
        while start < records.len() {
            let end = (start + 1..records.len())
                .find(|&i| records[i].xid != records[start].xid)
                .unwrap_or(records.len());
            for record in &records[start..end] {
                assert_eq!(record.origin.as_deref(), Some(id.as_str()));
                saw_revoke |= record.tag == "REVOKE";
                saw_grant |= record.tag == "GRANT";
            }
            assert!(matches!(
                versions.classify_transaction(&records[start..end]),
                Classification::Supported { .. }
            ));
            start = end;
        }
    }
    assert!(saw_revoke && saw_grant);
    assert!(versions.blocker().is_none());
}
