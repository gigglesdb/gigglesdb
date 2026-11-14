//! Opt-in real PostgreSQL tests. Point SKEDADDLE_PG_FENCE_DSN at a disposable
//! harness database (superuser, wal_level=logical, max_prepared_transactions>0).
//! Put psql on PATH or set SKEDADDLE_PSQL. No containers are started here.
//! Run with --nocapture to retain the loud infrastructure skip reasons.

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use skedaddle::{
    event::{
        AssembledTxn, EventError, Position, PositionOrder, SourceIdentity, SourceOrigin, TxnId,
    },
    pg::fence::{
        Breach, Catalog, Connection, Fence, FenceConfig, FenceError, FenceWindow, Membership,
        ResidualKind, Role, Rows, Step, Table, classify_residuals, detect_breach,
    },
};

static SERIAL: Mutex<()> = Mutex::new(());
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Psql {
    dsn: String,
    executable: String,
    scratch: PathBuf,
}

impl Psql {
    fn command(&self) -> Command {
        let mut command = Command::new(&self.executable);
        command.args([
            "-X",
            "-q",
            "-A",
            "-t",
            "-F",
            "\t",
            "-v",
            "ON_ERROR_STOP=1",
            "--dbname",
            &self.dsn,
        ]);
        command
    }

    fn sql(&mut self, sql: &str) -> Rows {
        self.query(sql, Duration::from_secs(10)).unwrap()
    }

    fn app(&self, role: &str) -> Self {
        // libpq accepts URI and keyword connection strings. The final keyword
        // occurrence overrides the admin identity for a real application login.
        let dsn = if let Some((protocol, rest)) = self.dsn.split_once("://") {
            let authority_end = rest.find('/').unwrap_or(rest.len());
            let authority = &rest[..authority_end];
            let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
            format!(
                "{protocol}://{role}:p10a_fixture@{host}{}",
                &rest[authority_end..]
            )
        } else {
            format!("{} user='{role}' password='p10a_fixture'", self.dsn)
        };
        Self {
            dsn,
            executable: self.executable.clone(),
            scratch: self.scratch.clone(),
        }
    }

    fn transaction(&self, sql: &str) -> Child {
        let mut command = self.command();
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(sql.as_bytes())
            .unwrap();
        child.stdin.take();
        child
    }
}

impl Connection for Psql {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<Rows, String> {
        let id = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stdout = self.scratch.join(format!("psql-{id}.out"));
        let stderr = self.scratch.join(format!("psql-{id}.err"));
        let mut child = self
            .command()
            .arg("-c")
            .arg(sql)
            .stdout(File::create(&stdout).map_err(|e| e.to_string())?)
            .stderr(File::create(&stderr).map_err(|e| e.to_string())?)
            .spawn()
            .map_err(|e| e.to_string())?;
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("psql wall-clock query deadline expired".into());
            }
            thread::sleep(Duration::from_millis(5));
        };
        let out = fs::read_to_string(&stdout).map_err(|e| e.to_string())?;
        let err = fs::read_to_string(&stderr).map_err(|e| e.to_string())?;
        // Test scratch is always beneath SKEDADDLE_TEST_ROOT.
        fs::remove_file(stdout).map_err(|e| e.to_string())?;
        fs::remove_file(stderr).map_err(|e| e.to_string())?;
        if !status.success() {
            return Err(err);
        }
        Ok(out
            .lines()
            .map(|line| line.split('\t').map(str::to_owned).collect())
            .collect())
    }
}

// Armed before installation: cleanup must work even when host psql cannot start.
struct ConstructionGuard {
    dsn: String,
    schema: String,
    app: String,
    armed: bool,
    created_hook_schema: bool,
}

impl Drop for ConstructionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Fixture::new is synchronous. Use the TLS driver directly rather than
        // the potentially missing/broken psql executable that caused setup to fail.
        let Ok(runtime) = tokio::runtime::Runtime::new() else {
            eprintln!("fence partial-construction cleanup: runtime unavailable");
            return;
        };
        let result = runtime.block_on(async {
            let (client, connection) = tls_connect(&self.dsn).await.map_err(|e| e.to_string())?;
            let driver = tokio::spawn(connection);
            let schema = &self.schema;
            let app = &self.app;
            let sql = format!("DROP EVENT TRIGGER IF EXISTS skedaddle_ddl_{schema}; DROP FUNCTION IF EXISTS _skedaddle.skedaddle_ddl_{schema}(); DROP TABLE IF EXISTS _skedaddle.skedaddle_ddl_{schema}_version; DROP SCHEMA IF EXISTS {schema} CASCADE; DROP ROLE IF EXISTS {app};");
            let result = client.batch_execute(&sql).await.map_err(|e| e.to_string());
            if result.is_ok() && self.created_hook_schema {
                // No CASCADE: a concurrent/foreign installation must be preserved.
                if let Err(error) = client.batch_execute("DROP SCHEMA IF EXISTS _skedaddle").await {
                    eprintln!("fence partial-construction shared schema preserved: {error}");
                }
            }
            drop(client);
            let _ = driver.await;
            result
        });
        if let Err(error) = result {
            eprintln!("fence partial-construction cleanup failed: {error}");
        }
    }
}

struct Probe {
    client: tokio_postgres::Client,
    runtime: tokio::runtime::Runtime,
}
impl Probe {
    fn connect(dsn: &str) -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (client, connection) = runtime.block_on(tls_connect(dsn)).unwrap();
        runtime.spawn(async move {
            let _ = connection.await;
        });
        Self { client, runtime }
    }
}
impl Connection for Probe {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<Rows, String> {
        self.runtime.block_on(async {
            let messages = tokio::time::timeout(timeout, self.client.simple_query(sql))
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            Ok(messages
                .into_iter()
                .filter_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(
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
    db: Psql,
    fence: Fence,
    schema: String,
    app: String,
    slot: Option<String>,
    publication: Option<String>,
    prepared: Option<String>,
}

impl Fixture {
    fn new(test: &str) -> Option<Self> {
        Self::new_with_psql(test, None)
    }

    fn new_with_psql(test: &str, executable: Option<&str>) -> Option<Self> {
        let Ok(dsn) = std::env::var("SKEDADDLE_PG_FENCE_DSN") else {
            eprintln!(
                "LOUD SKIP {test}: SKEDADDLE_PG_FENCE_DSN is unset; requires disposable harness PG, psql, superuser, logical WAL, and prepared transactions; no Docker was started"
            );
            return None;
        };
        let root = PathBuf::from(
            std::env::var("SKEDADDLE_TEST_ROOT")
                .expect("set SKEDADDLE_TEST_ROOT beneath this task's J: folder"),
        );
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let schema = format!("p10a_{epoch}");
        let app = format!("p10a_app_{epoch}");
        let scratch = root.join(&schema);
        fs::create_dir_all(&scratch).unwrap();
        let mut db = Psql {
            dsn,
            executable: executable.map(str::to_owned).unwrap_or_else(|| {
                std::env::var("SKEDADDLE_PSQL").unwrap_or_else(|_| "psql".into())
            }),
            scratch,
        };
        let mut construction = ConstructionGuard {
            dsn: db.dsn.clone(),
            schema: schema.clone(),
            app: app.clone(),
            armed: true,
            created_hook_schema: false,
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (client, connection) = tls_connect(&db.dsn).await.unwrap();
            let driver = tokio::spawn(connection);
            let version: String = client
                .query_one("SHOW server_version_num", &[])
                .await
                .unwrap()
                .get(0);
            construction.created_hook_schema = client
                .query_one("SELECT to_regnamespace('_skedaddle') IS NULL", &[])
                .await
                .unwrap()
                .get(0);
            let install =
                skedaddle::pg::ddl::TriggerSql::new(&schema, version.parse().unwrap()).unwrap();
            client.batch_execute(&install.install).await.unwrap();
            drop(client);
            driver.await.unwrap().unwrap();
        });
        let admin = db.sql("SELECT current_user")[0][0].clone();
        assert_eq!(
            db.sql("SELECT rolsuper::text FROM pg_roles WHERE rolname=current_user")[0][0],
            "true",
            "fence fixture requires a harness superuser"
        );
        db.sql(&format!("CREATE ROLE {app} LOGIN PASSWORD 'p10a_fixture'; CREATE SCHEMA {schema}; CREATE TABLE {schema}.items(id bigint PRIMARY KEY,value text); GRANT USAGE ON SCHEMA {schema} TO {app}; GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON {schema}.items TO {app}; GRANT UPDATE(value) ON {schema}.items TO {app} WITH GRANT OPTION;"));
        let table = Table {
            schema: schema.clone(),
            name: "items".into(),
            owner: admin,
        };
        let roles = db
            .sql("SELECT rolname,rolcanlogin::text,rolsuper::text,rolbypassrls::text FROM pg_roles")
            .into_iter()
            .map(|r| Role {
                name: r[0].clone(),
                login: r[1] == "true",
                superuser: r[2] == "true",
                bypass_rls: r[3] == "true",
            })
            .collect();
        let memberships = db
            .sql("SELECT pg_get_userbyid(member),pg_get_userbyid(roleid) FROM pg_auth_members")
            .into_iter()
            .map(|r| Membership {
                member: r[0].clone(),
                role: r[1].clone(),
            })
            .collect();
        let applications = BTreeSet::from([app.clone()]);
        let catalog = Catalog {
            roles,
            memberships,
            tables: vec![table.clone()],
            grants: vec![],
        };
        let acknowledged = classify_residuals(&catalog, &applications, &BTreeSet::new())
            .into_iter()
            .map(|r| r.principal)
            .collect();
        let mut fence = Fence::new(FenceConfig {
            epoch,
            applications,
            tables: vec![table],
            acknowledged,
            deadline_ms: 5_000,
        })
        .unwrap();
        fence.bind_migration(&schema).unwrap();
        construction.armed = false;
        Some(Self {
            db,
            fence,
            schema,
            app,
            slot: None,
            publication: None,
            prepared: None,
        })
    }

    fn step(&mut self) -> Result<(), FenceError> {
        self.fence
            .execute_next(&mut self.db, &mut |_| Ok(()), &mut || Ok(()))
    }

    fn through(&mut self, next: Step) {
        while self.fence.next != next {
            self.step().unwrap();
        }
    }

    fn wait_for_app_transaction(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.db.sql(&format!("SELECT pid::text FROM pg_stat_activity WHERE usename='{}' AND xact_start IS NOT NULL", self.app)).is_empty() {
            assert!(Instant::now() < deadline, "application transaction did not start");
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(gid) = &self.prepared {
            // This fixture created this exact GID. Never resolve other GIDs.
            let _ = self.db.query(
                &format!("ROLLBACK PREPARED '{gid}'"),
                Duration::from_secs(5),
            );
        }
        let _ = self.fence.unfence(&mut self.db, &mut |_| Ok(()));
        if let Some(slot) = &self.slot {
            let _ = self.db.query(
                &format!("SELECT pg_drop_replication_slot('{slot}')"),
                Duration::from_secs(5),
            );
        }
        let _ = self.db.query(
            &format!("BEGIN; DROP EVENT TRIGGER IF EXISTS skedaddle_ddl_{}; DROP FUNCTION IF EXISTS _skedaddle.skedaddle_ddl_{}(); DROP TABLE IF EXISTS _skedaddle.skedaddle_ddl_{}_version; COMMIT;", self.schema, self.schema, self.schema),
            Duration::from_secs(5),
        );
        let publication = self
            .publication
            .as_ref()
            .map(|p| format!("DROP PUBLICATION IF EXISTS {p};"))
            .unwrap_or_default();
        let saved_state = if self.fence.saved.is_some() {
            format!(
                "DELETE FROM _skedaddle.fence_state WHERE epoch={} AND migration='{}'; DELETE FROM _skedaddle.authority WHERE migration='{}';",
                self.fence.config.epoch, self.schema, self.schema
            )
        } else {
            String::new()
        };
        let sql = format!(
            "{publication} SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='{}'; DROP SCHEMA {} CASCADE; DROP ROLE {}; {saved_state}",
            self.app, self.schema, self.app
        );
        if let Err(error) = self.db.query(&sql, Duration::from_secs(10)) {
            eprintln!("fence fixture cleanup failed: {error}");
        }
    }
}

fn wait_child(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("terminated pooled session did not exit");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn stale_pooled_session_cannot_commit_after_fence() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut f) = Fixture::new("stale_pooled_session_cannot_commit_after_fence") else {
        return;
    };
    let mut stale = f.db.app(&f.app).transaction(&format!(
        "BEGIN; INSERT INTO {}.items VALUES(1,'stale'); SELECT pg_sleep(30); COMMIT;",
        f.schema
    ));
    f.wait_for_app_transaction();
    f.through(Step::Complete);
    assert!(!wait_child(&mut stale).success());
    assert_eq!(
        f.db.sql(&format!("SELECT count(*) FROM {}.items", f.schema))[0][0],
        "0"
    );
    assert!(
        f.db.app(&f.app)
            .query("SELECT 1", Duration::from_secs(3))
            .is_err(),
        "NOLOGIN must reject a pool reconnect"
    );
}

#[test]
fn in_flight_txn_drained_or_blocks() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut f) = Fixture::new("in_flight_txn_drained_or_blocks") else {
        return;
    };
    let mut active =
        f.db.app(&f.app)
            .transaction("BEGIN; SELECT pg_sleep(30); COMMIT;");
    f.wait_for_app_transaction();
    f.through(Step::Terminate);
    // Deliberately delay termination to prove the drain independently blocks.
    f.fence.next = Step::Drain;
    f.fence.config.deadline_ms = 500;
    let mut probe = Probe::connect(&f.db.dsn);
    f.wait_for_app_transaction();
    let result = f
        .fence
        .execute_next(&mut probe, &mut |_| Ok(()), &mut || Ok(()));
    eprintln!("in-flight persistent drain: {result:?}");
    assert!(
        matches!(result, Err(FenceError::DrainBlocked(_))),
        "{result:?}"
    );
    assert!(f.fence.drain_lsn.is_none());
    f.fence.next = Step::Terminate;
    f.fence.config.deadline_ms = 5_000;
    f.through(Step::Complete);
    assert!(!wait_child(&mut active).success());
}

#[test]
fn prepared_xact_blocks() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut f) = Fixture::new("prepared_xact_blocks") else {
        return;
    };
    assert_ne!(
        f.db.sql("SHOW max_prepared_transactions")[0][0],
        "0",
        "harness must enable prepared transactions"
    );
    let gid = format!("{}_owned_prepared", f.schema);
    f.prepared = Some(gid.clone());
    f.db.sql(&format!("BEGIN; SET LOCAL ROLE {}; INSERT INTO {}.items VALUES(1,'prepared'); PREPARE TRANSACTION '{gid}';", f.app, f.schema));
    f.through(Step::Drain);
    f.fence.config.deadline_ms = 500;
    let mut probe = Probe::connect(&f.db.dsn);
    let result = f
        .fence
        .execute_next(&mut probe, &mut |_| Ok(()), &mut || Ok(()));
    eprintln!("prepared persistent drain: {result:?}");
    assert!(
        matches!(result, Err(FenceError::DrainBlocked(ref rows)) if rows.iter().any(|r| r.get(1) == Some(&gid))),
        "{result:?}"
    );
    assert!(f.fence.drain_lsn.is_none());
    assert_eq!(
        f.db.sql(&format!(
            "SELECT gid FROM pg_prepared_xacts WHERE gid='{gid}'"
        ))[0][0],
        gid
    );
    // Only this fixture's creator resolves its own GID; fence never does so.
    f.db.sql(&format!("ROLLBACK PREPARED '{gid}'"));
    f.prepared = None;
    f.fence.config.deadline_ms = 5_000;
    f.through(Step::Complete);
}

struct PgOrder;
impl PositionOrder for PgOrder {
    fn domain(&self) -> &str {
        "pg:fence-test"
    }
    fn compare(&self, left: &[u8], right: &[u8]) -> Result<std::cmp::Ordering, EventError> {
        Ok(left.cmp(right))
    }
}

#[test]
fn residual_superuser_reported_and_detected() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut f) = Fixture::new("residual_superuser_reported_and_detected") else {
        return;
    };
    let publication = format!("{}_pub", f.schema);
    let slot = format!("{}_slot", f.schema);
    f.db.sql(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {}.items",
        f.schema
    ));
    f.publication = Some(publication.clone());
    f.db.sql(&format!(
        "SELECT slot_name FROM pg_create_logical_replication_slot('{slot}','pgoutput')"
    ));
    f.slot = Some(slot.clone());
    f.through(Step::Recheck);
    let acknowledgements = std::mem::take(&mut f.fence.config.acknowledged);
    assert!(
        matches!(f.step(), Err(FenceError::ResidualBlocked(ref residuals)) if residuals.iter().any(|r| r.principal.kind == ResidualKind::Superuser && !r.acknowledged))
    );
    f.fence.config.acknowledged = acknowledgements;
    f.through(Step::Complete);
    f.db.sql(&format!(
        "INSERT INTO {}.items VALUES(99,'residual superuser')",
        f.schema
    ));
    // Decode actual pgoutput Origin/Commit messages; no synthesized DB outcome.
    let mut origin = None;
    let mut breach = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !breach && Instant::now() < deadline {
        let records = f.db.sql(&format!("SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes('{slot}',NULL,NULL,'proto_version','1','publication_names','{publication}')"));
        for row in records {
            let hex = &row[0];
            let data: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            match data.first() {
                Some(b'B') => origin = None,
                Some(b'O') => {
                    origin = Some(SourceOrigin {
                        lsn: u64::from_be_bytes(data[1..9].try_into().unwrap()),
                        name: String::from_utf8(data[9..data.len() - 1].to_vec()).unwrap(),
                    })
                }
                Some(b'C') => {
                    let commit = u64::from_be_bytes(data[2..10].try_into().unwrap());
                    let txn = AssembledTxn::new_with_origin(
                        SourceIdentity {
                            engine: "postgresql".into(),
                            system_id: "fixture".into(),
                            lineage: "fixture".into(),
                            db_name: f.schema.clone(),
                            slot: slot.clone(),
                        },
                        TxnId(commit.to_be_bytes().to_vec()),
                        Position::new(PgOrder.domain(), commit.to_be_bytes()),
                        None,
                        origin.take(),
                        vec![],
                    )
                    .unwrap();
                    let drain =
                        Position::new(PgOrder.domain(), f.fence.drain_lsn.unwrap().to_be_bytes());
                    if detect_breach(
                        &txn,
                        &drain,
                        &PgOrder,
                        "skedaddle/fence-test",
                        FenceWindow::BeforeActivation,
                    )
                    .unwrap()
                        == Breach::AbortBeforeActivation
                    {
                        assert_eq!(
                            detect_breach(
                                &txn,
                                &drain,
                                &PgOrder,
                                "skedaddle/fence-test",
                                FenceWindow::RollbackWindow
                            )
                            .unwrap(),
                            Breach::CriticalAlert {
                                rollback_eligible: false
                            }
                        );
                        breach = true;
                    }
                }
                _ => {}
            }
        }
    }
    assert!(breach, "residual commit after drain was not detected");
}

#[test]
fn abort_before_activation_restores_source() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut f) = Fixture::new("abort_before_activation_restores_source") else {
        return;
    };
    // Persist/reload at every boundary and replay each in-flight intent against
    // real PG. Step 1 hooks must obey the documented idempotency contract.
    while f.fence.next != Step::Complete {
        let before: Fence =
            serde_json::from_str(&serde_json::to_string(&f.fence).unwrap()).unwrap();
        f.step().unwrap();
        let expected_next = f.fence.next;
        if before.next == Step::Save {
            f.fence.next = Step::Save;
        } else {
            f.fence = before;
        }
        f.step().unwrap();
        assert_eq!(f.fence.next, expected_next);
    }
    let original = f.fence.saved.clone();
    f.fence.unfence(&mut f.db, &mut |_| Ok(())).unwrap();
    f.fence.unfence(&mut f.db, &mut |_| Ok(())).unwrap();
    assert_eq!(f.fence.saved, original);
    let mut app = f.db.app(&f.app);
    app.sql(&format!("INSERT INTO {}.items VALUES(1,'restored'); UPDATE {}.items SET value='column restored' WHERE id=1;", f.schema, f.schema));
    assert_eq!(
        app.sql(&format!("SELECT value FROM {}.items WHERE id=1", f.schema))[0][0],
        "column restored"
    );
    assert_eq!(
        f.db.sql(&format!(
            "SELECT epoch::text FROM _skedaddle.authority WHERE migration='{}'",
            f.schema
        ))[0][0],
        f.fence.config.epoch.to_string()
    );
}

#[test]
fn authority_is_migration_scoped_monotonic_and_atomic_with_final_fence() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut f) =
        Fixture::new("authority_is_migration_scoped_monotonic_and_atomic_with_final_fence")
    else {
        return;
    };
    // Same epoch is valid for distinct migrations; the fence touches only its key.
    let other = format!("{}_other", f.schema);
    f.db.sql(&format!("INSERT INTO _skedaddle.authority VALUES('{}',7); INSERT INTO _skedaddle.authority VALUES('{}',{});", other, f.schema, f.fence.config.epoch - 1));
    f.through(Step::Authority);
    assert_eq!(
        f.fence
            .saved
            .as_ref()
            .unwrap()
            .authority
            .as_ref()
            .unwrap()
            .epoch,
        f.fence.config.epoch - 1
    );
    f.step().unwrap();
    assert_eq!(
        f.db.sql(&format!(
            "SELECT epoch::text FROM _skedaddle.authority WHERE migration='{}'",
            other
        ))[0][0],
        "7"
    );
    f.fence.next = Step::Authority;
    f.step().unwrap(); // same-epoch retry is idempotent
    let newer = f.fence.config.epoch + 1;
    f.db.sql(&format!(
        "UPDATE _skedaddle.authority SET epoch={newer} WHERE migration='{}'; ALTER ROLE {} LOGIN;",
        f.schema, f.app
    ));
    f.fence.next = Step::Authority;
    assert!(matches!(f.step(), Err(FenceError::Database(_))));
    assert_eq!(f.fence.next, Step::Authority);
    // A rejected epoch rolls back the final NOLOGIN statement in the same batch.
    assert_eq!(
        f.db.sql(&format!(
            "SELECT rolcanlogin::text FROM pg_roles WHERE rolname='{}'",
            f.app
        ))[0][0],
        "true"
    );
    assert_eq!(
        f.db.sql(&format!(
            "SELECT epoch::text FROM _skedaddle.authority WHERE migration='{}'",
            f.schema
        ))[0][0],
        newer.to_string()
    );
    assert!(matches!(
        f.fence.unfence(&mut f.db, &mut |_| Ok(())),
        Err(FenceError::Database(_))
    ));
    // Restore our fixture's role state for its normal cleanup; never rewind epoch.
    f.db.sql(&format!(
        "ALTER ROLE {} LOGIN; DELETE FROM _skedaddle.authority WHERE migration='{other}';",
        f.app
    ));
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
fn same_epoch_migrations_preserve_independent_saved_state() {
    let _serial = SERIAL.lock().unwrap();
    let Some(mut first) = Fixture::new("same_epoch_migrations") else {
        return;
    };
    let Some(mut second) = Fixture::new("same_epoch_migrations") else {
        return;
    };
    second.fence.config.epoch = first.fence.config.epoch;
    first.through(Step::NoLogin);
    second.through(Step::NoLogin);
    let first_saved = first.fence.saved.clone();
    let second_saved = second.fence.saved.clone();
    assert_ne!(first_saved, second_saved);
    for fixture in [&mut first, &mut second] {
        fixture.fence.saved = None;
        fixture.fence.next = Step::Save;
        fixture.step().unwrap();
    }
    assert_eq!(first.fence.saved, first_saved);
    assert_eq!(second.fence.saved, second_saved);
    let rows = first.db.sql(&format!("SELECT migration FROM _skedaddle.fence_state WHERE epoch={} AND migration IN ('{}','{}') ORDER BY migration", first.fence.config.epoch, first.schema, second.schema));
    assert_eq!(rows.len(), 2);
    drop(second);
    first.fence.saved = None;
    first.fence.next = Step::Save;
    first.step().unwrap();
    assert_eq!(first.fence.saved, first_saved);
}

#[test]
fn missing_psql_cleans_partial_construction() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Ok(dsn) = std::env::var("SKEDADDLE_PG_FENCE_DSN") else {
        eprintln!(
            "LOUD SKIP missing_psql_cleans_partial_construction: SKEDADDLE_PG_FENCE_DSN unset"
        );
        return;
    };
    let snapshot = || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (client, connection) = tls_connect(&dsn).await.unwrap();
            let driver = tokio::spawn(connection);
            let rows = client.query("SELECT 'trigger:' || evtname FROM pg_event_trigger WHERE evtname LIKE 'skedaddle_ddl_p10a_%' UNION ALL SELECT 'role:' || rolname FROM pg_roles WHERE rolname LIKE 'p10a_app_%' UNION ALL SELECT 'schema:' || nspname FROM pg_namespace WHERE nspname LIKE 'p10a_%' OR nspname='_skedaddle' ORDER BY 1", &[]).await.unwrap();
            let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
            drop(client);
            driver.await.unwrap().unwrap();
            names
        })
    };
    let before = snapshot();
    let missing = PathBuf::from(std::env::var("SKEDADDLE_TEST_ROOT").unwrap())
        .join("deliberately-absent-psql.exe");
    assert!(!missing.exists());
    let failed = std::panic::catch_unwind(|| {
        Fixture::new_with_psql("missing_psql_cleanup", Some(missing.to_str().unwrap()))
    });
    assert!(failed.is_err(), "missing psql must fail setup");
    assert_eq!(
        snapshot(),
        before,
        "partial fixture leaked database objects"
    );
}
