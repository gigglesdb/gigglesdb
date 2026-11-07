mod support;

#[cfg(feature = "test-crash")]
use skedaddle::coordinator::{journal::Journal, local::Local, *};
#[cfg(feature = "test-crash")]
use support::*;

#[cfg(feature = "test-crash")]
fn fixture(
    op: &str,
    objects: &std::path::Path,
    local: &std::path::Path,
    output: &std::path::Path,
) -> std::process::Output {
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_coordinator-fixture"))
        .args([
            op,
            objects.to_str().unwrap(),
            local.to_str().unwrap(),
            output.to_str().unwrap(),
        ])
        .env(
            "TEMP",
            std::env::var_os("TEMP").expect("TEMP must be set to J: task folder"),
        )
        .env(
            "TMP",
            std::env::var_os("TMP").expect("TMP must be set to J: task folder"),
        )
        .env(
            "CARGO_TARGET_DIR",
            std::env::var_os("CARGO_TARGET_DIR").expect("CARGO_TARGET_DIR must be set"),
        )
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let start = std::time::Instant::now();
    while child.try_wait().unwrap().is_none() {
        if start.elapsed() > std::time::Duration::from_secs(10) {
            let _ = child.kill();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

#[cfg(feature = "test-crash")]
#[tokio::test]
async fn crash_after_put_before_local_apply_replays() {
    for op in [
        "put",
        "local",
        "effect-fence",
        "effect-unfence",
        "effect-marker",
        "effect-activation",
        "effect-rollback",
        "effect-buffer",
    ] {
        let objects = scratch(&format!("crash-{op}-objects"));
        let local_path = scratch("crash-parent").join("db");
        let effects = scratch("effects");
        let result = fixture(op, &objects, &local_path, &effects);
        assert_eq!(
            result.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let j = Journal::new(
            fs_store(&objects),
            "p2",
            "fixture",
            std::time::Duration::from_secs(3),
        )
        .unwrap();
        let state = j.load(false).await.unwrap();
        assert_eq!(state.applied_seq, 2);
        let prefix = if op == "put" {
            let mut prefix = Materialized::default();
            for entry in j.history_page_through(0, 1, 1).await.unwrap() {
                prefix.apply(&entry).unwrap();
            }
            prefix
        } else {
            state.clone()
        };
        let mut local = Local::open(&local_path, &prefix).unwrap();
        assert_eq!(local.state().applied_seq, if op == "put" { 1 } else { 2 });
        local.replay_journal(&j).await.unwrap();
        assert_eq!(local.state(), &state);
        if op.starts_with("effect") {
            assert_eq!(std::fs::read_dir(&effects).unwrap().count(), 1);
            assert!(matches!(
                state.audit[&2].transition,
                Transition::Intent { .. }
            ));
            let mut successor = epoch::Coordinator::observer(j, local, "successor").unwrap();
            assert_eq!(successor.acquire().await.unwrap(), 2);
            successor
                .resume_effect(2, |token| async move {
                    assert_eq!(
                        (token.epoch(), token.seq(), token.authority_epoch()),
                        (1, 2, 2)
                    );
                    let key = effects.join(format!("effect-{}-{}", token.epoch(), token.seq()));
                    assert_eq!(std::fs::read_to_string(key).unwrap(), token.payload());
                    assert_eq!(std::fs::read_dir(effects).unwrap().count(), 1);
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(successor.state().applied_seq, 5);
            successor
                .resume_effect(2, |_| async { panic!("completed effect ran again") })
                .await
                .unwrap();
        }
    }
}

#[cfg(feature = "test-crash")]
#[tokio::test]
#[ignore = "DOCKER PG18 CRASH TEST: run harness/run.ps1 p2"]
async fn pg_crash_effect_adoption_preserves_original_key() {
    let cfg = config();
    for op in ["effect-pg-before", "effect-pg"] {
        let (mut client, connection) =
            tokio_postgres::connect(cfg["pg"].as_str().unwrap(), tokio_postgres::NoTls)
                .await
                .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        epoch::initialize_pg_authority(&client).await.unwrap();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS business_effects (id integer PRIMARY KEY);
            DELETE FROM business_effects; DELETE FROM _skedaddle.effects;
            DELETE FROM _skedaddle.authority",
            )
            .await
            .unwrap();
        let objects = scratch("pg-crash-objects");
        let path = scratch("pg-crash-local").join("db");
        let cfg_path = scratch("pg-crash-config").join("config.json");
        std::fs::write(&cfg_path, serde_json::to_vec(&cfg).unwrap()).unwrap();
        let child = fixture(op, &objects, &path, &cfg_path);
        assert_eq!(
            child.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let j = Journal::new(
            fs_store(objects),
            "p2",
            "fixture",
            std::time::Duration::from_secs(3),
        )
        .unwrap();
        let state = j.load(false).await.unwrap();
        let local = Local::open(path, &state).unwrap();
        let mut successor = epoch::Coordinator::observer(j, local, "pg-successor").unwrap();
        assert_eq!(successor.acquire().await.unwrap(), 2);
        successor
            .resume_effect(2, |token| async {
                assert_eq!(
                    (token.epoch(), token.seq(), token.authority_epoch()),
                    (1, 2, 2)
                );
                epoch::apply_pg_effect(&mut client, token).await
            })
            .await
            .unwrap();
        successor
            .resume_effect(4, |_| async { panic!("completed adoption repeated") })
            .await
            .unwrap();
        assert_eq!(
            client
                .query_one("SELECT count(*) FROM business_effects", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        let ledger = client
            .query("SELECT epoch,seq FROM _skedaddle.effects", &[])
            .await
            .unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!(
            (ledger[0].get::<_, i64>(0), ledger[0].get::<_, i64>(1)),
            (1, 2)
        );
        assert_eq!(
            client
                .query_one("SELECT epoch FROM _skedaddle.authority", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            2
        );
    }
}

#[cfg(feature = "test-crash")]
#[tokio::test]
#[ignore = "DOCKER PG18/TOXIPROXY: real lost COMMIT reply; run harness/run.ps1 p2"]
async fn pg_commit_response_loss_steps_down_and_deduplicates() {
    let cfg = config();
    let (mut direct, connection) =
        tokio_postgres::connect(cfg["pg"].as_str().unwrap(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    epoch::initialize_pg_authority(&direct).await.unwrap();
    direct.batch_execute("CREATE TABLE IF NOT EXISTS business_effects (id bigint PRIMARY KEY);
        DELETE FROM business_effects; DELETE FROM _skedaddle.effects; DELETE FROM _skedaddle.authority").await.unwrap();
    let http = reqwest::Client::new();
    let toxic = format!("{}/proxies/pg/toxics", cfg["toxiproxy"].as_str().unwrap());
    let _ = http
        .delete(format!("{toxic}/commit-response"))
        .send()
        .await
        .unwrap();
    let (mut proxied, connection) =
        tokio_postgres::connect(cfg["proxy_pg"].as_str().unwrap(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let j = fresh_journal("pg-lost-commit");
    let mut first = coordinator(j.clone(), "first");
    first.acquire().await.unwrap();
    let result = first.effect(EffectKind::Activation, "INSERT INTO business_effects VALUES (1)".into(), |token| async {
        epoch::apply_pg_effect_with_commit_hook(&mut proxied, token, || async {
            http.post(&toxic).json(&serde_json::json!({"name":"commit-response","type":"timeout","stream":"downstream","toxicity":1,"attributes":{"timeout":1000}}))
                .send().await.unwrap().error_for_status().unwrap();
            Ok(())
        }).await
    }).await;
    http.delete(format!("{toxic}/commit-response"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(
        matches!(result, Err(Error::UnknownOutcome(_))),
        "{result:?}"
    );
    assert!(!first.is_owner());
    assert_eq!(
        direct
            .query_one("SELECT count(*) FROM business_effects", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    let mut second = coordinator(j, "second");
    second.acquire().await.unwrap();
    second
        .resume_effect(2, |token| async {
            epoch::apply_pg_effect(&mut direct, token).await
        })
        .await
        .unwrap();
    assert_eq!(
        direct
            .query_one("SELECT count(*) FROM business_effects", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        direct
            .query_one("SELECT count(*) FROM _skedaddle.effects", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        direct
            .query_one("SELECT epoch FROM _skedaddle.authority", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
}

#[cfg(feature = "test-crash")]
#[tokio::test]
#[ignore = "DOCKER PG18: migrations have separate durable effect namespaces; run harness/run.ps1 p2"]
async fn pg_effect_namespaces_do_not_cross_fence_or_deduplicate() {
    let cfg = config();
    let (mut client, connection) =
        tokio_postgres::connect(cfg["pg"].as_str().unwrap(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    epoch::initialize_pg_authority(&client).await.unwrap();
    client.batch_execute("CREATE TABLE IF NOT EXISTS business_effects (id bigint PRIMARY KEY);
        DELETE FROM business_effects; DELETE FROM _skedaddle.effects; DELETE FROM _skedaddle.authority").await.unwrap();
    let store = fs_store(scratch("pg-namespaces"));
    for name in ["migration-one", "migration-two"] {
        let j = journal(store.clone(), name);
        let mut c = coordinator(j.clone(), "owner");
        c.acquire().await.unwrap();
        c.effect(
            EffectKind::Activation,
            "INSERT INTO business_effects SELECT COALESCE(MAX(id),0)+1 FROM business_effects"
                .into(),
            |token| async {
                assert_eq!(token.migration(), j.namespace());
                epoch::apply_pg_effect(&mut client, token).await
            },
        )
        .await
        .unwrap();
        c.acquire().await.unwrap();
        c.effect(EffectKind::FinalMarker, "SELECT 1".into(), |token| async {
            epoch::apply_pg_effect(&mut client, token).await
        })
        .await
        .unwrap();
    }
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM business_effects", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        client
            .query_one(
                "SELECT count(DISTINCT migration) FROM _skedaddle.effects",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    let rows = client
        .query("SELECT epoch FROM _skedaddle.authority", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.get::<_, i64>(0) == 2));
}

#[cfg(feature = "test-crash")]
#[tokio::test]
#[ignore = "DOCKER PG18: fresh authority schema including repeated setup; run harness/run.ps1 p2"]
async fn pg_authority_schema_initializes_fresh_database() {
    let cfg = config();
    let (admin, connection) =
        tokio_postgres::connect(cfg["pg"].as_str().unwrap(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let name = format!(
        "p2_init_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .batch_execute(&format!("CREATE DATABASE {name} TEMPLATE template0"))
        .await
        .unwrap();
    let connection_string = cfg["pg"]
        .as_str()
        .unwrap()
        .replace("dbname=skedaddle", &format!("dbname={name}"));
    let (mut client, connection) =
        tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client.batch_execute(&format!("CREATE ROLE {name} NOLOGIN; REVOKE CREATE ON DATABASE {name} FROM PUBLIC; SET ROLE {name}")).await.unwrap();
    assert!(matches!(epoch::initialize_pg_authority(&client).await,
        Err(Error::Postgres(e)) if e.code() == Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)));
    client.batch_execute("RESET ROLE").await.unwrap();
    epoch::initialize_pg_authority(&client).await.unwrap();
    epoch::initialize_pg_authority(&client).await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM _skedaddle.authority", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM _skedaddle.effects", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let mut c = coordinator(fresh_journal("pg-fresh"), "owner");
    c.acquire().await.unwrap();
    c.effect(EffectKind::Activation, "SELECT 1".into(), |token| async {
        epoch::apply_pg_effect(&mut client, token).await
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM _skedaddle.effects", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}

#[cfg(feature = "test-crash")]
#[tokio::test]
async fn rebuild_on_new_host_equals_original() {
    let objects = scratch("newhost-objects");
    let original = scratch("oldhost").join("db");
    let effects = scratch("newhost-effects");
    assert_eq!(
        fixture("local", &objects, &original, &effects)
            .status
            .code(),
        Some(86)
    );
    let fresh = scratch("newhost").join("db");
    let result_file = scratch("newhost-result").join("state.json");
    let checkpoint_journal = Journal::new(
        fs_store(&objects),
        "p2",
        "fixture",
        std::time::Duration::from_secs(3),
    )
    .unwrap();
    let checkpoint_state = checkpoint_journal.load(false).await.unwrap();
    let original_local = Local::open(&original, &checkpoint_state).unwrap();
    original_local
        .checkpoint(&checkpoint_journal)
        .await
        .unwrap();
    drop(original_local);
    let result = fixture("rebuild", &objects, &fresh, &result_file);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let j = Journal::new(
        fs_store(objects),
        "p2",
        "fixture",
        std::time::Duration::from_secs(3),
    )
    .unwrap();
    let expected = j.load(false).await.unwrap();
    let actual: Materialized =
        serde_json::from_slice(&std::fs::read(result_file).unwrap()).unwrap();
    assert_eq!(actual, expected);
    let old = Local::open(original, &expected).unwrap();
    let new = Local::open(fresh, &expected).unwrap();
    for table in [
        "definition",
        "lifecycle",
        "journal_applied",
        "progress",
        "chunks",
        "audit",
        "fence_state",
        "verifier_ranges",
        "verifier_exceptions",
    ] {
        let values = |local: &Local| {
            local
                .read_collection(table, 2)
                .unwrap()
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&old), values(&new));
    }
}

#[cfg(not(feature = "test-crash"))]
#[test]
#[ignore = "CRASH SUITE SKIPPED: enable --features test-crash, or run harness/run.ps1 p2 (no environment-controlled crash seams)"]
fn crash_suite_requires_test_crash_feature() {}
