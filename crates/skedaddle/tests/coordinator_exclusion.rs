mod support;

use skedaddle::coordinator::{
    MigrationExclusion,
    exclusion::{Error, ExclusionGuard},
};
use std::time::Duration;

#[tokio::test]
async fn independent_coordinators_serialize_promotion() {
    let objects = support::scratch("exclusion-objects");
    let locks = support::scratch("exclusion-locks");
    let ja = support::journal(support::fs_store(&objects), "migration");
    let jb = support::journal(support::fs_store(&objects), "migration");
    let mut a = support::coordinator(ja.clone(), "exclusion-a")
        .with_exclusion(MigrationExclusion::file(&locks).unwrap());
    let mut b = support::coordinator(jb, "exclusion-b")
        .with_exclusion(MigrationExclusion::file(&locks).unwrap());
    assert_eq!(a.acquire().await.unwrap(), 1);
    let guard = a.exclusive().await.unwrap();
    assert_eq!(guard.owner(), Some((a.instance_id(), 1)));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), b.acquire())
            .await
            .is_err()
    );
    assert!(!b.is_owner());
    let state = ja.load(true).await.unwrap();
    assert_eq!((state.epoch, state.applied_seq), (1, 1));
    drop(guard);
    assert_eq!(b.acquire().await.unwrap(), 2);
    assert_eq!(ja.load(true).await.unwrap().applied_seq, 2);
    assert!(matches!(a.exclusive().await, Err(Error::NotOwner)));
}

#[tokio::test]
async fn held_guard_promotion_is_not_recursive() {
    let exclusion = MigrationExclusion::file(support::scratch("held-locks")).unwrap();
    let mut c = support::coordinator(support::fresh_journal("held-journal"), "held-owner")
        .with_exclusion(exclusion.clone());
    let guard = exclusion.acquire("migration").await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), c.acquire_with_guard(&guard))
            .await
            .unwrap()
            .unwrap(),
        1
    );
    drop(guard);
    let guard = c.exclusive().await.unwrap();
    assert_eq!(c.acquire_with_guard(&guard).await.unwrap(), 2);
    // The snapshot remains epoch 1: external authorities must compare it with
    // their requested key, rather than silently reusing it for a new epoch.
    assert_eq!(guard.owner(), Some((c.instance_id(), 1)));
}

#[tokio::test]
async fn panic_error_and_cancel_release_guards() {
    let exclusion = MigrationExclusion::file(support::scratch("unwind-locks")).unwrap();
    let guard = exclusion.acquire("migration").await.unwrap();
    let panic = tokio::spawn(async move {
        let _guard = guard;
        panic!("intentional exclusion unwind");
    })
    .await
    .unwrap_err();
    assert!(panic.is_panic());
    let guard = exclusion.try_acquire("migration").await.unwrap();
    async fn fail(_guard: ExclusionGuard) -> Result<(), &'static str> {
        Err("injected")
    }
    assert_eq!(fail(guard).await, Err("injected"));
    let guard = exclusion.try_acquire("migration").await.unwrap();
    let pending = {
        let exclusion = exclusion.clone();
        tokio::spawn(async move { exclusion.acquire("migration").await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    pending.abort();
    assert!(pending.await.is_err());
    drop(guard);
    exclusion
        .try_acquire("migration")
        .await
        .unwrap()
        .check_alive()
        .await
        .unwrap();
}

#[tokio::test]
async fn different_migrations_and_guard_validation() {
    let exclusion = MigrationExclusion::file(support::scratch("different-locks")).unwrap();
    let guard = exclusion.acquire("migration").await.unwrap();
    let other = exclusion
        .try_acquire("other/../../safe?\\name")
        .await
        .unwrap();
    other.check_alive().await.unwrap();
    // Their base64 encodings differ only in case (YWFh / YWFH).
    let lower = exclusion.try_acquire("aaa").await.unwrap();
    let upper = exclusion.try_acquire("aaG").await.unwrap();
    lower.check_alive().await.unwrap();
    upper.check_alive().await.unwrap();
    let mut c = support::coordinator(support::fresh_journal("wrong-guard"), "wrong")
        .with_exclusion(exclusion.clone());
    assert!(c.acquire_with_guard(&other).await.is_err());
    let another_backend = MigrationExclusion::file(support::scratch("wrong-backend")).unwrap();
    let wrong = another_backend.acquire("migration").await.unwrap();
    assert!(c.acquire_with_guard(&wrong).await.is_err());
    assert!(!c.is_owner());
    c.acquire_with_guard(&guard).await.unwrap();
}

#[tokio::test]
async fn unconfigured_behavior_and_owner_scope() {
    let j = support::fresh_journal("unconfigured");
    let mut a = support::coordinator(j.clone(), "unconfigured-a");
    let mut b = support::coordinator(j, "unconfigured-b");
    assert!(matches!(a.exclusive().await, Err(Error::NotConfigured)));
    assert_eq!(a.acquire().await.unwrap(), 1);
    assert_eq!(b.acquire().await.unwrap(), 2);
    assert!(matches!(b.exclusive().await, Err(Error::NotConfigured)));
    let mut observer =
        support::coordinator(support::fresh_journal("observer-exclusion"), "observer")
            .with_exclusion(MigrationExclusion::file(support::scratch("observer-locks")).unwrap());
    assert!(matches!(observer.exclusive().await, Err(Error::NotOwner)));
}

// Reuse this integration-test executable: no feature-only binary or dependency.
#[test]
fn process_probe() {
    let Some(root) = std::env::var_os("SKEDADDLE_EXCLUSION_PROBE_ROOT") else {
        return;
    };
    let mode = std::env::var("SKEDADDLE_EXCLUSION_PROBE_MODE").unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let exclusion = MigrationExclusion::file(&root).unwrap();
        if mode == "busy" {
            assert!(matches!(
                exclusion.try_acquire("migration").await,
                Err(Error::Busy)
            ));
        } else {
            let _guard = exclusion.try_acquire("migration").await.unwrap();
            if mode == "hold" {
                std::fs::write(std::path::Path::new(&root).join("ready"), b"held").unwrap();
                std::future::pending::<()>().await;
            }
        }
    });
}

fn child(root: &std::path::Path, mode: &str) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "process_probe", "--nocapture"])
        .env("SKEDADDLE_EXCLUSION_PROBE_ROOT", root)
        .env("SKEDADDLE_EXCLUSION_PROBE_MODE", mode);
    command
}

#[tokio::test]
async fn other_process_contends_and_stale_file_does_not_block() {
    let root = support::scratch("process-locks");
    let exclusion = MigrationExclusion::file(&root).unwrap();
    let guard = exclusion.acquire("migration").await.unwrap();
    let result = child(&root, "busy").output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    drop(guard);
    assert!(child(&root, "available").output().unwrap().status.success());

    let mut holder = child(&root, "hold")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !root.join("ready").exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ready = root.join("ready").exists();
    let busy = matches!(exclusion.try_acquire("migration").await, Err(Error::Busy));
    holder.kill().unwrap();
    holder.wait().unwrap();
    assert!(ready, "child did not acquire lock");
    assert!(busy);
    exclusion
        .try_acquire("migration")
        .await
        .unwrap()
        .check_alive()
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_dir(root).unwrap().count(),
        2,
        "lock file remains beside ready marker"
    );
}

#[tokio::test]
async fn postgres_sessions_contend_and_termination_revokes_guard() {
    if std::env::var_os("SKEDADDLE_TEST_CONFIG").is_none() {
        use std::io::Write;
        writeln!(std::io::stderr(), "SKIP coordinator exclusion real-PG: SKEDADDLE_TEST_CONFIG absent; contention/termination qualification NOT RUN").unwrap();
        return;
    }
    let cfg = support::config();
    let ca = cfg["ca_pem"]
        .as_str()
        .or_else(|| cfg["replication"]["ca_pem"].as_str())
        .unwrap();
    let config = skedaddle::pg::tls::Config::new(cfg["pg"].as_str().unwrap().parse().unwrap(), ca);
    let policy = skedaddle::security::TlsPolicy::Required;
    let a = MigrationExclusion::postgres(config.clone(), policy);
    let b = MigrationExclusion::postgres(config.clone(), policy);
    let migration = format!("exclusion-{}", uuid::Uuid::new_v4());
    let guard = a.acquire(&migration).await.unwrap();
    guard.check_alive().await.unwrap();
    assert!(matches!(b.try_acquire(&migration).await, Err(Error::Busy)));
    let other = b.try_acquire(&format!("{migration}-other")).await.unwrap();
    let (admin, driver, _) = skedaddle::pg::tls::connect(&config, &policy).await.unwrap();
    let driver = tokio::spawn(driver);
    assert!(
        admin
            .query_one(
                "SELECT pg_terminate_backend($1)",
                &[&guard.backend_pid().unwrap()]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(matches!(guard.check_alive().await, Err(Error::Lost)));
    let successor = tokio::time::timeout(Duration::from_secs(5), b.acquire(&migration))
        .await
        .unwrap()
        .unwrap();
    successor.check_alive().await.unwrap();
    drop((guard, successor, other));

    // Drop outside block_on while the acquiring current-thread runtime stays
    // alive but idle. Aborting a task on that runtime alone leaks the socket.
    let idle_migration = format!("{migration}-idle-runtime");
    let holder_migration = idle_migration.clone();
    let holder_config = config.clone();
    let (dropped, ready) = tokio::sync::oneshot::channel();
    let (release, retained) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let exclusion = MigrationExclusion::postgres(holder_config, policy);
        let guard = runtime
            .block_on(exclusion.acquire(&holder_migration))
            .unwrap();
        drop(guard);
        let _ = dropped.send(());
        let _ = retained.recv();
        drop(runtime);
    });
    ready.await.unwrap();
    let acquired = tokio::time::timeout(Duration::from_secs(5), b.acquire(&idle_migration)).await;
    release.send(()).unwrap();
    holder.join().unwrap();
    acquired.unwrap().unwrap().check_alive().await.unwrap();
    drop(admin);
    driver.await.unwrap().unwrap();
}
