#![allow(dead_code)]

#[path = "root.rs"]
pub mod test_root;

pub mod counting;
pub mod oracle;
pub mod workload;

use object_store::ObjectStore;
use skedaddle::coordinator::{
    Materialized, ReadinessFacts, Transition, epoch::Coordinator, journal::Journal, local::Local,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub fn scratch(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = test_root::scratch_root();
    let path = root.join(format!(
        "p2-{name}-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}
pub fn fs_store(root: impl AsRef<std::path::Path>) -> Arc<dyn ObjectStore> {
    Arc::new(skedaddle::coordinator::journal::local_store(root).unwrap())
}

pub fn relative_files(root: &std::path::Path) -> Vec<PathBuf> {
    fn visit(root: &std::path::Path, dir: &std::path::Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            assert!(!kind.is_symlink(), "unexpected symlink: {:?}", entry.path());
            if kind.is_dir() {
                visit(root, &entry.path(), files);
            } else {
                files.push(entry.path().strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }
    let mut files = Vec::new();
    visit(root, root, &mut files);
    files.sort();
    files
}

pub fn assert_database_only(root: &std::path::Path) {
    let entries = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, [std::ffi::OsString::from("db")]);
    assert_eq!(relative_files(root), [PathBuf::from("db/data.redb")]);
}
pub fn journal(store: Arc<dyn ObjectStore>, id: &str) -> Journal {
    Journal::new(store, "p2", id, Duration::from_secs(3)).unwrap()
}
pub fn fresh_journal(name: &str) -> Journal {
    journal(fs_store(scratch(name)), "migration")
}
pub fn local(name: &str) -> Local {
    Local::create(scratch(name).join("db")).unwrap()
}
pub fn coordinator(journal: Journal, name: &str) -> Coordinator {
    Coordinator::observer(journal, local(name), name).unwrap()
}
pub fn progress(n: u64) -> Transition {
    Transition::Progress {
        stream: "s".into(),
        marker: "m".into(),
        position: n.to_string(),
    }
}
pub fn ready() -> ReadinessFacts {
    ReadinessFacts {
        all_chunks_done: true,
        capture_healthy: true,
        lineage_matches: true,
        apply_lag: 5,
        lag_budget: 5,
        blocked_ddl: false,
        unknown_schema_job: false,
        verified_rows: 100,
        in_scope_rows: 100,
        busy_rows_past_budget: 0,
        dead_letters: 0,
        indexes_constraints_valid: true,
        sequences_syncable: true,
        reverse_prerequisites: true,
        fence_risks_acknowledged: true,
        buffer_contract_matches: true,
    }
}
pub async fn history(j: &Journal, n: u64) -> Materialized {
    let mut state = Materialized::default();
    for i in 0..n {
        let transition = if i == 0 {
            Transition::AcquireOwnership { epoch: 1 }
        } else {
            progress(i)
        };
        let e = skedaddle::coordinator::journal::Entry::new(&state, transition, "writer").unwrap();
        j.put(&e).await.unwrap();
        state.apply(&e).unwrap();
    }
    state
}
pub fn config() -> serde_json::Value {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG")
        .expect("Docker test requires harness/run.ps1 p2 (configuration file missing)");
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
pub fn backend(name: &str, cfg: &serde_json::Value) -> Arc<dyn ObjectStore> {
    match name {
        "local" => fs_store(scratch("backend-local")),
        "minio" => s3(cfg["minio"].as_str().unwrap()),
        "gcs" => {
            let auth=object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_bucket_name("p2-bucket").with_service_account_key(r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#)
            .with_skip_signature(true).with_base_url(cfg["gcs"].as_str().unwrap())
            .with_client_options(object_store::ClientOptions::new().with_allow_http(true).with_timeout(Duration::from_secs(2)))
            .with_retry(object_store::RetryConfig { max_retries:0,..Default::default() }).build().unwrap();
            Arc::new(
                skedaddle::coordinator::gcs::GcsJsonStore::for_emulator(
                    "p2-bucket",
                    cfg["gcs"].as_str().unwrap(),
                    auth,
                )
                .unwrap()
                .with_list_page_size(2)
                .unwrap(),
            )
        }
        _ => panic!("unknown backend"),
    }
}
pub fn s3(endpoint: &str) -> Arc<dyn ObjectStore> {
    s3_bucket(endpoint, "p2-bucket")
}
pub fn s3_bucket(endpoint: &str, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_access_key_id("skedaddle")
            .with_secret_access_key("skedaddle-secret")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_secs(2)),
            )
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                retry_timeout: Duration::from_secs(2),
                ..Default::default()
            })
            .build()
            .unwrap(),
    )
}

/// Test fixture inspection only. Product callers consume one page at a time.
pub async fn entries(j: &Journal) -> Vec<skedaddle::coordinator::journal::Entry> {
    let mut result = Vec::new();
    loop {
        let page = j
            .history_page(
                result
                    .last()
                    .map_or(0, |e: &skedaddle::coordinator::journal::Entry| e.seq),
                128,
            )
            .await
            .unwrap();
        if page.is_empty() {
            return result;
        }
        result.extend(page);
    }
}
