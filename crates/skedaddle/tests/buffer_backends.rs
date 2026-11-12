use object_store::aws::AmazonS3Builder;
use sha2::{Digest, Sha256};
use skedaddle::buffer::backends::{
    BufferStore, Encryption, Error, HostLoss, ObjectBufferStore, PutOutcome, TransportSecurity,
};
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
};
use std::{path::PathBuf, time::Duration};

const LOCAL_TEST: TransportSecurity = TransportSecurity::DisableForLocalTest;

#[tokio::test]
async fn local_stream_limit_accepts_maximum_and_rejects_one_more_byte() {
    const LIMIT: u64 = 1_000_000_000 + 64 * 1024 + 128;
    let root = scratch("stream-limit");
    let input = scratch("stream-limit-input").join("sparse-source");
    File::create(&input).unwrap().set_len(LIMIT).unwrap();
    let store = ObjectBufferStore::local(&root, Duration::from_secs(180)).unwrap();
    assert_eq!(
        store
            .put_file_create_only("maximum", File::open(&input).unwrap())
            .await,
        Ok(PutOutcome::Created)
    );
    assert_eq!(
        std::fs::metadata(root.join("maximum")).unwrap().len(),
        LIMIT
    );
    store.delete("maximum").await.unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&input)
        .unwrap()
        .set_len(LIMIT + 1)
        .unwrap();
    assert_eq!(
        store
            .put_file_create_only("oversized", File::open(&input).unwrap())
            .await,
        Err(Error::Invalid("streamed object exceeds 1 GB segment limit"))
    );
    assert!(!root.join("oversized").exists());
}

#[tokio::test]
async fn byte_only_store_defaults_refuse_streaming() {
    // This adapter uses real local storage for every required operation and
    // deliberately inherits the trait's unsupported streaming defaults.
    struct ByteOnly(ObjectBufferStore);
    #[async_trait::async_trait]
    impl BufferStore for ByteOnly {
        fn report(&self) -> skedaddle::buffer::backends::BackendReport {
            self.0.report()
        }
        async fn put_create_only(
            &self,
            key: &str,
            content: &[u8],
        ) -> skedaddle::buffer::backends::Result<PutOutcome> {
            self.0.put_create_only(key, content).await
        }
        async fn get(&self, key: &str) -> skedaddle::buffer::backends::Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn list_prefix(
            &self,
            prefix: &str,
            token: Option<&str>,
            limit: usize,
        ) -> skedaddle::buffer::backends::Result<skedaddle::buffer::backends::ListPage> {
            self.0.list_prefix(prefix, token, limit).await
        }
        async fn delete(&self, key: &str) -> skedaddle::buffer::backends::Result<()> {
            self.0.delete(key).await
        }
    }
    let store = ByteOnly(
        ObjectBufferStore::local(scratch("byte-only-defaults"), Duration::from_secs(5)).unwrap(),
    );
    assert_eq!(
        store.put_create_only("object", b"real").await,
        Ok(PutOutcome::Created)
    );
    assert_eq!(store.get("object").await.unwrap().unwrap(), b"real");
    let (input, _) = stream_fixture(1, b'x');
    assert_eq!(
        store
            .put_file_create_only("stream", File::open(&input).unwrap())
            .await,
        Err(Error::Invalid("streaming upload unsupported by this store"))
    );
    assert_eq!(
        store.get_into("object", &mut Vec::new()).await,
        Err(Error::Invalid(
            "streaming download unsupported by this store"
        ))
    );
}

#[tokio::test]
async fn local_single_character_leaf_put_reproducer() {
    let root = scratch("short-key-reproducer");
    let store = ObjectBufferStore::local(&root, Duration::from_secs(5)).unwrap();
    let mut outcomes = Vec::new();
    for (key, bytes) in [
        ("a", b"".as_slice()),
        ("b", b"x"),
        ("aa", b""),
        ("bb", b"x"),
        ("nested/a", b""),
        ("nested/b", b"x"),
        ("é", b"x"),
        ("nested/é", b""),
    ] {
        let result = store.put_create_only(key, bytes).await;
        eprintln!("{key}, {} bytes: {result:?}", bytes.len());
        outcomes.push(result);
    }
    assert!(outcomes.iter().all(|r| *r == Ok(PutOutcome::Created)));
}

#[tokio::test]
async fn local_durability_counts_include_resolved_puts_and_delete() {
    let root = scratch("durability-counts");
    let store = ObjectBufferStore::local(&root, Duration::from_secs(5)).unwrap();
    assert_eq!(store.local_sync_counts(), Some((0, 0)));
    assert_eq!(
        store.put_create_only("root", b"one").await,
        Ok(PutOutcome::Created)
    );
    assert_eq!(store.local_sync_counts(), Some((1, 1)));
    assert_eq!(
        store.put_create_only("root", b"one").await,
        Ok(PutOutcome::Resolved)
    );
    // The attempted staging file and the existing destination are both synced.
    assert_eq!(store.local_sync_counts(), Some((3, 2)));
    assert_eq!(
        store.put_create_only("root", b"two").await,
        Err(Error::Conflict)
    );
    assert_eq!(store.local_sync_counts(), Some((4, 2)));
    assert_eq!(store.get("root").await.unwrap().unwrap(), b"one");

    let (input, _) = stream_fixture(19, b'f');
    assert_eq!(
        store
            .put_file_create_only("nested/object", File::open(&input).unwrap())
            .await,
        Ok(PutOutcome::Created)
    );
    assert_eq!(store.local_sync_counts(), Some((5, 4)));
    assert_eq!(
        store
            .put_file_create_only("nested/object", File::open(&input).unwrap())
            .await,
        Ok(PutOutcome::Resolved)
    );
    assert_eq!(store.local_sync_counts(), Some((7, 6)));
    store.delete("nested/object").await.unwrap();
    assert_eq!(store.local_sync_counts(), Some((7, 8)));
    store.delete("nested/object").await.unwrap();
    store.delete("absent/parent/object").await.unwrap();
    assert_eq!(store.local_sync_counts(), Some((7, 8)));
    assert_eq!(walk(&root).iter().filter(|p| p.is_file()).count(), 1);
}

#[tokio::test]
async fn portable_keys_reject_aliases_and_accept_length_boundary() {
    let root = scratch("portable-keys");
    let store = ObjectBufferStore::local(&root, Duration::from_secs(5)).unwrap();
    let (input, _) = stream_fixture(1, b'x');
    let long = format!("{}bbb", "a/".repeat(2047));
    let owned = format!("nested/txn-{}.skedaddle-scratch", uuid::Uuid::new_v4());
    for key in [
        "",
        "nul.txt",
        "con",
        "PRN.log",
        "aux",
        "COM0",
        "com9.txt",
        "LPT1",
        "lpt9.log",
        "trailing ",
        "embedded\0zero",
        long.as_str(),
        owned.as_str(),
    ] {
        assert_eq!(store.get(key).await, Err(Error::UnsafeLocalPath), "{key:?}");
        assert_eq!(
            store.put_create_only(key, b"bad").await,
            Err(Error::UnsafeLocalPath),
            "{key:?}"
        );
        assert_eq!(
            store
                .put_file_create_only(key, File::open(&input).unwrap())
                .await,
            Err(Error::UnsafeLocalPath),
            "{key:?}"
        );
        assert_eq!(
            store.get_into(key, &mut std::io::sink()).await,
            Err(Error::UnsafeLocalPath),
            "{key:?}"
        );
        assert_eq!(
            store.delete(key).await,
            Err(Error::UnsafeLocalPath),
            "{key:?}"
        );
    }
    // Missing first directory avoids OS component/path limits while exercising
    // the public key parser's exact 4096-byte acceptance boundary.
    let boundary = format!("{}{}", "a/".repeat(2047), "bb");
    assert_eq!(boundary.len(), 4096);
    assert_eq!(store.get(&boundary).await.unwrap(), None);
    for key in [
        "COM",
        "LPT",
        "COMx",
        "LPTx",
        "COM10",
        "xCOM1",
        "ordinary.txt",
        "space inside",
    ] {
        assert_eq!(
            store.put_create_only(key, b"ok").await,
            Ok(PutOutcome::Created),
            "{key}"
        );
    }
}

#[tokio::test]
async fn local_listing_preserves_sizes_bounds_and_token_binding() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let root = scratch("listing-bounds");
    let store = ObjectBufferStore::local(&root, Duration::from_secs(5)).unwrap();
    for (key, bytes) in [
        ("aa", b"".as_slice()),
        ("ab", b"one"),
        ("ac/nested", b"four"),
        ("zz", b"outside"),
    ] {
        store
            .put_create_only(key, bytes)
            .await
            .unwrap_or_else(|error| panic!("{key}: {error:?}"));
    }
    std::fs::write(root.join("#foreign-stage"), b"hidden").unwrap();
    let first = store.list_prefix("a", None, 1).await.unwrap();
    assert_eq!(
        first.objects,
        vec![skedaddle::buffer::backends::BufferObject {
            key: "aa".into(),
            bytes: 0
        }]
    );
    let token = first.next.unwrap();
    let rest = store.list_prefix("a", Some(&token), 2).await.unwrap();
    assert_eq!(
        rest.objects
            .iter()
            .map(|o| (o.key.as_str(), o.bytes))
            .collect::<Vec<_>>(),
        vec![("ab", 3), ("ac/nested", 4)]
    );
    assert_eq!(rest.next, None);
    let all = store.list_prefix("", None, 1000).await.unwrap();
    assert_eq!(all.objects.len(), 4);
    assert_eq!(all.next, None);
    assert!(
        store
            .list_prefix(&"a".repeat(4096), None, 1)
            .await
            .unwrap()
            .objects
            .is_empty()
    );
    for prefix in ["a\\b".to_string(), "a\0b".to_string(), "a".repeat(4097)] {
        assert!(matches!(
            store.list_prefix(&prefix, None, 1).await,
            Err(Error::Invalid(_))
        ));
    }
    let other =
        ObjectBufferStore::local(scratch("other-token-scope"), Duration::from_secs(5)).unwrap();
    assert!(matches!(
        other.list_prefix("a", Some(&token), 1).await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store.list_prefix("ab", Some(&token), 1).await,
        Err(Error::Invalid(_))
    ));
    for malformed in [
        "!".to_string(),
        URL_SAFE_NO_PAD.encode(b"{}"),
        "a".repeat(65537),
    ] {
        assert!(matches!(
            store.list_prefix("a", Some(&malformed), 1).await,
            Err(Error::Invalid(_))
        ));
    }
    // Construct valid JSON tokens exactly at and just over the token-size cap;
    // trailing JSON whitespace changes size without changing continuation data.
    let json = URL_SAFE_NO_PAD.decode(&token).unwrap();
    let mut padded = json.clone();
    padded.resize(49152, b' ');
    let at_limit = URL_SAFE_NO_PAD.encode(&padded);
    assert_eq!(at_limit.len(), 65536);
    assert_eq!(
        store.list_prefix("a", Some(&at_limit), 2).await.unwrap(),
        rest
    );
    padded.push(b' ');
    assert!(matches!(
        store
            .list_prefix("a", Some(&URL_SAFE_NO_PAD.encode(&padded)), 2)
            .await,
        Err(Error::Invalid("page token too large"))
    ));
    let mut unknown: serde_json::Value = serde_json::from_slice(&json).unwrap();
    unknown["unexpected"] = true.into();
    let unknown = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&unknown).unwrap());
    assert!(matches!(
        store.list_prefix("a", Some(&unknown), 1).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn local_io_failures_are_not_missing_objects() {
    let root = scratch("io-errors");
    let store = ObjectBufferStore::local(&root, Duration::from_secs(5)).unwrap();
    std::fs::write(root.join("file-parent"), b"not a directory").unwrap();
    std::fs::create_dir(root.join("directory-leaf")).unwrap();
    assert_eq!(
        store.get("file-parent/object").await,
        Err(Error::UnsafeLocalPath)
    );
    for key in ["file-parent/object", "directory-leaf"] {
        assert!(store.get(key).await.is_err(), "{key}");
        assert!(
            store.get_into(key, &mut std::io::sink()).await.is_err(),
            "{key}"
        );
        assert!(store.delete(key).await.is_err(), "{key}");
    }
    assert_eq!(store.get("missing/parent/object").await.unwrap(), None);
    assert_eq!(
        store
            .get_into("missing/parent/object", &mut std::io::sink())
            .await
            .unwrap(),
        None
    );
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        store.put_create_only("pinned", b"preserved").await.unwrap();
        let _pin = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(1)
            .open(root.join("pinned"))
            .unwrap();
        let Error::LocalIoDetail(detail) = store.delete("pinned").await.unwrap_err() else {
            panic!("missing delete I/O diagnostic");
        };
        assert_eq!(detail.operation, "delete-object");
        assert_eq!(detail.raw_os_error, Some(32));
        assert_eq!(detail.kind, std::io::Error::from_raw_os_error(32).kind());
        drop(_pin);
        assert_eq!(store.get("pinned").await.unwrap().unwrap(), b"preserved");
    }
}

#[test]
fn backend_configuration_acceptance_and_error_diagnostics() {
    assert!(matches!(
        ObjectBufferStore::s3(
            s3_builder("https://example.invalid"),
            Encryption::UnencryptedLocalTest,
            TransportSecurity::Required,
            Duration::from_secs(1)
        ),
        Err(Error::Invalid(_))
    ));
    for encryption in [Encryption::SseS3, Encryption::SseKms("key-id".into())] {
        let store = ObjectBufferStore::s3(
            s3_builder("https://example.invalid"),
            encryption.clone(),
            TransportSecurity::Required,
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(store.report().encryption, encryption);
        assert!(!store.report().plaintext_local_test);
        assert_eq!(
            store.report().failure_domain.host_loss,
            HostLoss::SurvivedByService
        );
        assert!(store.report().failure_domain.process_crash_survived);
        assert_eq!(store.local_sync_counts(), None);
        assert!(format!("{store:?}").contains("s3-compatible"));
    }
    let endpoint = "https://storage.googleapis.com";
    let auth = gcs_auth(endpoint);
    for encryption in [
        Encryption::GcsDefault,
        Encryption::GcsCmek("projects/p/locations/l/keyRings/r/cryptoKeys/k".into()),
    ] {
        let store = ObjectBufferStore::gcs(
            "p2-bucket",
            endpoint,
            auth.clone(),
            encryption.clone(),
            TransportSecurity::Required,
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(store.report().encryption, encryption);
        assert_eq!(store.report().backend, "gcs");
        assert!(!store.report().plaintext_local_test);
        assert!(store.report().failure_domain.process_crash_survived);
        assert_eq!(
            store.report().failure_domain.host_loss,
            HostLoss::SurvivedByService
        );
    }
    for encryption in [
        Encryption::GcsCmek(String::new()),
        Encryption::VolumeEncryption,
        Encryption::SseS3,
    ] {
        assert!(matches!(
            ObjectBufferStore::gcs(
                "p2-bucket",
                endpoint,
                auth.clone(),
                encryption,
                TransportSecurity::Required,
                Duration::from_secs(1)
            ),
            Err(Error::Invalid(_))
        ));
    }
    for bad_endpoint in ["http://storage.googleapis.com", "https://example.invalid"] {
        assert!(matches!(
            ObjectBufferStore::gcs(
                "p2-bucket",
                bad_endpoint,
                auth.clone(),
                Encryption::GcsDefault,
                TransportSecurity::Required,
                Duration::from_secs(1)
            ),
            Err(Error::Invalid(_))
        ));
    }
    assert!(ObjectBufferStore::local("relative-root", Duration::from_secs(1)).is_err());
    assert!(matches!(
        ObjectBufferStore::local(scratch("zero-timeout"), Duration::ZERO),
        Err(Error::Invalid(_))
    ));
    for error in [
        Error::Invalid("reason"),
        Error::Conflict,
        Error::UnknownPutOutcome,
        Error::Store,
        Error::UnsafeLocalPath,
        Error::LocalParentMissing,
        Error::LocalIo,
    ] {
        let message = error.to_string();
        assert!(!message.is_empty(), "{error:?}");
        assert!(message.starts_with("buffer ") || message.starts_with("invalid buffer "));
    }
}

fn stream_fixture(size: usize, value: u8) -> (PathBuf, skedaddle::buffer::backends::ObjectDigest) {
    let path = scratch("stream-input").join("source");
    let mut file = File::create(&path).unwrap();
    let buffer = [value; 16 * 1024];
    let mut hash = Sha256::new();
    let mut remaining = size;
    while remaining != 0 {
        let take = remaining.min(buffer.len());
        file.write_all(&buffer[..take]).unwrap();
        hash.update(&buffer[..take]);
        remaining -= take;
    }
    (
        path,
        skedaddle::buffer::backends::ObjectDigest {
            bytes: size as u64,
            sha256: hash.finalize().into(),
        },
    )
}

struct PatternSink {
    byte: u8,
    count: u64,
    max_write: usize,
}
impl Write for PatternSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        assert!(bytes.iter().all(|b| *b == self.byte));
        self.count += bytes.len() as u64;
        self.max_write = self.max_write.max(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn streaming_matrix(store: &ObjectBufferStore) {
    let prefix = format!("p3/stream-{}/", uuid::Uuid::new_v4());
    let key = format!("{prefix}object");
    let (input, expected) = stream_fixture(16 * 1024 * 1024 + 19, b's');
    let mut sink = PatternSink {
        byte: b's',
        count: 0,
        max_write: 0,
    };
    assert_eq!(store.get_into(&key, &mut sink).await.unwrap(), None);
    let mut input_file = File::open(&input).unwrap();
    input_file.seek(SeekFrom::End(0)).unwrap();
    assert_eq!(
        store.put_file_create_only(&key, input_file).await.unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        store.get_into(&key, &mut sink).await.unwrap(),
        Some(expected)
    );
    assert_eq!(sink.count, expected.bytes);
    assert!(sink.max_write <= 16 * 1024);
    assert_eq!(
        store
            .put_file_create_only(&key, File::open(&input).unwrap())
            .await
            .unwrap(),
        PutOutcome::Resolved
    );
    let (different, _) = stream_fixture(17, b'd');
    assert_eq!(
        store
            .put_file_create_only(&key, File::open(&different).unwrap())
            .await
            .unwrap_err(),
        Error::Conflict
    );
    assert_eq!(
        store.get_into(&key, &mut std::io::sink()).await.unwrap(),
        Some(expected)
    );
    // Both uploads may start before either destination exists. The final
    // publication precondition must still select exactly one winner.
    let race = format!("{prefix}race");
    let (left, right) = tokio::join!(
        store.put_file_create_only(&race, File::open(&input).unwrap()),
        store.put_file_create_only(&race, File::open(&different).unwrap())
    );
    assert!(matches!(
        (left, right),
        (Ok(PutOutcome::Created), Err(Error::Conflict))
            | (Err(Error::Conflict), Ok(PutOutcome::Created))
    ));
    store.delete(&race).await.unwrap();
    let empty = format!("{prefix}empty");
    let (empty_input, empty_digest) = stream_fixture(0, 0);
    assert_eq!(
        store
            .put_file_create_only(&empty, File::open(&empty_input).unwrap())
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        store.get_into(&empty, &mut std::io::sink()).await.unwrap(),
        Some(empty_digest)
    );
    store.delete(&empty).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(
        store
            .list_prefix(&prefix, None, 100)
            .await
            .unwrap()
            .objects
            .is_empty()
    );
    eprintln!(
        "streaming {}: {} bytes, full SHA-256 and byte pattern verified, max sink write {}",
        store.report().backend,
        expected.bytes,
        sink.max_write
    );
}

#[tokio::test]
async fn local_streaming_create_only_and_digest() {
    let root = scratch("stream-local");
    let store = ObjectBufferStore::local(&root, Duration::from_secs(30)).unwrap();
    streaming_matrix(&store).await;
    assert!(
        !walk(&root)
            .iter()
            .any(|p| p.file_name().unwrap().to_string_lossy().contains('#'))
    );
    let (files, directories) = store.local_sync_counts().unwrap();
    assert!(files >= 2 && directories >= 2);
}

#[tokio::test]
#[ignore = "Real multipart/resumable and range downloads require harness/run.ps1 p3"]
async fn streaming_backend_matrix() {
    let cfg = config();
    streaming_matrix(&s3(cfg["minio"].as_str().unwrap(), Duration::from_secs(90))).await;
    streaming_matrix(&gcs(cfg["gcs"].as_str().unwrap(), Encryption::GcsDefault)).await;
}

#[tokio::test]
#[ignore = "Accepted conditional multipart completion with delayed response requires harness/run.ps1 p3"]
async fn streaming_unknown_completion_resolved_by_digest() {
    let cfg = config();
    let client = reqwest::Client::new();
    let toxics = format!(
        "{}/proxies/minio/toxics",
        cfg["toxiproxy"].as_str().unwrap()
    );
    client.post(&toxics).json(&serde_json::json!({"name":"p3-stream-delay","type":"latency","stream":"downstream","toxicity":1,"attributes":{"latency":550,"jitter":0}}))
        .send().await.unwrap().error_for_status().unwrap();
    let key = format!("p3/stream-unknown-{}/object", uuid::Uuid::new_v4());
    let (input, expected) = stream_fixture(43, b'u');
    let direct = s3(cfg["minio"].as_str().unwrap(), Duration::from_secs(10));
    let delayed = s3(
        cfg["proxy_minio"].as_str().unwrap(),
        Duration::from_millis(1500),
    );
    let put = tokio::spawn({
        let key = key.clone();
        async move {
            delayed
                .put_file_create_only(&key, File::open(input).unwrap())
                .await
        }
    });
    tokio::time::timeout(Duration::from_millis(1450), async {
        loop {
            if direct.get_into(&key, &mut std::io::sink()).await.unwrap() == Some(expected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real MinIO must accept completion before the public timeout");
    let result = put.await.unwrap();
    client
        .delete(format!("{toxics}/p3-stream-delay"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(result.unwrap(), PutOutcome::Resolved);
    assert_eq!(
        direct.get_into(&key, &mut std::io::sink()).await.unwrap(),
        Some(expected)
    );
    direct.delete(&key).await.unwrap();
    let unavailable = s3(
        cfg["proxy_errors"].as_str().unwrap(),
        Duration::from_secs(1),
    );
    let (input, _) = stream_fixture(43, b'u');
    assert_eq!(
        unavailable
            .put_file_create_only("p3/stream-never-known", File::open(input).unwrap())
            .await
            .unwrap_err(),
        Error::UnknownPutOutcome
    );
}

#[test]
fn startup_reclaims_only_identified_unlocked_scratch() {
    // Intentionally retained unknown scratch belongs outside the harness's
    // leak-audited product roots. It remains under this task's explicit TEMP.
    let root = PathBuf::from(std::env::var_os("TEMP").unwrap())
        .join(format!("p3-reclamation-evidence-{}", uuid::Uuid::new_v4()));
    assert!(root.starts_with("J:/gigglesdb-work"));
    std::fs::create_dir_all(&root).unwrap();
    let stage = root.join(format!("#skedaddle-stage-{}", uuid::Uuid::new_v4()));
    let txn = root.join(format!("txn-{}.skedaddle-scratch", uuid::Uuid::new_v4()));
    let active = root.join(format!("#skedaddle-stage-{}", uuid::Uuid::new_v4()));
    let unknown = root.join("#123");
    let malformed = root.join("txn-user-file.skedaddle-scratch");
    for path in [&stage, &txn, &unknown, &malformed, &active] {
        std::fs::write(path, b"fixture").unwrap();
    }
    let owner = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&active)
        .unwrap();
    owner.try_lock().unwrap();
    let store = ObjectBufferStore::local(&root, Duration::from_secs(3)).unwrap();
    let report = store.startup_reclamation();
    assert_eq!(report.removed.len(), 2);
    assert!(report.removed.contains(&stage) && report.removed.contains(&txn));
    assert!(
        report.retained.contains(&active)
            && report.retained.contains(&unknown)
            && report.retained.contains(&malformed)
    );
    assert!(active.exists() && unknown.exists() && malformed.exists());
    assert_eq!(
        futures::executor::block_on(store.put_create_only(
            &format!("txn-{}.skedaddle-scratch", uuid::Uuid::new_v4()),
            b"reserved"
        )),
        Err(Error::UnsafeLocalPath)
    );
    drop(owner);
    let report = skedaddle::buffer::backends::reclaim_scratch(&root).unwrap();
    assert_eq!(report.removed, vec![active]);
    assert_eq!(std::fs::read(unknown).unwrap(), b"fixture");
}
fn scratch(name: &str) -> PathBuf {
    let root = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT")
            .expect("set SKEDADDLE_TEST_ROOT under J:/gigglesdb-work"),
    );
    assert!(root.starts_with("J:/gigglesdb-work"));
    let path = root.join(format!("p3-{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn config() -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("run harness/run.ps1 p3"))
            .unwrap(),
    )
    .unwrap()
}
fn s3_builder(endpoint: &str) -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_bucket_name("p2-bucket")
        .with_region("us-east-1")
        .with_access_key_id("skedaddle")
        .with_secret_access_key("skedaddle-secret")
        .with_endpoint(endpoint)
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
}
fn s3(endpoint: &str, timeout: Duration) -> ObjectBufferStore {
    ObjectBufferStore::s3(
        s3_builder(endpoint),
        Encryption::UnencryptedLocalTest,
        LOCAL_TEST,
        timeout,
    )
    .unwrap()
}
fn gcs(endpoint: &str, encryption: Encryption) -> ObjectBufferStore {
    let auth = gcs_auth(endpoint);
    ObjectBufferStore::gcs(
        "p2-bucket",
        endpoint,
        auth,
        encryption,
        LOCAL_TEST,
        Duration::from_secs(30),
    )
    .unwrap()
}
fn gcs_auth(endpoint: &str) -> object_store::gcp::GoogleCloudStorage {
    object_store::gcp::GoogleCloudStorageBuilder::new()
        .with_bucket_name("p2-bucket")
        .with_service_account_key(
            r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#,
        )
        .with_skip_signature(true)
        .with_base_url(endpoint)
        .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
        .unwrap()
}
async fn matrix(store: &dyn BufferStore, large: bool) {
    let prefix = format!("p3/{}/", uuid::Uuid::new_v4());
    let key = format!("{prefix}nested/one");
    assert_eq!(store.get(&key).await.unwrap(), None);
    store.delete(&key).await.unwrap();
    assert_eq!(
        store.put_create_only(&key, b"first").await.unwrap(),
        PutOutcome::Created
    );
    assert!(matches!(
        store.put_create_only(&key, b"first").await.unwrap(),
        PutOutcome::Resolved
    ));
    assert_eq!(
        store.put_create_only(&key, b"other").await.unwrap_err(),
        Error::Conflict
    );
    assert_eq!(store.get(&key).await.unwrap().unwrap(), b"first");
    let mut expected = vec![key.clone()];
    for index in 0..7 {
        let k = format!("{prefix}item-{index}");
        store.put_create_only(&k, &[index]).await.unwrap();
        expected.push(k);
    }
    // A literal prefix includes partial names, and does not include neighbors.
    let partial = store
        .list_prefix(&format!("{prefix}item-"), None, 2)
        .await
        .unwrap();
    assert_eq!(partial.objects.len(), 2);
    assert!(
        partial
            .objects
            .iter()
            .all(|o| o.key.starts_with(&format!("{prefix}item-")))
    );
    let mut found = Vec::new();
    let mut token = None;
    let mut pages = 0;
    loop {
        let page = store
            .list_prefix(&prefix, token.as_deref(), 2)
            .await
            .unwrap();
        assert!(page.objects.len() <= 2);
        found.extend(page.objects.into_iter().map(|o| o.key));
        pages += 1;
        token = page.next;
        if token.is_none() {
            break;
        }
        assert!(pages < 10);
    }
    assert!(pages >= 4);
    found.sort();
    expected.sort();
    assert_eq!(found, expected);
    let token = partial.next.unwrap();
    assert!(
        store
            .list_prefix("other-prefix", Some(&token), 2)
            .await
            .is_err()
    );
    assert!(store.list_prefix(&prefix, None, 0).await.is_err());
    assert!(store.list_prefix(&prefix, None, 1001).await.is_err());
    for size in if large {
        vec![0, 1, 1024 * 1024, 100_000_000]
    } else {
        vec![0, 1, 1024 * 1024]
    } {
        let content = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let key = format!("{prefix}large-{size}");
        store
            .put_create_only(&key, &content)
            .await
            .unwrap_or_else(|error| panic!("{} {size}-byte put: {error}", store.report().backend));
        let read = store.get(&key).await.unwrap().unwrap();
        assert_eq!(read, content);
        store.delete(&key).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), None);
    }
    for key in expected {
        store.delete(&key).await.unwrap();
        store.delete(&key).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), None);
    }
    assert!(
        store
            .list_prefix(&prefix, None, 2)
            .await
            .unwrap()
            .objects
            .is_empty()
    );
}

#[tokio::test]
async fn backend_matrix_local() {
    let root = scratch("matrix");
    let store = ObjectBufferStore::local(&root, Duration::from_secs(5)).unwrap();
    matrix(&store, true).await;
    let report = store.report();
    assert_eq!(report.encryption, Encryption::VolumeEncryption);
    assert_eq!(
        report.failure_domain.host_loss,
        HostLoss::RecoveryRequiredThenResnapshot
    );
    let (files, directories) = store.local_sync_counts().unwrap();
    assert!(files >= 12);
    assert!(directories > files);
    assert!(
        !walk(&root)
            .iter()
            .any(|p| p.to_string_lossy().contains('#')),
        "product must clean only its own staging files"
    );
}
fn walk(root: &std::path::Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    for e in std::fs::read_dir(root).unwrap() {
        let e = e.unwrap();
        result.push(e.path());
        if e.file_type().unwrap().is_dir() {
            result.extend(walk(&e.path()));
        }
    }
    result
}

#[tokio::test]
#[ignore = "Docker backend matrix requires MinIO and fake-gcs-server: harness/run.ps1 p3"]
async fn backend_matrix() {
    let cfg = config();
    for store in [
        s3(cfg["minio"].as_str().unwrap(), Duration::from_secs(30)),
        gcs(cfg["gcs"].as_str().unwrap(), Encryption::GcsDefault),
    ] {
        matrix(&store, true).await;
        assert_eq!(
            store.report().failure_domain.host_loss,
            HostLoss::SurvivedByService
        );
    }
}

#[tokio::test]
async fn local_path_confinement_and_cleanup() {
    let root = scratch("confined");
    let outside = scratch("outside");
    std::fs::write(outside.join("sentinel"), b"untouched").unwrap();
    let store = ObjectBufferStore::local(&root, Duration::from_secs(3)).unwrap();
    for key in [
        "../sentinel",
        "a/../../sentinel",
        "/absolute",
        "a\\sentinel",
        "x:stream",
        "a//b",
        "a/./b",
        "NUL",
        "name.",
        "object#1",
    ] {
        assert!(store.put_create_only(key, b"bad").await.is_err(), "{key}");
        assert!(store.get(key).await.is_err());
        assert!(store.delete(key).await.is_err());
    }
    let key = "owned/object";
    store.put_create_only(key, b"one").await.unwrap();
    for _ in 0..5 {
        assert_eq!(
            store
                .put_create_only(key, b"conflicting")
                .await
                .unwrap_err(),
            Error::Conflict
        );
    }
    assert_eq!(walk(&root).iter().filter(|p| p.is_file()).count(), 1);
    store.delete(key).await.unwrap();
    assert_eq!(
        std::fs::read(outside.join("sentinel")).unwrap(),
        b"untouched"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn local_refuses_junctions_and_symlinks() {
    use std::os::windows::fs::{symlink_dir, symlink_file};
    let outside = scratch("link-outside");
    std::fs::write(outside.join("sentinel"), b"safe").unwrap();
    let root = scratch("junction");
    // mklink creates a fixture; it never removes or moves host files.
    let status = std::process::Command::new("cmd")
        .args(["/c", "mklink", "/J"])
        .arg(root.join("junction").to_string_lossy().replace('/', "\\"))
        .arg(outside.to_string_lossy().replace('/', "\\"))
        .status()
        .unwrap();
    assert!(status.success());
    let store = ObjectBufferStore::local(&root, Duration::from_secs(3)).unwrap();
    assert!(
        store
            .put_create_only("junction/sentinel", b"bad")
            .await
            .is_err()
    );
    assert!(store.get("junction/sentinel").await.is_err());
    assert!(store.delete("junction/sentinel").await.is_err());
    assert!(store.list_prefix("", None, 10).await.is_err());
    assert!(ObjectBufferStore::local(root.join("junction"), Duration::from_secs(3)).is_err());
    let links = scratch("symlinks");
    if let Err(error) = symlink_dir(&outside, links.join("dir-link")) {
        if error.raw_os_error() == Some(1314) {
            eprintln!(
                "SYMLINK SUBCASE NOT RUN: Windows denied symlink creation (1314); enable Developer Mode/SeCreateSymbolicLinkPrivilege and rerun local_refuses_junctions_and_symlinks. Real junction rejection passed and exercises the same reparse-point check."
            );
            assert_eq!(std::fs::read(outside.join("sentinel")).unwrap(), b"safe");
            return;
        }
        panic!("symlink fixture failed: {error}");
    }
    symlink_file(outside.join("sentinel"), links.join("file-link")).unwrap();
    let store = ObjectBufferStore::local(&links, Duration::from_secs(3)).unwrap();
    for key in ["dir-link/sentinel", "file-link"] {
        assert!(store.get(key).await.is_err());
        assert!(store.delete(key).await.is_err());
        assert!(store.put_create_only(key, b"bad").await.is_err());
    }
    assert!(store.list_prefix("", None, 10).await.is_err());
    assert_eq!(std::fs::read(outside.join("sentinel")).unwrap(), b"safe");
}

#[test]
fn configuration_and_diagnostics() {
    assert!(
        ObjectBufferStore::s3(
            s3_builder("http://127.0.0.1:1"),
            Encryption::SseS3,
            TransportSecurity::Required,
            Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(
        ObjectBufferStore::s3(
            s3_builder("http://127.0.0.1:1"),
            Encryption::UnencryptedLocalTest,
            TransportSecurity::Required,
            Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(
        ObjectBufferStore::s3(
            s3_builder("http://127.0.0.1:1"),
            Encryption::SseKms(String::new()),
            LOCAL_TEST,
            Duration::from_secs(1)
        )
        .is_err()
    );
    let store = s3("http://127.0.0.1:1", Duration::from_secs(1));
    let diagnostic = format!("{store:?}");
    assert!(!diagnostic.contains("skedaddle-secret"));
    assert!(!diagnostic.contains("127.0.0.1"));
    assert!(store.report().plaintext_local_test);
}

#[tokio::test]
#[ignore = "Real accepted PUT plus Toxiproxy downstream timeout requires harness/run.ps1 p3"]
async fn unknown_put_outcome_resolved() {
    let cfg = config();
    let client = reqwest::Client::new();
    let toxics = format!(
        "{}/proxies/minio/toxics",
        cfg["toxiproxy"].as_str().unwrap()
    );
    client.post(&toxics).json(&serde_json::json!({"name":"p3-delay","type":"latency","stream":"downstream","toxicity":1,"attributes":{"latency":1500,"jitter":0}})).send().await.unwrap().error_for_status().unwrap();
    let key = format!("p3/unknown-{}/object", uuid::Uuid::new_v4());
    let content = b"server accepted, response delayed";
    let store = s3(
        cfg["proxy_minio"].as_str().unwrap(),
        Duration::from_millis(700),
    );
    let direct = s3(cfg["minio"].as_str().unwrap(), Duration::from_secs(3));
    let put = tokio::spawn({
        let key = key.clone();
        async move { store.put_create_only(&key, content).await }
    });
    // Prove acceptance through the real server, independently of the delayed response.
    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if direct.get(&key).await.unwrap().as_deref() == Some(content.as_slice()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("MinIO must accept the PUT before the caller times out");
    tokio::time::sleep(Duration::from_millis(720)).await;
    client
        .delete(format!("{toxics}/p3-delay"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let result = put.await.unwrap();
    assert_eq!(result.unwrap(), PutOutcome::Resolved);
    assert_eq!(direct.get(&key).await.unwrap().unwrap(), content);
    assert_eq!(
        direct
            .put_create_only(&key, b"different")
            .await
            .unwrap_err(),
        Error::Conflict
    );
    direct.delete(&key).await.unwrap();
    // If neither response nor reread is available, uncertainty remains explicit.
    let unavailable = s3(
        cfg["proxy_errors"].as_str().unwrap(),
        Duration::from_secs(1),
    );
    assert_eq!(
        unavailable
            .put_create_only("p3/never-known", b"data")
            .await
            .unwrap_err(),
        Error::UnknownPutOutcome
    );
}

#[tokio::test]
#[ignore = "Encryption request capture and real MinIO/GCS checks require harness/run.ps1 p3"]
async fn encryption_passthrough() {
    let cfg = config();
    for encryption in [
        Encryption::SseS3,
        Encryption::SseKms("p3-test-kms-key".into()),
    ] {
        let store = ObjectBufferStore::s3(
            s3_builder(cfg["minio"].as_str().unwrap()),
            encryption.clone(),
            LOCAL_TEST,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(store.report().encryption, encryption);
        let key = format!("p3/encrypted-{}", uuid::Uuid::new_v4());
        match store.put_create_only(&key, b"encrypted").await {
            Ok(_) => {
                assert_eq!(store.get(&key).await.unwrap().unwrap(), b"encrypted");
                store.delete(&key).await.unwrap();
            }
            Err(Error::UnknownPutOutcome) => eprintln!(
                "MinIO fixture rejected the encrypted write (HTTP endpoint, no configured KMS); actual upload headers are asserted by the request-capture fault injector"
            ),
            Err(error) => panic!("unexpected encryption result: {error}"),
        }
    }
    // A response-only local fault injector records headers/query and rejects
    // writes. This is only encryption/fault injection; matrix uses real servers.
    let capture = cfg["capture"].as_str().unwrap();
    for encryption in [
        Encryption::SseS3,
        Encryption::SseKms("p3-test-kms-key".into()),
    ] {
        let store = ObjectBufferStore::s3(
            s3_builder(capture),
            encryption.clone(),
            LOCAL_TEST,
            Duration::from_secs(3),
        )
        .unwrap();
        assert_eq!(
            store
                .put_create_only("p3/capture", b"secret-payload")
                .await
                .unwrap_err(),
            Error::UnknownPutOutcome
        );
        let records: serde_json::Value = reqwest::get(format!("{capture}/capture"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let put = records
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .find(|r| r["method"] == "PUT")
            .unwrap();
        assert_eq!(
            put["headers"]["x-amz-server-side-encryption"],
            if encryption == Encryption::SseS3 {
                "AES256"
            } else {
                "aws:kms"
            }
        );
        if matches!(encryption, Encryption::SseKms(_)) {
            assert_eq!(
                put["headers"]["x-amz-server-side-encryption-aws-kms-key-id"],
                "p3-test-kms-key"
            );
        }
    }
    let kms = "projects/p/locations/l/keyRings/r/cryptoKeys/k";
    let store = gcs(capture, Encryption::GcsCmek(kms.into()));
    assert_eq!(store.report().encryption, Encryption::GcsCmek(kms.into()));
    assert_eq!(
        store.put_create_only("p3/cmek", b"data").await.unwrap_err(),
        Error::UnknownPutOutcome
    );
    let records: serde_json::Value = reqwest::get(format!("{capture}/capture"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let post = records
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|r| r["method"] == "POST")
        .unwrap();
    assert_eq!(post["query"]["kmsKeyName"][0], kms);
    assert_eq!(post["query"]["ifGenerationMatch"][0], "0");
    // Emulator is real JSON adapter coverage; it does not actually encrypt.
    let emulator = gcs(
        cfg["gcs"].as_str().unwrap(),
        Encryption::GcsCmek(kms.into()),
    );
    let key = format!("p3/cmek-{}", uuid::Uuid::new_v4());
    emulator
        .put_create_only(&key, b"cmek request")
        .await
        .unwrap();
    assert_eq!(emulator.get(&key).await.unwrap().unwrap(), b"cmek request");
    emulator.delete(&key).await.unwrap();
}

// This forwards real emulator traffic. It fabricates no object contents.
// Connection-close framing keeps the fault injector independent of a server crate.
struct RequestProxy {
    endpoint: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for RequestProxy {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl RequestProxy {
    async fn start(upstream: &str, delay_write: bool, reject_reads: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let upstream = upstream.trim_end_matches('/').to_owned();
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let server = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap();
            let mut pending = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let client = client.clone();
                        let upstream = upstream.clone();
                        let recorded = recorded.clone();
                        pending.spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut input = Vec::new();
                            let header_end = loop {
                                let mut buf = [0u8; 4096];
                                let n = socket.read(&mut buf).await.unwrap();
                                if n == 0 { return; }
                                input.extend_from_slice(&buf[..n]);
                                assert!(input.len() < 1024 * 1024, "proxy is for small fault/GET requests only");
                                if let Some(n) = input.windows(4).position(|w| w == b"\r\n\r\n") { break n + 4; }
                            };
                            let header = std::str::from_utf8(&input[..header_end]).unwrap().to_owned();
                            let mut lines = header.split("\r\n");
                            let mut first = lines.next().unwrap().split_whitespace();
                            let method = first.next().unwrap().to_owned();
                            let target = first.next().unwrap().to_owned();
                            recorded.lock().unwrap().push((method.clone(), target.clone()));
                            let mut request = client.request(reqwest::Method::from_bytes(method.as_bytes()).unwrap(), format!("{upstream}{target}"));
                            let mut length = 0;
                            for line in lines.filter(|l| !l.is_empty()) {
                                let (name, value) = line.split_once(':').unwrap();
                                let value = value.trim();
                                if name.eq_ignore_ascii_case("content-length") { length = value.parse::<usize>().unwrap(); }
                                assert!(!name.eq_ignore_ascii_case("transfer-encoding"), "unexpected chunked request");
                                if !name.eq_ignore_ascii_case("connection") { request = request.header(name, value); }
                            }
                            assert!(length < 1024 * 1024);
                            while input.len() - header_end < length {
                                let mut buf = [0u8; 4096];
                                let n = socket.read(&mut buf).await.unwrap();
                                assert_ne!(n, 0);
                                input.extend_from_slice(&buf[..n]);
                            }
                            let (status, headers, body) = if reject_reads && method == "GET" {
                                (503, String::new(), Vec::new())
                            } else {
                                let response = request.body(input[header_end..header_end + length].to_vec()).send().await.unwrap();
                                let status = response.status().as_u16();
                                let mut headers = String::new();
                                for (name, value) in response.headers() {
                                    if !matches!(name.as_str(), "connection" | "content-length" | "transfer-encoding") {
                                        headers.push_str(&format!("{}: {}\r\n", name, value.to_str().unwrap()));
                                    }
                                }
                                let body = response.bytes().await.unwrap().to_vec();
                                // Delay only after the real upstream has accepted the write.
                                if delay_write && matches!(method.as_str(), "POST" | "PUT") && (200..300).contains(&status) {
                                    tokio::time::sleep(Duration::from_secs(3)).await;
                                }
                                (status, headers, body)
                            };
                            let head = format!("HTTP/1.1 {status} Proxy\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                            if socket.write_all(head.as_bytes()).await.is_ok() { let _ = socket.write_all(&body).await; }
                        });
                    }
                    result = pending.join_next(), if !pending.is_empty() => { result.unwrap().unwrap(); }
                }
            }
        });
        Self {
            endpoint,
            requests,
            server,
        }
    }
}

#[tokio::test]
#[ignore = "GCS accepted PUT response loss requires fake-gcs via harness/run.ps1 p3; plain cargo test explicitly skips infrastructure"]
async fn gcs_unknown_put_outcome_resolved() {
    let cfg = config();
    let upstream = cfg["gcs"].as_str().unwrap();
    let direct = gcs(upstream, Encryption::GcsDefault);
    for reject_reads in [false, true] {
        let proxy = RequestProxy::start(upstream, true, reject_reads).await;
        let store = ObjectBufferStore::gcs(
            "p2-bucket",
            &proxy.endpoint,
            gcs_auth(&proxy.endpoint),
            Encryption::GcsDefault,
            LOCAL_TEST,
            Duration::from_secs(1),
        )
        .unwrap();
        let key = format!("p3/gcs-unknown-{}/object", uuid::Uuid::new_v4());
        let content = b"fake-gcs accepted, response lost";
        let put = tokio::spawn({
            let key = key.clone();
            async move { store.put_create_only(&key, content).await }
        });
        tokio::time::timeout(Duration::from_millis(800), async {
            loop {
                if direct.get(&key).await.unwrap().as_deref() == Some(content.as_slice()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fake-gcs must accept the actual PUT before caller timeout");
        // Ordinary Created is explicitly insufficient: acceptance was proved
        // independently, but the successful write response never reaches caller.
        let result = put.await.unwrap();
        if reject_reads {
            assert_eq!(result, Err(Error::UnknownPutOutcome));
        } else {
            assert_eq!(result, Ok(PutOutcome::Resolved));
        }
        assert_eq!(direct.get(&key).await.unwrap().unwrap(), content);
        assert_eq!(
            direct.put_create_only(&key, b"different").await,
            Err(Error::Conflict)
        );
        assert!(
            proxy
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|(method, _)| method == "GET"),
            "must attempt read-after-timeout resolution"
        );
        direct.delete(&key).await.unwrap();
    }
}
