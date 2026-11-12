//! Immutable buffer storage. Local storage relies on **volume encryption**;
//! losing that volume requires recovery and a resnapshot. Cloud contracts assume
//! the configured service itself supplies durable storage (a single-host MinIO
//! deployment cannot acquire host-loss tolerance merely by using this API).
//!
//! Callers supply runtime credential providers/builders, never credential values
//! in reports. Errors deliberately exclude upstream bodies, URLs and credentials.
use crate::fsconfine::Dir;
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use object_store::{
    GetOptions, ObjectStore, ObjectStoreExt, PutMode,
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut},
    gcp::GoogleCloudStorage,
    list::{PaginatedListOptions, PaginatedListStore},
    multipart::MultipartStore,
    path::Path,
    signer::{SignedUrlOptions, Signer},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Conflict,
    /// The reread failed or found no object. The caller must not acknowledge it.
    UnknownPutOutcome,
    /// The put remains unacknowledgeable; retain its first local I/O failure.
    UnknownPutOutcomeLocal(LocalIoDiagnostic),
    Store,
    UnsafeLocalPath,
    LocalParentMissing,
    LocalIo,
    /// Only structured, redacted context is retained; never the upstream message.
    LocalIoDetail(LocalIoDiagnostic),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalIoDiagnostic {
    pub operation: &'static str,
    pub path_fingerprint: u64,
    pub kind: std::io::ErrorKind,
    pub raw_os_error: Option<i32>,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid buffer configuration: {reason}"),
            Self::Conflict => f.write_str("buffer create-only content conflict"),
            Self::UnknownPutOutcome => {
                f.write_str("buffer put outcome remains unknown; do not acknowledge")
            }
            Self::UnknownPutOutcomeLocal(detail) => write!(
                f,
                "{}; {}",
                Self::UnknownPutOutcome,
                Self::LocalIoDetail(*detail)
            ),
            Self::Store => {
                f.write_str("buffer object-store operation failed (diagnostic redacted)")
            }
            Self::UnsafeLocalPath => {
                f.write_str("buffer local path contains an unsafe component or reparse point")
            }
            Self::LocalParentMissing => f.write_str("buffer local parent directory is absent"),
            Self::LocalIo => f.write_str("buffer local I/O or durability operation failed"),
            Self::LocalIoDetail(d) => write!(
                f,
                "buffer local I/O or durability operation failed; operation={}; path-sha256-prefix={:016x}; kind={:?}; os={:?}",
                d.operation, d.path_fingerprint, d.kind, d.raw_os_error
            ),
        }
    }
}
impl std::error::Error for Error {}
impl Error {
    fn unknown_put(self) -> Self {
        match self {
            Self::LocalIoDetail(detail) | Self::UnknownPutOutcomeLocal(detail) => {
                Self::UnknownPutOutcomeLocal(detail)
            }
            _ => Self::UnknownPutOutcome,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "key_id", rename_all = "kebab-case")]
pub enum Encryption {
    SseS3,
    SseKms(String),
    GcsDefault,
    GcsCmek(String),
    VolumeEncryption,
    /// Only for explicit local-test endpoints without a KMS service.
    UnencryptedLocalTest,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HostLoss {
    SurvivedByService,
    RecoveryRequiredThenResnapshot,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureDomain {
    pub process_crash_survived: bool,
    pub host_loss: HostLoss,
    pub assumption: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendReport {
    pub backend: String,
    pub encryption: Encryption,
    pub failure_domain: FailureDomain,
    pub plaintext_local_test: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportSecurity {
    Required,
    /// Explicit equivalent of definition tls: disable-for-local-test.
    DisableForLocalTest,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    /// A failed/expired write was resolved by reading the identical object.
    Resolved,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferObject {
    pub key: String,
    pub bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<BufferObject>,
    /// Opaque, bound to this backend and prefix; not a snapshot of concurrent GC.
    pub next: Option<String>,
}
/// SHA-256 of the complete object (not a multipart ETag).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectDigest {
    pub bytes: u64,
    pub sha256: [u8; 32],
}

/// Startup audit. Only well-formed product scratch names with an obtainable
/// exclusive file lock are removed; unknown files and active owners are retained.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScratchReclamation {
    pub removed: Vec<PathBuf>,
    pub retained: Vec<PathBuf>,
}

const STREAM_BUFFER: usize = 16 * 1024;
// One maximum-sized transaction, the maximum v1 header, and framing. This
// also bounds S3's retained completion metadata independently of caller input.
const MAX_STREAM_BYTES: u64 = 1_000_000_000 + 64 * 1024 + 128;

#[async_trait]
pub trait BufferStore: Send + Sync {
    fn report(&self) -> BackendReport;
    async fn put_create_only(&self, key: &str, content: &[u8]) -> Result<PutOutcome>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    /// Uploads a private, immutable file from offset zero. The handle is owned
    /// through completion and uncertain-outcome resolution, including cancellation.
    /// Implementations must fail rather than silently fall back to materialization.
    async fn put_file_create_only(&self, _key: &str, _file: File) -> Result<PutOutcome> {
        Err(Error::Invalid("streaming upload unsupported by this store"))
    }
    /// Appends a verified object to `output` with bounded buffering. Output is
    /// provisional until this succeeds; a failure may leave a partial prefix.
    async fn get_into(
        &self,
        _key: &str,
        _output: &mut (dyn Write + Send),
    ) -> Result<Option<ObjectDigest>> {
        Err(Error::Invalid(
            "streaming download unsupported by this store",
        ))
    }
    /// Literal object-name prefix, including partial path components. 1..=1000.
    async fn list_prefix(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> Result<ListPage>;
    /// Idempotent; callers are responsible for verified retention/GC eligibility.
    async fn delete(&self, key: &str) -> Result<()>;
}

#[derive(Clone)]
enum Backend {
    S3(AmazonS3),
    Gcs(Box<GcsBuffer>),
    Local(Arc<LocalBuffer>),
}
/// Concrete object_store-backed implementation. The file/`get_into` APIs use
/// fixed buffers; the byte-vector compatibility APIs still materialize objects.
#[derive(Clone)]
pub struct ObjectBufferStore {
    backend: Backend,
    report: BackendReport,
    timeout: Duration,
    token_scope: String,
    reclamation: ScratchReclamation,
    stream_client: reqwest::Client,
}
impl fmt::Debug for ObjectBufferStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectBufferStore")
            .field("report", &self.report)
            .finish_non_exhaustive()
    }
}
impl ObjectBufferStore {
    pub fn s3(
        builder: AmazonS3Builder,
        encryption: Encryption,
        security: TransportSecurity,
        timeout: Duration,
    ) -> Result<Self> {
        let builder = match &encryption {
            Encryption::SseS3 => builder.with_config(
                "aws_server_side_encryption"
                    .parse()
                    .map_err(|_| Error::Store)?,
                "AES256",
            ),
            Encryption::SseKms(key) if !key.is_empty() => builder.with_sse_kms_encryption(key),
            Encryption::UnencryptedLocalTest
                if security == TransportSecurity::DisableForLocalTest =>
            {
                builder
            }
            _ => {
                return Err(Error::Invalid(
                    "S3 requires SSE-S3 or a nonempty SSE-KMS key id",
                ));
            }
        };
        if security == TransportSecurity::Required
            && builder
                .get_config_value(&AmazonS3ConfigKey::Endpoint)
                .is_some_and(|e| !e.starts_with("https://"))
        {
            return Err(Error::Invalid("S3 endpoint requires HTTPS"));
        }
        let store = builder
            .with_allow_http(security == TransportSecurity::DisableForLocalTest)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map_err(|_| Error::Store)?;
        Self::new(
            Backend::S3(store),
            "s3-compatible",
            encryption,
            security,
            timeout,
        )
    }
    pub fn gcs(
        bucket: &str,
        endpoint: &str,
        auth: GoogleCloudStorage,
        encryption: Encryption,
        security: TransportSecurity,
        timeout: Duration,
    ) -> Result<Self> {
        if !matches!(&encryption, Encryption::GcsDefault | Encryption::GcsCmek(_))
            || matches!(&encryption, Encryption::GcsCmek(k) if k.is_empty())
        {
            return Err(Error::Invalid(
                "GCS requires default encryption or a nonempty CMEK name",
            ));
        }
        let adapter = if security == TransportSecurity::DisableForLocalTest {
            crate::coordinator::gcs::GcsJsonStore::for_emulator(bucket, endpoint, auth.clone())
        } else {
            // The public production adapter is intentionally bound to Google's endpoint.
            if endpoint != "https://storage.googleapis.com" {
                return Err(Error::Invalid(
                    "production GCS endpoint must be storage.googleapis.com",
                ));
            }
            crate::coordinator::gcs::GcsJsonStore::new(bucket, auth.clone())
        }
        .map_err(|_| Error::Invalid("invalid GCS endpoint or bucket"))?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|_| Error::Store)?;
        let gcs = GcsBuffer {
            adapter,
            auth,
            bucket: bucket.into(),
            endpoint: reqwest::Url::parse(endpoint).map_err(|_| Error::Store)?,
            client,
        };
        Self::new(
            Backend::Gcs(Box::new(gcs)),
            "gcs",
            encryption,
            security,
            timeout,
        )
    }
    /// The caller provides an existing dedicated root. Symlinks, Windows reparse
    /// points (including junctions), alternate streams and traversal are refused.
    /// The root is opened once. All accesses use retained directory handles;
    /// Windows handles also deny ancestor rename/delete.
    pub fn local(root: impl AsRef<FsPath>, timeout: Duration) -> Result<Self> {
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(Error::UnsafeLocalPath);
        }
        let confined = Dir::open_root(root)
            .map_err(|e| local_error("open-root", &root.to_string_lossy(), e))?;
        let local = LocalBuffer {
            root: confined,
            root_path: root.to_path_buf(),
            file_syncs: AtomicU64::new(0),
            directory_syncs: AtomicU64::new(0),
            #[cfg(all(test, windows))]
            read_wait: None,
        };
        let _guard = local.parents(None, false)?;
        let mut reclamation = ScratchReclamation::default();
        reclaim_directory(&local.root, root, &mut reclamation)?;
        let mut store = Self::new(
            Backend::Local(Arc::new(local)),
            "local-fs",
            Encryption::VolumeEncryption,
            TransportSecurity::Required,
            timeout,
        )?;
        store.reclamation = reclamation;
        Ok(store)
    }
    fn new(
        backend: Backend,
        name: &str,
        encryption: Encryption,
        security: TransportSecurity,
        timeout: Duration,
    ) -> Result<Self> {
        if timeout.is_zero() {
            return Err(Error::Invalid("timeout must be positive"));
        }
        let local = matches!(backend, Backend::Local(_));
        Ok(Self {
            backend,
            timeout,
            token_scope: uuid::Uuid::new_v4().to_string(),
            reclamation: ScratchReclamation::default(),
            stream_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| Error::Store)?,
            report: BackendReport {
                backend: name.into(),
                encryption,
                plaintext_local_test: security == TransportSecurity::DisableForLocalTest,
                failure_domain: FailureDomain {
                    process_crash_survived: true,
                    host_loss: if local {
                        HostLoss::RecoveryRequiredThenResnapshot
                    } else {
                        HostLoss::SurvivedByService
                    },
                    assumption: if local {
                        "Dedicated local volume; relies on volume encryption"
                    } else {
                        "Service deployment must provide host-independent durable storage"
                    }
                    .into(),
                },
            },
        })
    }
    /// Successful explicit sync calls, observable for durability qualification.
    pub fn local_sync_counts(&self) -> Option<(u64, u64)> {
        match &self.backend {
            Backend::Local(l) => Some((
                l.file_syncs.load(Ordering::Relaxed),
                l.directory_syncs.load(Ordering::Relaxed),
            )),
            _ => None,
        }
    }
    pub fn startup_reclamation(&self) -> &ScratchReclamation {
        &self.reclamation
    }
    async fn raw_put(&self, key: &Path, content: &[u8]) -> Result<()> {
        match &self.backend {
            Backend::S3(s) => s
                .put_opts(key, content.to_vec().into(), PutMode::Create.into())
                .await
                .map(|_| ())
                .map_err(|_| Error::Store),
            Backend::Gcs(g) => match &self.report.encryption {
                Encryption::GcsCmek(kms) => g.put_cmek(key, content, kms).await,
                _ => g
                    .adapter
                    .put_opts(key, content.to_vec().into(), PutMode::Create.into())
                    .await
                    .map(|_| ())
                    .map_err(|_| Error::Store),
            },
            Backend::Local(l) => {
                let l = l.clone();
                let key = key.clone();
                let content = content.to_vec();
                // The worker owns the guards even if its caller is cancelled.
                // The owned immutable bytes cannot change between validation and
                // staging. Unlike a caller-owned file, they need no digest pass
                // to detect mutation during the copy.
                tokio::task::spawn_blocking(move || l.put_bytes(&key, &content))
                    .await
                    .map_err(|_| Error::LocalIo)?
            }
        }
    }
    async fn resolve(&self, key: &str, content: &[u8], initial: Error) -> Result<PutOutcome> {
        // A cancelled response can leave a poisoned pooled connection. Retry a
        // bounded number of exact-key reads, never a second unconditional PUT.
        let mut unknown = initial.unknown_put();
        for _ in 0..3 {
            match tokio::time::timeout(self.timeout, self.get(key)).await {
                Ok(Ok(Some(existing))) if existing == content => return Ok(PutOutcome::Resolved),
                Ok(Ok(Some(_))) => return Err(Error::Conflict),
                Ok(Err(error)) if unknown == Error::UnknownPutOutcome => {
                    unknown = error.unknown_put()
                }
                _ => (),
            }
        }
        Err(unknown)
    }

    async fn put_file_owned(&self, key: String, mut file: File) -> Result<PutOutcome> {
        let path = key_path(&key)?;
        file.rewind()
            .map_err(|e| local_error("rewind-input", &key, e))?;
        let expected = copy_digest_at(&mut file, &mut std::io::sink(), &key)?;
        if expected.bytes > MAX_STREAM_BYTES {
            return Err(Error::Invalid("streamed object exceeds 1 GB segment limit"));
        }
        file.rewind()
            .map_err(|e| local_error("rewind-input", &key, e))?;
        let put = async {
            match &self.backend {
                Backend::Local(local) => {
                    let local = local.clone();
                    let path = path.clone();
                    tokio::task::spawn_blocking(move || local.put_file(&path, file, expected))
                        .await
                        .map_err(|_| Error::LocalIo)?
                }
                Backend::S3(s3) => self.s3_put_file(s3, &path, file, expected.bytes).await,
                Backend::Gcs(gcs) => {
                    gcs.put_file(&path, file, expected.bytes, &self.report.encryption)
                        .await
                }
            }
        };
        let result = tokio::time::timeout(self.timeout, put)
            .await
            .unwrap_or(Err(Error::UnknownPutOutcome));
        match result {
            Ok(()) => Ok(PutOutcome::Created),
            Err(e @ (Error::UnsafeLocalPath | Error::Invalid(_))) => Err(e),
            Err(error) => {
                let mut unknown = error.unknown_put();
                for _ in 0..3 {
                    let mut sink = std::io::sink();
                    match tokio::time::timeout(self.timeout, self.get_into(&key, &mut sink)).await {
                        Ok(Ok(Some(actual))) if actual == expected => {
                            if let Backend::Local(local) = &self.backend {
                                let guards = local.parents(Some(&path), false)?;
                                local.sync_object(&path, &guards)?;
                            }
                            return Ok(PutOutcome::Resolved);
                        }
                        Ok(Ok(Some(_))) => return Err(Error::Conflict),
                        Ok(Err(error)) if unknown == Error::UnknownPutOutcome => {
                            unknown = error.unknown_put()
                        }
                        _ => (),
                    }
                }
                Err(unknown)
            }
        }
    }

    async fn s3_put_file(&self, s3: &AmazonS3, key: &Path, file: File, bytes: u64) -> Result<()> {
        // Own the session in a detached cleanup task: cancellation/timeout of
        // the caller cannot skip aborting a failed or abandoned multipart upload.
        let s3 = s3.clone();
        let key = key.clone();
        let timeout = self.timeout;
        let client = self.stream_client.clone();
        tokio::spawn(async move {
            let id = s3.create_multipart(&key).await.map_err(|_| Error::Store)?;
            let result =
                tokio::time::timeout(timeout, s3_upload(&client, &s3, &key, &id, file, bytes))
                    .await
                    .unwrap_or(Err(Error::UnknownPutOutcome));
            if result.is_err() {
                let _ = tokio::time::timeout(timeout, s3.abort_multipart(&key, &id)).await;
            }
            result
        })
        .await
        .map_err(|_| Error::Store)?
    }
}
#[async_trait]
impl BufferStore for ObjectBufferStore {
    fn report(&self) -> BackendReport {
        self.report.clone()
    }
    async fn put_create_only(&self, key: &str, content: &[u8]) -> Result<PutOutcome> {
        let path = key_path(key)?;
        // Local workers own their guards through staging/publication/cleanup,
        // even when this timeout drops the JoinHandle before I/O has finished.
        let result = tokio::time::timeout(self.timeout, self.raw_put(&path, content))
            .await
            .unwrap_or(Err(Error::UnknownPutOutcome));
        match result {
            Ok(()) => Ok(PutOutcome::Created),
            Err(e @ (Error::UnsafeLocalPath | Error::Invalid(_))) => Err(e),
            Err(error) => {
                let resolved = self.resolve(key, content, error).await?;
                // A local read must re-establish durability before resolving an
                // error after publication (bytes alone do not prove fsync).
                if let Backend::Local(l) = &self.backend {
                    let guards = l.parents(Some(&path), false)?;
                    l.sync_object(&path, &guards)?;
                }
                Ok(resolved)
            }
        }
    }
    async fn put_file_create_only(&self, key: &str, file: File) -> Result<PutOutcome> {
        key_path(key)?;
        let store = self.clone();
        let key = key.to_owned();
        // This task owns file/session guards even when its JoinHandle is dropped.
        tokio::spawn(async move { store.put_file_owned(key, file).await })
            .await
            .map_err(|_| Error::Store)?
    }
    async fn get_into(
        &self,
        key: &str,
        output: &mut (dyn Write + Send),
    ) -> Result<Option<ObjectDigest>> {
        let path = key_path(key)?;
        let store: &dyn ObjectStore = match &self.backend {
            Backend::S3(store) => store,
            Backend::Gcs(store) => &store.adapter,
            Backend::Local(local) => {
                let local = local.clone();
                let opened = tokio::task::spawn_blocking(move || {
                    let guards = match local.parents(Some(&path), false) {
                        Ok(guards) => guards,
                        Err(Error::LocalParentMissing) => return Ok(None),
                        Err(e) => return Err(e),
                    };
                    Ok(local
                        .check_leaf(&path)?
                        .map(|file| (guards, local.reader(file))))
                })
                .await
                .map_err(|_| Error::LocalIo)??;
                let Some(mut opened) = opened else {
                    return Ok(None);
                };
                let mut hashed = Sha256::new();
                let mut bytes = 0_u64;
                loop {
                    let read_key = key.to_owned();
                    let (next, chunk) = tokio::task::spawn_blocking(move || {
                        let mut chunk = vec![0; STREAM_BUFFER];
                        let read = opened
                            .1
                            .read(&mut chunk)
                            .map_err(|e| local_error("read-stream", &read_key, e))?;
                        chunk.truncate(read);
                        Ok::<_, Error>((opened, chunk))
                    })
                    .await
                    .map_err(|_| Error::LocalIo)??;
                    opened = next;
                    if chunk.is_empty() {
                        break;
                    }
                    output
                        .write_all(&chunk)
                        .map_err(|e| local_error("write-stream", key, e))?;
                    hashed.update(&chunk);
                    bytes = bytes
                        .checked_add(chunk.len() as u64)
                        .ok_or(Error::LocalIo)?;
                }
                return Ok(Some(ObjectDigest {
                    bytes,
                    sha256: hashed.finalize().into(),
                }));
            }
        };
        let meta = match store.head(&path).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => return Err(Error::Store),
        };
        if meta.e_tag.is_none() && meta.version.is_none() {
            return Err(Error::Store);
        }
        let mut hashed = Sha256::new();
        let mut offset = 0;
        while offset < meta.size {
            let end = (offset + STREAM_BUFFER as u64).min(meta.size);
            // Bind every range to the original object, including a concurrent
            // delete/recreate. Never concatenate bytes from two generations.
            let options = GetOptions {
                range: Some((offset..end).into()),
                if_match: meta.e_tag.clone(),
                version: meta.version.clone(),
                ..Default::default()
            };
            let result = store
                .get_opts(&path, options)
                .await
                .map_err(|_| Error::Store)?;
            if result.range != (offset..end) || result.meta.size != meta.size {
                return Err(Error::Store);
            }
            let bytes = result.bytes().await.map_err(|_| Error::Store)?;
            if bytes.len() as u64 != end - offset {
                return Err(Error::Store);
            }
            output
                .write_all(&bytes)
                .map_err(|e| local_error("write-output", key, e))?;
            hashed.update(&bytes);
            offset = end;
        }
        Ok(Some(ObjectDigest {
            bytes: offset,
            sha256: hashed.finalize().into(),
        }))
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = key_path(key)?;
        let store: &dyn ObjectStore = match &self.backend {
            Backend::S3(s) => s,
            Backend::Gcs(g) => &g.adapter,
            Backend::Local(l) => {
                let l = l.clone();
                return tokio::task::spawn_blocking(move || {
                    let _guards = match l.parents(Some(&path), false) {
                        Ok(guards) => guards,
                        Err(Error::LocalParentMissing) => return Ok(None),
                        Err(error) => return Err(error),
                    };
                    match l.check_leaf(&path)? {
                        Some(file) => {
                            let mut bytes = Vec::new();
                            l.reader(file)
                                .read_to_end(&mut bytes)
                                .map_err(|e| local_error("read-object", path.as_ref(), e))?;
                            Ok(Some(bytes))
                        }
                        None => Ok(None),
                    }
                })
                .await
                .map_err(|_| Error::LocalIo)?;
            }
        };
        read_object(store, &path).await
    }
    async fn list_prefix(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        if !(1..=1000).contains(&limit) || prefix.len() > 4096 || prefix.contains(['\\', '\0']) {
            return Err(Error::Invalid("invalid page limit or prefix"));
        }
        let continuation = token
            .map(|t| decode_token(t, &self.token_scope, prefix))
            .transpose()?;
        let (objects, next) = match &self.backend {
            Backend::S3(s) => {
                let page = s
                    .list_paginated(
                        Some(prefix),
                        PaginatedListOptions {
                            max_keys: Some(limit),
                            page_token: continuation,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|_| Error::Store)?;
                (
                    page.result
                        .objects
                        .into_iter()
                        .map(|m| BufferObject {
                            key: m.location.to_string(),
                            bytes: m.size,
                        })
                        .collect(),
                    page.page_token,
                )
            }
            Backend::Gcs(g) => g.list(prefix, continuation.as_deref(), limit).await?,
            Backend::Local(l) => {
                // LocalFileSystem has no paginated API. Scan without following
                // links, retaining only limit+1 keys rather than the full list.
                // The directory walk is blocking I/O, so run it off the runtime.
                let l = l.clone();
                let owned_prefix = prefix.to_owned();
                let mut selected = tokio::task::spawn_blocking(move || {
                    let prefix = owned_prefix.as_str();
                    let mut selected = BTreeMap::new();
                    let _ancestors = l.parents(None, false)?;
                    LocalBuffer::visit(&l.root, "", &mut |key, bytes| {
                        if key.starts_with(prefix)
                            && continuation.as_ref().is_none_or(|after| key > *after)
                        {
                            selected.insert(key, bytes);
                            if selected.len() > limit + 1 {
                                selected.pop_last();
                            }
                        }
                    })?;
                    Ok::<_, Error>(selected)
                })
                .await
                .map_err(|_| Error::LocalIo)??;
                let more = selected.len() > limit;
                if more {
                    selected.pop_last();
                }
                let next =
                    more.then(|| selected.last_key_value().expect("nonzero limit").0.clone());
                (
                    selected
                        .into_iter()
                        .map(|(key, bytes)| BufferObject { key, bytes })
                        .collect(),
                    next,
                )
            }
        };
        Ok(ListPage {
            objects,
            next: next.map(|next| encode_token(&self.token_scope, prefix, &next)),
        })
    }
    async fn delete(&self, key: &str) -> Result<()> {
        let path = key_path(key)?;
        match &self.backend {
            Backend::S3(s) => match s.delete(&path).await {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(_) => Err(Error::Store),
            },
            Backend::Gcs(g) => g.delete(&path).await,
            Backend::Local(l) => {
                let l = l.clone();
                tokio::task::spawn_blocking(move || {
                    let guards = match l.parents(Some(&path), false) {
                        Ok(guards) => guards,
                        Err(Error::LocalParentMissing) => return Ok(()),
                        Err(error) => return Err(error),
                    };
                    match l.root.remove_file(path.as_ref()) {
                        Ok(()) => l.sync_dirs(&guards, &path),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                        Err(e) => Err(local_error("delete-object", path.as_ref(), e)),
                    }
                })
                .await
                .map_err(|_| Error::LocalIo)?
            }
        }
    }
}

async fn read_object(store: &dyn ObjectStore, path: &Path) -> Result<Option<Vec<u8>>> {
    match store.get(path).await {
        Ok(result) => {
            let len = result.meta.size;
            let data = result.bytes().await.map_err(|_| Error::Store)?;
            if data.len() as u64 != len {
                return Err(Error::Store);
            }
            Ok(Some(data.to_vec()))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(_) => Err(Error::Store),
    }
}

fn copy_digest_at(
    reader: &mut impl Read,
    output: &mut (dyn Write + Send),
    path: &str,
) -> Result<ObjectDigest> {
    let mut buffer = [0; STREAM_BUFFER];
    let mut hash = Sha256::new();
    let mut bytes = 0_u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|e| local_error("read-stream", path, e))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|e| local_error("write-stream", path, e))?;
        hash.update(&buffer[..read]);
        bytes = bytes.checked_add(read as u64).ok_or(Error::LocalIo)?;
    }
    Ok(ObjectDigest {
        bytes,
        sha256: hash.finalize().into(),
    })
}

fn file_body(mut file: File, offset: u64, bytes: u64) -> Result<reqwest::Body> {
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| local_error("seek-input", "input", e))?;
    let stream = futures::stream::try_unfold((file, bytes), |(mut file, remaining)| async move {
        if remaining == 0 {
            return Ok::<_, std::io::Error>(None);
        }
        let mut chunk = vec![0; STREAM_BUFFER.min(remaining as usize)];
        file.read_exact(&mut chunk)?;
        Ok(Some((
            chunk,
            (
                file,
                remaining - STREAM_BUFFER.min(remaining as usize) as u64,
            ),
        )))
    });
    Ok(reqwest::Body::wrap_stream(stream))
}

async fn response_bytes(mut response: reqwest::Response) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Error::Store)? {
        if bytes.len().saturating_add(chunk.len()) > STREAM_BUFFER {
            return Err(Error::Store);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn s3_upload(
    client: &reqwest::Client,
    s3: &AmazonS3,
    key: &Path,
    id: &str,
    file: File,
    bytes: u64,
) -> Result<()> {
    let mut xml = String::from("<CompleteMultipartUpload>");
    let mut offset = 0;
    let mut part = 1;
    // Even a zero-byte file gets one final part. The HTTP body streams fixed
    // 16 KiB chunks; the service's minimum part size never becomes a heap buffer.
    loop {
        let length = (bytes - offset).min(8 * 1024 * 1024);
        let options = SignedUrlOptions::new().with_query([
            ("uploadId", id.to_owned()),
            ("partNumber", part.to_string()),
        ]);
        let url = s3
            .signed_url_opts(
                reqwest::Method::PUT,
                key,
                Duration::from_secs(3600),
                &options,
            )
            .await
            .map_err(|_| Error::Store)?;
        let response = client
            .put(url)
            .header("content-length", length)
            .body(file_body(
                file.try_clone()
                    .map_err(|e| local_error("clone-input", "input", e))?,
                offset,
                length,
            )?)
            .send()
            .await
            .map_err(|_| Error::Store)?;
        if !response.status().is_success() {
            return Err(Error::Store);
        }
        let etag = response
            .headers()
            .get("etag")
            .ok_or(Error::Store)?
            .to_str()
            .map_err(|_| Error::Store)?;
        // S3 part ETags are quoted hexadecimal MD5s. Refuse arbitrary response
        // text in XML and retain only bounded completion metadata.
        let etag = etag.trim_matches('"');
        if etag.len() != 32 || !etag.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::Store);
        }
        use std::fmt::Write as _;
        write!(
            xml,
            "<Part><PartNumber>{part}</PartNumber><ETag>\"{etag}\"</ETag></Part>"
        )
        .map_err(|_| Error::Store)?;
        response_bytes(response).await?;
        offset += length;
        if offset == bytes {
            break;
        }
        part += 1;
    }
    xml.push_str("</CompleteMultipartUpload>");
    // The final publication itself is conditional. No object temp key or
    // unconditional server-side copy can replace the destination.
    let options = SignedUrlOptions::new()
        .with_query([("uploadId", id)])
        .with_signed_header(
            "if-none-match".parse().map_err(|_| Error::Store)?,
            "*".parse().map_err(|_| Error::Store)?,
        );
    let url = s3
        .signed_url_opts(
            reqwest::Method::POST,
            key,
            Duration::from_secs(3600),
            &options,
        )
        .await
        .map_err(|_| Error::Store)?;
    let response = client
        .post(url)
        .header("if-none-match", "*")
        .header("content-type", "application/xml")
        .body(xml)
        .send()
        .await
        .map_err(|_| Error::Store)?;
    if !response.status().is_success() {
        return Err(Error::Store);
    }
    // S3 can return HTTP 200 with an embedded failure. Treat every ambiguous
    // response as unknown and let the caller resolve by full-object SHA-256.
    let bytes = response_bytes(response).await?;
    let text = std::str::from_utf8(&bytes).map_err(|_| Error::Store)?;
    if !text.contains("<CompleteMultipartUploadResult") || text.contains("<Error>") {
        return Err(Error::Store);
    }
    Ok(())
}

fn key_path(key: &str) -> Result<Path> {
    // Conservative portable names also exclude Windows ADS, reserved devices,
    // trailing-dot aliases, and object_store's numeric staging-file namespace.
    if key.is_empty()
        || key.len() > 4096
        || key.contains(['\\', ':', '\0', '#'])
        || key.split('/').any(|p| {
            p.is_empty()
                || matches!(p, "." | "..")
                || p.ends_with(['.', ' '])
                || owned_scratch_name(p)
                || {
                    let stem = p.split('.').next().unwrap_or("").to_ascii_uppercase();
                    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
                        || (stem.len() == 4
                            && (stem.starts_with("COM") || stem.starts_with("LPT"))
                            && stem.as_bytes()[3].is_ascii_digit())
                }
        })
    {
        return Err(Error::UnsafeLocalPath);
    }
    Path::parse(key).map_err(|_| Error::UnsafeLocalPath)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Token {
    scope: String,
    prefix: String,
    next: String,
}
fn encode_token(scope: &str, prefix: &str, next: &str) -> String {
    URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&Token {
            scope: scope.into(),
            prefix: prefix.into(),
            next: next.into(),
        })
        .expect("string serialization"),
    )
}
fn decode_token(token: &str, scope: &str, prefix: &str) -> Result<String> {
    if token.len() > 65536 {
        return Err(Error::Invalid("page token too large"));
    }
    let t: Token = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| Error::Invalid("invalid page token"))?,
    )
    .map_err(|_| Error::Invalid("invalid page token"))?;
    if t.scope != scope || t.prefix != prefix {
        return Err(Error::Invalid(
            "page token belongs to another store or prefix",
        ));
    }
    Ok(t.next)
}

#[derive(Clone)]
struct GcsBuffer {
    adapter: crate::coordinator::gcs::GcsJsonStore,
    auth: GoogleCloudStorage,
    bucket: String,
    endpoint: reqwest::Url,
    client: reqwest::Client,
}
impl GcsBuffer {
    async fn put_file(
        &self,
        key: &Path,
        file: File,
        bytes: u64,
        encryption: &Encryption,
    ) -> Result<()> {
        let store = self.clone();
        let key = key.clone();
        let encryption = encryption.clone();
        // The worker owns the session through error cleanup even if the caller
        // cancels or times out while initiating or sending the upload.
        tokio::spawn(async move { store.put_file_inner(&key, file, bytes, &encryption).await })
            .await
            .map_err(|_| Error::Store)?
    }
    async fn put_file_inner(
        &self,
        key: &Path,
        file: File,
        bytes: u64,
        encryption: &Encryption,
    ) -> Result<()> {
        let mut url = self.url(true, None)?;
        // JSON resumable sessions retain the insert preconditions through the
        // final data request. The destination remains create-only at commit.
        url.query_pairs_mut()
            .append_pair("uploadType", "resumable")
            .append_pair("name", key.as_ref())
            .append_pair("ifGenerationMatch", "0");
        if let Encryption::GcsCmek(kms) = encryption {
            url.query_pairs_mut().append_pair("kmsKeyName", kms);
        }
        let response = self
            .request(reqwest::Method::POST, url)
            .await?
            .header("x-upload-content-type", "application/octet-stream")
            .header("x-upload-content-length", bytes)
            .header("content-length", 0)
            .send()
            .await
            .map_err(|_| Error::Store)?;
        if !response.status().is_success() {
            return Err(Error::Store);
        }
        let session = response
            .headers()
            .get("location")
            .ok_or(Error::Store)?
            .to_str()
            .map_err(|_| Error::Store)?;
        let session = reqwest::Url::parse(session).map_err(|_| Error::Store)?;
        // Session URLs are bearer capabilities. Never forward credentials to a
        // redirect or a server-supplied foreign origin, even on local endpoints.
        if session.origin() != self.endpoint.origin()
            || !session.username().is_empty()
            || session.password().is_some()
        {
            return Err(Error::Store);
        }
        response_bytes(response).await?;
        let response = self
            .client
            .put(session.clone())
            .header("content-length", bytes)
            .header("content-type", "application/octet-stream")
            .body(file_body(file, 0, bytes)?)
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                response_bytes(response).await?;
                Ok(())
            }
            _ => {
                // Cancel this exact session; cancellation cannot remove an
                // already finalized object. Unknown completion is reread above.
                let _ = self.client.delete(session).send().await;
                Err(Error::Store)
            }
        }
    }
    fn url(&self, upload: bool, key: Option<&Path>) -> Result<reqwest::Url> {
        let mut url = self.endpoint.clone();
        let mut parts = url.path_segments_mut().map_err(|_| Error::Store)?;
        parts.pop_if_empty();
        if upload {
            parts.push("upload");
        }
        parts.extend(["storage", "v1", "b", &self.bucket, "o"]);
        if let Some(key) = key {
            parts.push(key.as_ref());
        }
        drop(parts);
        Ok(url)
    }
    async fn request(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
    ) -> Result<reqwest::RequestBuilder> {
        let credential = self
            .auth
            .credentials()
            .get_credential()
            .await
            .map_err(|_| Error::Store)?;
        let request = self.client.request(method, url);
        Ok(if credential.bearer.is_empty() {
            request
        } else {
            request.bearer_auth(&credential.bearer)
        })
    }
    async fn put_cmek(&self, key: &Path, bytes: &[u8], kms: &str) -> Result<()> {
        // The public coordinator adapter has no encryption option. Keep the
        // extension here until that API supports kmsKeyName (never silently omit).
        let mut url = self.url(true, None)?;
        url.query_pairs_mut()
            .append_pair("uploadType", "media")
            .append_pair("name", key.as_ref())
            .append_pair("ifGenerationMatch", "0")
            .append_pair("kmsKeyName", kms);
        let response = self
            .request(reqwest::Method::POST, url)
            .await?
            .header("content-type", "application/octet-stream")
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|_| Error::Store)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::Store)
        }
    }
    async fn delete(&self, key: &Path) -> Result<()> {
        let response = self
            .request(reqwest::Method::DELETE, self.url(false, Some(key))?)
            .await?
            .send()
            .await
            .map_err(|_| Error::Store)?;
        if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(Error::Store)
        }
    }
    async fn list(
        &self,
        prefix: &str,
        token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<BufferObject>, Option<String>)> {
        #[derive(Deserialize)]
        struct Item {
            name: String,
            size: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Page {
            #[serde(default)]
            items: Vec<Item>,
            next_page_token: Option<String>,
        }
        let mut url = self.url(false, None)?;
        url.query_pairs_mut()
            .append_pair("prefix", prefix)
            .append_pair("maxResults", &limit.to_string());
        if let Some(token) = token {
            url.query_pairs_mut().append_pair("pageToken", token);
        }
        let response = self
            .request(reqwest::Method::GET, url)
            .await?
            .send()
            .await
            .map_err(|_| Error::Store)?;
        if !response.status().is_success() {
            return Err(Error::Store);
        }
        let page: Page = response.json().await.map_err(|_| Error::Store)?;
        if page.items.len() > limit || (token.is_some() && page.next_page_token.as_deref() == token)
        {
            return Err(Error::Store);
        }
        Ok((
            page.items
                .into_iter()
                .map(|i| {
                    Ok(BufferObject {
                        key: i.name,
                        bytes: i.size.parse().map_err(|_| Error::Store)?,
                    })
                })
                .collect::<Result<_>>()?,
            page.next_page_token,
        ))
    }
}

/// Defense in depth for explicit/external byte-range locks. StageFile locks a
/// separate lease, never published data. On Windows a data lock denies reads
/// through another handle, including one in this process:
/// https://learn.microsoft.com/en-us/windows/win32/fileio/locking-and-unlocking-byte-ranges-in-files
struct PublishedFileReader {
    file: File,
    #[cfg(all(test, windows))]
    wait: Option<Arc<dyn Fn(Duration) + Send + Sync>>,
}

impl Read for PublishedFileReader {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(windows)]
        {
            #[cfg(test)]
            if let Some(wait) = self.wait.clone() {
                return self.read_with_wait(bytes, |delay| wait(delay));
            }
            self.read_with_wait(bytes, std::thread::sleep)
        }
        #[cfg(not(windows))]
        self.file.read(bytes)
    }
}

#[cfg(windows)]
impl PublishedFileReader {
    fn read_with_wait(
        &mut self,
        bytes: &mut [u8],
        mut wait: impl FnMut(Duration),
    ) -> std::io::Result<usize> {
        let offset = self.file.stream_position()?;
        // ERROR_LOCK_VIOLATION only: at most eight retries, 255 ms total delay.
        // Never retry generic PermissionDenied, sharing, sync or rename errors.
        // Restore the input offset before retry; the caller sees only one read
        // result and streaming output is never repeated or rewound.
        for retry in 0..=8 {
            match self.file.read(bytes) {
                Err(error) if error.raw_os_error() == Some(33) && retry < 8 => {
                    wait(Duration::from_millis(1 << retry));
                    self.file.seek(SeekFrom::Start(offset))?;
                }
                result => return result,
            }
        }
        unreachable!("bounded read loop always returns")
    }
}

struct LocalBuffer {
    root: Dir,
    root_path: PathBuf,
    file_syncs: AtomicU64,
    directory_syncs: AtomicU64,
    #[cfg(all(test, windows))]
    read_wait: Option<Arc<dyn Fn(Duration) + Send + Sync>>,
}
impl LocalBuffer {
    fn reader(&self, file: File) -> PublishedFileReader {
        PublishedFileReader {
            file,
            #[cfg(all(test, windows))]
            wait: self.read_wait.clone(),
        }
    }
    fn put_file(&self, key: &Path, mut file: File, expected: ObjectDigest) -> Result<()> {
        self.put_reader(key, &mut file, expected)
    }
    fn put_bytes(&self, key: &Path, content: &[u8]) -> Result<()> {
        self.put_staged(key, |stage| {
            stage
                .write_all(content)
                .map_err(|e| local_error("write-stage", key.as_ref(), e))
        })
    }
    fn put_reader(&self, key: &Path, input: &mut impl Read, expected: ObjectDigest) -> Result<()> {
        self.put_staged(key, |stage| {
            let actual = copy_digest_at(input, stage, key.as_ref())?;
            if actual != expected {
                return Err(Error::LocalIo);
            }
            Ok(())
        })
    }
    fn put_staged(&self, key: &Path, write: impl FnOnce(&mut File) -> Result<()>) -> Result<()> {
        let guards = self.parents(Some(key), true)?;
        self.check_leaf(key)?;
        let parent = guards.last().expect("root handle").clone();
        let mut stage = StageFile::new(parent.clone(), key.as_ref())?;
        write(stage.file.as_mut().expect("live staging file"))?;
        stage
            .file
            .as_ref()
            .expect("live staging file")
            .sync_all()
            .map_err(|e| local_error("sync-stage", key.as_ref(), e))?;
        self.file_syncs.fetch_add(1, Ordering::Relaxed);
        let leaf = key
            .as_ref()
            .rsplit('/')
            .next()
            .ok_or(Error::UnsafeLocalPath)?;
        parent
            .rename_new(&stage.name, leaf)
            .map_err(|e| local_error("publish-stage", key.as_ref(), e))?;
        // Publication moved the opened object. Do not dispose the destination
        // through the still-open Windows staging handle during Drop.
        drop(stage.file.take());
        drop(stage);
        self.sync_dirs(&guards, key)
    }
    fn parents(&self, key: Option<&Path>, create: bool) -> Result<Vec<Dir>> {
        let mut guards = vec![self.root.clone()];
        if let Some(parent) = key.and_then(|key| key.as_ref().rsplit_once('/').map(|p| p.0)) {
            match self.root.dir(parent, create) {
                Ok(dir) => guards.push(dir),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(Error::LocalParentMissing);
                }
                Err(e) => return Err(local_error("open-parent", parent, e)),
            }
        }
        Ok(guards)
    }
    fn check_leaf(&self, key: &Path) -> Result<Option<File>> {
        match self.root.open_file(key.as_ref()) {
            Ok(file) => Ok(Some(file)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(local_error("open-object", key.as_ref(), e)),
        }
    }
    fn sync_dirs(&self, dirs: &[Dir], key: &Path) -> Result<()> {
        for (index, dir) in dirs.iter().enumerate().rev() {
            let path = if index == 0 {
                self.root_path.clone()
            } else {
                self.root_path
                    .join(key.as_ref().rsplit_once('/').expect("parent guard").0)
            };
            dir.sync()
                .map_err(|e| local_error("sync-directory", &path.to_string_lossy(), e))?;
            self.directory_syncs.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
    fn sync_object(&self, key: &Path, dirs: &[Dir]) -> Result<()> {
        self.check_leaf(key)?
            .ok_or(Error::LocalIo)?
            .sync_all()
            .map_err(|e| local_error("sync-object", key.as_ref(), e))?;
        self.file_syncs.fetch_add(1, Ordering::Relaxed);
        self.sync_dirs(dirs, key)
    }
    fn visit(dir: &Dir, prefix: &str, accept: &mut impl FnMut(String, u64)) -> Result<()> {
        for name in dir
            .names()
            .map_err(|e| local_error("list-directory", prefix, e))?
        {
            let text = name.to_str().ok_or(Error::UnsafeLocalPath)?;
            let key = format!("{prefix}{text}");
            match dir.dir(&name, false) {
                Ok(child) => Self::visit(&child, &format!("{key}/"), accept)?,
                Err(_) => {
                    let file = dir
                        .open_file(&name)
                        .map_err(|e| local_error("list-open-object", &key, e))?;
                    if !key.contains('#') {
                        let bytes = file
                            .metadata()
                            .map_err(|e| local_error("list-object-metadata", &key, e))?
                            .len();
                        accept(key, bytes);
                    }
                }
            }
        }
        Ok(())
    }
}

struct StageFile {
    file: Option<File>,
    lease: Option<File>,
    dir: Dir,
    name: String,
}
impl StageFile {
    fn new(dir: Dir, key: &str) -> Result<Self> {
        Self::new_with_lease_hook(dir, key, || {})
    }

    fn new_with_lease_hook(dir: Dir, key: &str, created: impl FnOnce()) -> Result<Self> {
        let name = format!("#skedaddle-stage-{}", uuid::Uuid::new_v4());
        let lease_name = format!("{name}.lock");
        let lease = dir
            .create_file(&lease_name)
            .map_err(|e| local_error("create-stage-lease", key, e))?;
        created();
        lease.try_lock().map_err(|error| {
            local_error(
                "lock-stage-lease",
                key,
                match error {
                    std::fs::TryLockError::Error(error) => error,
                    std::fs::TryLockError::WouldBlock => std::io::ErrorKind::WouldBlock.into(),
                },
            )
        })?;
        // Reclamation may win between create and lock. It removes the lease
        // only while holding this lock; validate the name after locking before
        // creating data. UUID lease names are create-new and never reused.
        dir.open_file(&lease_name)
            .map_err(|e| local_error("verify-stage-lease", key, e))?;
        let mut stage = Self {
            file: None,
            lease: Some(lease),
            dir,
            name,
        };
        stage.file = Some(
            stage
                .dir
                .create_file(&stage.name)
                .map_err(|e| local_error("create-stage", key, e))?,
        );
        Ok(stage)
    }
}
impl Drop for StageFile {
    fn drop(&mut self) {
        if let Some(file) = &self.file {
            let _ = self.dir.remove_open_file(&self.name, file);
        }
        drop(self.file.take());
        if let Some(lease) = &self.lease {
            let _ = self
                .dir
                .remove_open_file(format!("{}.lock", self.name), lease);
        }
        drop(self.lease.take());
    }
}

/// Reclaims only identified, unlocked product scratch using pinned directories.
pub fn reclaim_scratch(root: &FsPath) -> Result<ScratchReclamation> {
    let confined = Dir::open_root(root)
        .map_err(|e| local_error("open-scratch-root", &root.to_string_lossy(), e))?;
    reclaim_scratch_in(&confined, root)
}

/// Audit the same capability retained by the scratch owner.
pub(super) fn reclaim_scratch_in(confined: &Dir, root: &FsPath) -> Result<ScratchReclamation> {
    let mut report = ScratchReclamation::default();
    reclaim_directory(confined, root, &mut report)?;
    Ok(report)
}
fn owned_scratch_name(name: &str) -> bool {
    let name = if name.starts_with("#skedaddle-stage-") {
        name.strip_suffix(".lock").unwrap_or(name)
    } else {
        name
    };
    let uuid = name.strip_prefix("#skedaddle-stage-").or_else(|| {
        name.strip_prefix("txn-")
            .and_then(|n| n.strip_suffix(".skedaddle-scratch"))
    });
    uuid.is_some_and(|s| uuid::Uuid::parse_str(s).is_ok_and(|u| u.hyphenated().to_string() == s))
}
fn reclaim_directory(dir: &Dir, display: &FsPath, report: &mut ScratchReclamation) -> Result<()> {
    let mut removed = false;
    for name in dir
        .names()
        .map_err(|e| local_error("list-scratch-directory", &display.to_string_lossy(), e))?
    {
        let path = display.join(&name);
        if let Ok(child) = dir.dir(&name, false) {
            reclaim_directory(&child, &path, report)?;
            continue;
        }
        if !name.to_str().is_some_and(owned_scratch_name) {
            report.retained.push(path);
            continue;
        }
        let text = name.to_str().expect("validated scratch name");
        if text.starts_with("#skedaddle-stage-") {
            let stage_name = text.strip_suffix(".lock").unwrap_or(text);
            let lease_name = format!("{stage_name}.lock");
            let lease = match dir.open_file(&lease_name) {
                Ok(file) => {
                    if file.try_lock().is_err() {
                        report.retained.push(path);
                        continue;
                    }
                    Some(file)
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(_) => {
                    report.retained.push(path);
                    continue;
                }
            };
            // Keep legacy directly locked stages safe too. A live new stage
            // cannot exist without its locked lease. Hold the lease through
            // data removal, including on Unix where unlink ignores file locks.
            if !reclaim_file(dir, display, stage_name, report, &mut removed) {
                continue;
            }
            if let Some(lease) = lease {
                match dir.remove_open_file(&lease_name, &lease) {
                    Ok(()) => {
                        report.removed.push(display.join(&lease_name));
                        removed = true;
                    }
                    Err(_) => report.retained.push(display.join(&lease_name)),
                }
            }
            continue;
        }
        reclaim_file(dir, display, text, report, &mut removed);
    }
    if removed {
        dir.sync()
            .map_err(|e| local_error("sync-scratch-directory", &display.to_string_lossy(), e))?;
    }
    Ok(())
}

fn reclaim_file(
    dir: &Dir,
    display: &FsPath,
    name: &str,
    report: &mut ScratchReclamation,
    removed: &mut bool,
) -> bool {
    let path = display.join(name);
    let file = match dir.open_file(name) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => {
            report.retained.push(path);
            return false;
        }
    };
    if file.try_lock().is_err() {
        report.retained.push(path);
        return false;
    }
    match dir.remove_open_file(name, &file) {
        Ok(()) => {
            report.removed.push(path);
            *removed = true;
            true
        }
        Err(_) => {
            report.retained.push(path);
            false
        }
    }
}

fn local_error(operation: &'static str, path: &str, error: std::io::Error) -> Error {
    #[cfg(unix)]
    if error.raw_os_error() == Some(rustix::io::Errno::LOOP.raw_os_error()) {
        return Error::UnsafeLocalPath;
    }
    match error.kind() {
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::NotADirectory => {
            Error::UnsafeLocalPath
        }
        _ => Error::LocalIoDetail(LocalIoDiagnostic {
            operation,
            path_fingerprint: u64::from_be_bytes(
                Sha256::digest(path.as_bytes())[..8].try_into().unwrap(),
            ),
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
        }),
    }
}

#[cfg(test)]
mod local_io_tests {
    use super::*;

    #[test]
    fn local_io_diagnostic_preserves_codes_and_redacts_path_and_message() {
        let path = "private/customer-secret/token";
        let original = std::io::Error::from_raw_os_error(33);
        let kind = original.kind();
        let error = local_error("read-object", path, original);
        let Error::LocalIoDetail(detail) = error else {
            panic!("missing local diagnostic: {error}");
        };
        assert_eq!(detail.operation, "read-object");
        assert_eq!(detail.kind, kind);
        assert_eq!(detail.raw_os_error, Some(33));
        assert_ne!(detail.path_fingerprint, 0);
        assert!(!error.to_string().contains(path));
        let secret = "credential=do-not-disclose";
        let error = local_error("read-object", path, std::io::Error::other(secret));
        assert!(!format!("{error:?}: {error}").contains(secret));
    }

    #[cfg(windows)]
    mod windows {
        use super::*;
        use std::sync::Mutex;

        fn fixture() -> ObjectBufferStore {
            let root =
                PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("test scratch root"))
                    .join(format!("buffer-publication-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            ObjectBufferStore::local(root, Duration::from_secs(3)).unwrap()
        }

        fn local(store: &ObjectBufferStore) -> &LocalBuffer {
            let Backend::Local(local) = &store.backend else {
                unreachable!()
            };
            local
        }

        fn set_wait(
            store: &mut ObjectBufferStore,
            wait: impl Fn(Duration) + Send + Sync + 'static,
        ) {
            let Backend::Local(local) = &mut store.backend else {
                unreachable!()
            };
            Arc::get_mut(local).unwrap().read_wait = Some(Arc::new(wait));
        }

        fn published_stage(store: &ObjectBufferStore, content: &[u8]) -> StageFile {
            let dir = local(store).root.clone();
            let mut stage = StageFile::new(dir.clone(), "published").unwrap();
            stage.file.as_mut().unwrap().write_all(content).unwrap();
            stage.file.as_ref().unwrap().sync_all().unwrap();
            dir.rename_new(&stage.name, "published").unwrap();
            // The publication exists immediately after rename, before either
            // the original data handle or its reclamation lease is released.
            let mut actual = Vec::new();
            dir.open_file("published")
                .unwrap()
                .read_to_end(&mut actual)
                .unwrap();
            assert_eq!(actual, content);
            dir.sync().unwrap();
            drop(stage.file.take());
            stage
        }

        fn explicitly_locked_publication(store: &ObjectBufferStore, content: &[u8]) -> File {
            let stage = published_stage(store, content);
            let file = local(store).root.open_file("published").unwrap();
            file.try_lock().unwrap();
            drop(stage);
            let dir = &local(store).root;
            let error = dir
                .open_file("published")
                .unwrap()
                .read(&mut [0])
                .unwrap_err();
            assert_eq!(
                error.raw_os_error(),
                Some(33),
                "explicit published-data lock must produce OS33"
            );
            file
        }

        #[tokio::test]
        async fn published_stage_is_readable_immediately_while_lease_is_held() {
            let mut store = fixture();
            let content = b"published bytes";
            let stage = published_stage(&store, content);
            let waits = Arc::new(AtomicU64::new(0));
            set_wait(&mut store, {
                let waits = waits.clone();
                move |_| {
                    waits.fetch_add(1, Ordering::Relaxed);
                }
            });
            assert_eq!(store.get("published").await.unwrap().unwrap(), content);
            let mut output = Vec::new();
            assert_eq!(
                store
                    .get_into("published", &mut output)
                    .await
                    .unwrap()
                    .unwrap()
                    .bytes,
                content.len() as u64
            );
            assert_eq!(output, content);
            assert_eq!(
                waits.load(Ordering::Relaxed),
                0,
                "publication must need no backoff"
            );
            assert!(stage.lease.is_some(), "lease still held across both reads");
            drop(stage);
        }

        #[test]
        fn active_stage_lease_prevents_reclamation_and_gap_winner_aborts_creation() {
            let store = fixture();
            let dir = local(&store).root.clone();
            let stage = StageFile::new(dir.clone(), "active").unwrap();
            let report = reclaim_scratch(&local(&store).root_path).unwrap();
            assert!(report.removed.is_empty());
            assert!(
                report
                    .retained
                    .contains(&local(&store).root_path.join(&stage.name))
            );
            assert!(dir.open_file(&stage.name).is_ok());
            let result = StageFile::new_with_lease_hook(dir.clone(), "gap", || {
                let report = reclaim_scratch(&local(&store).root_path).unwrap();
                assert_eq!(report.removed.len(), 1, "reclaim the not-yet-locked lease");
            });
            assert!(
                result.is_err(),
                "lost lease must abort before data creation"
            );
            assert_eq!(
                dir.names().unwrap().len(),
                2,
                "only active data and lease remain"
            );
        }

        #[test]
        fn stage_lease_crash_child() {
            let Some(root) = std::env::var_os("SKEDADDLE_STAGE_CRASH_ROOT") else {
                return;
            };
            let stage =
                StageFile::new(Dir::open_root(FsPath::new(&root)).unwrap(), "crash").unwrap();
            assert!(stage.lease.is_some());
            std::process::exit(0);
        }

        #[test]
        fn dead_process_stage_and_orphan_sidecar_are_reclaimed() {
            let store = fixture();
            let root = &local(&store).root_path;
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "buffer::backends::local_io_tests::windows::stage_lease_crash_child",
                    "--nocapture",
                ])
                .env("SKEDADDLE_STAGE_CRASH_ROOT", root)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(local(&store).root.names().unwrap().len(), 2);
            let orphan = format!("#skedaddle-stage-{}.lock", uuid::Uuid::new_v4());
            drop(local(&store).root.create_file(&orphan).unwrap());
            let report = reclaim_scratch(root).unwrap();
            assert_eq!(report.removed.len(), 3);
            assert!(report.retained.is_empty());
            assert!(local(&store).root.names().unwrap().is_empty());
        }

        async fn transient_read(streaming: bool) {
            let mut store = fixture();
            let content: Vec<_> = (0..(STREAM_BUFFER * 3 + 7))
                .map(|i| (i % 251) as u8)
                .collect();
            let stage = Arc::new(Mutex::new(Some(explicitly_locked_publication(
                &store, &content,
            ))));
            let waits = Arc::new(AtomicU64::new(0));
            set_wait(&mut store, {
                let stage = stage.clone();
                let waits = waits.clone();
                move |delay| {
                    assert_eq!(delay, Duration::from_millis(1));
                    waits.fetch_add(1, Ordering::Relaxed);
                    // Release only after the production reader has hit a real
                    // lock violation. No scheduler timing or fake I/O errors.
                    drop(stage.lock().unwrap().take());
                }
            });
            if streaming {
                let mut output = b"existing-output-prefix".to_vec();
                let digest = store
                    .get_into("published", &mut output)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(digest.bytes, content.len() as u64);
                assert_eq!(digest.sha256, <[u8; 32]>::from(Sha256::digest(&content)));
                assert_eq!(&output[b"existing-output-prefix".len()..], content);
                assert!(output.starts_with(b"existing-output-prefix"));
            } else {
                assert_eq!(store.get("published").await.unwrap(), Some(content));
            }
            assert_eq!(waits.load(Ordering::Relaxed), 1);
        }

        #[tokio::test]
        async fn get_retries_a_real_explicit_data_lock() {
            transient_read(false).await;
        }

        #[tokio::test]
        async fn get_into_retries_a_real_explicit_data_lock_without_repeating_output() {
            transient_read(true).await;
        }

        #[tokio::test]
        async fn persistent_explicit_data_lock_is_bounded_and_keeps_io_diagnostic() {
            let mut store = fixture();
            let stage = explicitly_locked_publication(&store, b"durable bytes");
            let delays = Arc::new(Mutex::new(Vec::new()));
            set_wait(&mut store, {
                let delays = delays.clone();
                move |delay| delays.lock().unwrap().push(delay)
            });
            let error = store.get("published").await.unwrap_err();
            let Error::LocalIoDetail(detail) = error else {
                panic!("{error}")
            };
            assert_eq!(detail.operation, "read-object");
            assert_eq!(detail.raw_os_error, Some(33));
            let mut output = b"untouched".to_vec();
            let error = store.get_into("published", &mut output).await.unwrap_err();
            let Error::LocalIoDetail(stream_detail) = error else {
                panic!("{error}")
            };
            assert_eq!(stream_detail.operation, "read-stream");
            assert_eq!(stream_detail.raw_os_error, Some(33));
            assert_eq!(stream_detail.path_fingerprint, detail.path_fingerprint);
            assert_eq!(output, b"untouched");
            let delays = delays.lock().unwrap();
            assert_eq!(delays.len(), 16, "eight retries per actual get path");
            assert_eq!(delays.iter().sum::<Duration>(), Duration::from_millis(510));
            drop(stage);
        }

        #[test]
        fn publication_retry_preserves_a_nonzero_read_offset() {
            let store = fixture();
            let mut stage = Some(explicitly_locked_publication(&store, b"0123456789"));
            let mut reader =
                local(&store).reader(local(&store).root.open_file("published").unwrap());
            reader.file.seek(SeekFrom::Start(3)).unwrap();
            let mut bytes = [0; 4];
            let count = reader
                .read_with_wait(&mut bytes, |_| drop(stage.take()))
                .unwrap();
            assert_eq!(count, 4);
            assert_eq!(&bytes, b"3456");
            assert_eq!(reader.file.stream_position().unwrap(), 7);
        }

        #[test]
        fn publication_retry_does_not_retry_access_denied() {
            let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap());
            let path = root.join(format!("write-only-{}", uuid::Uuid::new_v4()));
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .unwrap();
            let mut reader = PublishedFileReader { file, wait: None };
            let error = reader
                .read_with_wait(&mut [0], |_| panic!("non-lock failure retried"))
                .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(5));
        }

        #[tokio::test]
        async fn streaming_output_failure_is_not_retried() {
            struct FailingOutput(usize);
            impl Write for FailingOutput {
                fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                    self.0 += 1;
                    Err(std::io::Error::from_raw_os_error(5))
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            let store = fixture();
            store.put_create_only("published", b"bytes").await.unwrap();
            let mut output = FailingOutput(0);
            let error = store.get_into("published", &mut output).await.unwrap_err();
            let Error::LocalIoDetail(detail) = error else {
                panic!("{error}")
            };
            assert_eq!(detail.operation, "write-stream");
            assert_eq!(detail.raw_os_error, Some(5));
            assert_eq!(output.0, 1);
        }

        #[tokio::test]
        async fn unresolved_local_puts_preserve_the_initial_io_failure() {
            use std::os::windows::fs::OpenOptionsExt;

            let store = fixture();
            store
                .put_create_only("published", b"original")
                .await
                .unwrap();
            let _denial = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(0)
                .open(local(&store).root_path.join("published"))
                .unwrap();
            let expected = local(&store)
                .check_leaf(&Path::from("published"))
                .unwrap_err();
            let Error::LocalIoDetail(expected) = expected else {
                panic!("{expected}")
            };
            assert_eq!(expected.operation, "open-object");
            assert_eq!(expected.raw_os_error, Some(32));
            let error = store
                .put_create_only("published", b"replacement")
                .await
                .unwrap_err();
            assert_eq!(error, Error::UnknownPutOutcomeLocal(expected));
            assert!(error.to_string().contains("do not acknowledge"));

            let mut input = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(local(&store).root_path.join("input"))
                .unwrap();
            input.write_all(b"replacement").unwrap();
            let error = store
                .put_file_create_only("published", input)
                .await
                .unwrap_err();
            assert_eq!(error, Error::UnknownPutOutcomeLocal(expected));
        }
    }
}
