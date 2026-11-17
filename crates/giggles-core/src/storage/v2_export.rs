//! Streaming logical v2 backup. All integers are big endian.
//! Header: GDEXP002, u32 version (1), u64 features, u64 snapshot timestamp.
//! Records: u32 body length, u8 keyspace, u32 key length, key, value.
//! Tags: 0 = source domain metadata, 1 = catalog, 2 = rows, 3 = indexes,
//! 4 = allocator/counter high waters. Records are strictly (tag,key) ordered.
//! Versioned keyspaces contain only live user keys/payloads at the snapshot.
//! Trailer: SHA-256 of the entire header and framed records, excluding itself.
use super::*;
use engine::{
    codec,
    hlc::Ts,
    kv::{Direction, Durability, Keyspace, KvError, KvIter, KvRead, WriteBatch},
    mvcc,
};
use std::{
    fs::File,
    io::{Seek, SeekFrom},
    ops::Bound,
};

const MAGIC: &[u8; 8] = b"GDEXP002";
const HEADER: u64 = 28;
const TRAILER: u64 = 32;

fn corrupt(message: &str) -> CoreError {
    CoreError::Corrupt(format!("v2 export: {message}"))
}

pub(super) fn is_export(path: &Path) -> Result<bool, CoreError> {
    let mut magic = [0; 8];
    File::open(path)
        .map_err(io)?
        .read_exact(&mut magic)
        .map_err(io)?;
    Ok(&magic == MAGIC)
}

fn staging(path: &Path) -> Result<std::path::PathBuf, CoreError> {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(io)?;
    let mut name = path
        .file_name()
        .ok_or(CoreError::Unsupported)?
        .to_os_string();
    name.push(format!(".staging-{:032x}", u128::from_be_bytes(nonce)));
    Ok(path.with_file_name(name))
}

fn write_hashed(file: &mut File, hash: &mut Sha256, bytes: &[u8]) -> Result<(), CoreError> {
    file.write_all(bytes).map_err(io)?;
    hash.update(bytes);
    Ok(())
}

fn write_record(
    file: &mut File,
    hash: &mut Sha256,
    tag: u8,
    key: &[u8],
    value: &[u8],
) -> Result<(), CoreError> {
    let len = key
        .len()
        .checked_add(value.len())
        .and_then(|n| n.checked_add(5))
        .and_then(|n| u32::try_from(n).ok())
        .ok_or(CoreError::Limit)?;
    write_hashed(file, hash, &len.to_be_bytes())?;
    write_hashed(file, hash, &[tag])?;
    write_hashed(file, hash, &(key.len() as u32).to_be_bytes())?;
    write_hashed(file, hash, key)?;
    write_hashed(file, hash, value)
}

pub(super) fn export(
    db: &v2::V2Db,
    path: &Path,
    point: RecoveryPoint,
) -> Result<[u8; 32], CoreError> {
    export_inner(db, path, point, |_| {})
}

fn export_inner(
    db: &v2::V2Db,
    path: &Path,
    point: RecoveryPoint,
    after_snapshot: impl FnOnce(Ts),
) -> Result<[u8; 32], CoreError> {
    // Register with GC first, then pin a physical snapshot. Its durable commit
    // timestamp and all unversioned feature/counter records are one atomic view.
    // The earlier MVCC pin protects every version needed by that later view.
    db.require_local()?;
    let _pin = db.sequencer.snapshot();
    let read = db.kv.snapshot()?;
    let features = read
        .get(Keyspace::Meta, b"features")?
        .ok_or_else(|| corrupt("features"))?;
    let features: [u8; 8] = features
        .try_into()
        .map_err(|_| corrupt("features length"))?;
    // Task databases are not exportable (GD-0061 D8); refuse before any file exists.
    if u64::from_be_bytes(features) & engine::meta::EXPORT_REFUSED != 0 {
        return Err(CoreError::Unsupported);
    }
    let ts = read
        .get(Keyspace::Meta, b"last_commit_ts")?
        .map(|v| {
            v.try_into()
                .map(u64::from_be_bytes)
                .map(Ts)
                .map_err(|_| corrupt("timestamp"))
        })
        .transpose()?
        .unwrap_or(Ts(0));
    if u64::from_be_bytes(features) & engine::meta::NATIVE_OUTCOMES != 0
        || read.range(Keyspace::Kernel, Bound::Included(&[7]), Bound::Excluded(&[8]), Direction::Forward)?.next().transpose()?.is_some()
    {
        return Err(CoreError::Unsupported);
    }
    let checked = engine::meta::open_checks(read.as_ref()).map_err(v2::format_error)?;
    after_snapshot(ts);
    let stage = staging(path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stage)
        .map_err(io)?;
    let mut hash = Sha256::new();
    write_hashed(&mut file, &mut hash, MAGIC)?;
    recovery(
        point,
        RecoveryPoint::ExportWrite,
        RecoveryPoint::CrashExportWrite,
    )?;
    write_hashed(&mut file, &mut hash, &1u32.to_be_bytes())?;
    // Meta records are not copied: exports intentionally drop the local fence.
    let features = (u64::from_be_bytes(features) & !engine::meta::WRITE_FENCE).to_be_bytes();
    write_hashed(&mut file, &mut hash, &features)?;
    write_hashed(&mut file, &mut hash, &ts.0.to_be_bytes())?;
    write_record(&mut file, &mut hash, 0, b"domain", &db.meta.domain)?;
    for (tag, ks) in [
        (1, Keyspace::Catalog),
        (2, Keyspace::Rows),
        (3, Keyspace::Index),
    ] {
        for item in mvcc::VersionedRange::new(
            read.as_ref(),
            ks,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            ts,
        )? {
            let (key, value) = item?;
            write_record(&mut file, &mut hash, tag, &key, &value)?;
        }
    }
    for item in read.range(
        Keyspace::Seq,
        Bound::Unbounded,
        Bound::Unbounded,
        Direction::Forward,
    )? {
        let (key, value) = item?;
        if !checked.stale_counters.contains(&key) {
            write_record(&mut file, &mut hash, 4, &key, &value)?;
        }
    }
    let digest: [u8; 32] = hash.finalize().into();
    file.write_all(&digest).map_err(io)?;
    recovery(
        point,
        RecoveryPoint::ExportSync,
        RecoveryPoint::CrashExportSync,
    )?;
    file.sync_all().map_err(io)?;
    recovery(
        point,
        RecoveryPoint::ExportVerify,
        RecoveryPoint::CrashExportVerify,
    )?;
    verify(&stage)?;
    recovery(point, RecoveryPoint::ExportPublish, RecoveryPoint::None)?;
    // Match legacy's atomic, no-clobber publication and retained staging proof.
    fs::hard_link(&stage, path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            CoreError::Exists
        } else {
            io(e)
        }
    })?;
    if point == RecoveryPoint::CrashExportPublish {
        crash_now();
    }
    Ok(digest)
}

struct Record {
    tag: u8,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// A disk-backed read view lets the existing format/catalog validators run
/// without retaining exported rows or rebuilding an in-memory database.
struct ExportRead {
    file: Arc<Mutex<File>>,
    end: u64,
    features: u64,
    ts: Ts,
    domain: [u8; 16],
}

impl ExportRead {
    fn open(path: &Path) -> Result<Self, CoreError> {
        let mut file = File::open(path).map_err(io)?;
        let len = file.metadata().map_err(io)?.len();
        if len < HEADER + TRAILER {
            return Err(corrupt("short file"));
        }
        let mut header = [0; HEADER as usize];
        file.read_exact(&mut header).map_err(io)?;
        if &header[..8] != MAGIC || header[8..12] != 1u32.to_be_bytes() {
            return Err(CoreError::Unsupported);
        }
        if u64::from_be_bytes(header[12..20].try_into().unwrap()) & engine::meta::EXPORT_REFUSED != 0 {
            return Err(CoreError::Unsupported);
        }
        if u64::from_be_bytes(header[12..20].try_into().unwrap()) & engine::meta::WRITE_FENCE != 0 {
            return Err(corrupt("write_fence feature in export"));
        }
        let mut read = Self {
            file: Arc::new(Mutex::new(file)),
            end: len - TRAILER,
            features: u64::from_be_bytes(header[12..20].try_into().unwrap()),
            ts: Ts(u64::from_be_bytes(header[20..28].try_into().unwrap())),
            domain: [0; 16],
        };
        if read.features & engine::meta::NATIVE_OUTCOMES != 0 {
            return Err(CoreError::Unsupported);
        }
        let mut offset = HEADER;
        let domain = read
            .record(&mut offset)?
            .ok_or_else(|| corrupt("missing domain"))?;
        if domain.tag != 0 || domain.key != b"domain" {
            return Err(corrupt("missing domain"));
        }
        read.domain = domain
            .value
            .try_into()
            .map_err(|_| corrupt("domain length"))?;
        Ok(read)
    }

    fn record(&self, offset: &mut u64) -> Result<Option<Record>, CoreError> {
        if *offset == self.end {
            return Ok(None);
        }
        if self.end.saturating_sub(*offset) < 9 {
            return Err(corrupt("record framing"));
        }
        let mut file = self.file.lock().map_err(|_| CoreError::Fenced)?;
        file.seek(SeekFrom::Start(*offset)).map_err(io)?;
        let mut framing = [0; 9];
        file.read_exact(&mut framing).map_err(io)?;
        let len = u32::from_be_bytes(framing[..4].try_into().unwrap()) as u64;
        let key_len = u32::from_be_bytes(framing[5..].try_into().unwrap()) as u64;
        if len < 5 || len > self.end - *offset - 4 || key_len == 0 || key_len > len - 5 {
            return Err(corrupt("record lengths"));
        }
        let mut key = vec![0; key_len as usize];
        let mut value = vec![0; (len - 5 - key_len) as usize];
        file.read_exact(&mut key).map_err(io)?;
        file.read_exact(&mut value).map_err(io)?;
        *offset += 4 + len;
        Ok(Some(Record {
            tag: framing[4],
            key,
            value,
        }))
    }

    fn verify(&self) -> Result<Snapshot, CoreError> {
        // Check bytes in bounded chunks, including header and record framing.
        let mut file = self.file.lock().map_err(|_| CoreError::Fenced)?;
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        let mut hash = Sha256::new();
        let mut remaining = self.end;
        let mut buffer = [0; 64 * 1024];
        while remaining != 0 {
            let n = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..n]).map_err(io)?;
            hash.update(&buffer[..n]);
            remaining -= n as u64;
        }
        let mut digest = [0; 32];
        file.read_exact(&mut digest).map_err(io)?;
        if hash.finalize()[..] != digest {
            return Err(corrupt("checksum"));
        }
        drop(file);
        let mut previous = None;
        let mut offset = HEADER;
        while let Some(record) = self.record(&mut offset)? {
            let Record { tag, key, .. } = record;
            if tag > 4 || (tag == 0 && (offset != HEADER + 31 || key != b"domain")) {
                return Err(corrupt("record tag"));
            }
            if previous
                .as_ref()
                .is_some_and(|(old_tag, old_key): &(u8, Vec<u8>)| {
                    (*old_tag, old_key.as_slice()) >= (tag, key.as_slice())
                })
            {
                return Err(corrupt("record order"));
            }
            if matches!(tag, 2 | 3) && key.len() < 8 {
                return Err(corrupt("short row/index key"));
            }
            previous = Some((tag, key));
        }
        engine::meta::open_checks(self).map_err(v2::format_error)?;
        let mut snapshot = empty(u128::from_be_bytes(self.domain)).view;
        snapshot.schema = self.ts.0;
        Ok(snapshot)
    }
}

impl KvRead for ExportRead {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        if ks == Keyspace::Meta {
            return Ok(match key {
                b"family" => Some(2u32.to_be_bytes().to_vec()),
                b"subversion" => Some(1u32.to_be_bytes().to_vec()),
                b"features" => Some(self.features.to_be_bytes().to_vec()),
                b"domain" => Some(self.domain.to_vec()),
                b"engine" => Some(b"rocksdb".to_vec()),
                b"last_commit_ts" => Some(self.ts.0.to_be_bytes().to_vec()),
                _ => None,
            });
        }
        Ok(self
            .range(
                ks,
                Bound::Included(key),
                Bound::Included(key),
                Direction::Forward,
            )?
            .next()
            .transpose()?
            .map(|(_, v)| v))
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        if dir != Direction::Forward {
            return Err(KvError::Corrupt("reverse export scan".into()));
        }
        // Synthetic metadata is fixed-size; other keyspaces stream from disk.
        if ks == Keyspace::Meta {
            return Ok(Box::new(std::iter::empty()));
        }
        let tag = match ks {
            Keyspace::Catalog => 1,
            Keyspace::Rows => 2,
            Keyspace::Index => 3,
            Keyspace::Seq => 4,
            // Exports carry no other keyspace (verify rejects tags above 4), so
            // the open check's Kernel ranges need not stream the whole file.
            _ => return Ok(Box::new(std::iter::empty())),
        };
        let start = start.map(<[u8]>::to_vec);
        let end = end.map(<[u8]>::to_vec);
        let mut offset = HEADER;
        Ok(Box::new(std::iter::from_fn(move || {
            loop {
                let record = match self.record(&mut offset) {
                    Ok(Some(record)) => record,
                    Ok(None) => return None,
                    Err(e) => {
                        offset = self.end;
                        return Some(Err(KvError::Corrupt(e.to_string())));
                    }
                };
                if record.tag != tag {
                    continue;
                }
                let (key, value) = if tag == 4 {
                    (record.key, record.value)
                } else {
                    (
                        codec::user_key_seek_at(&record.key, self.ts),
                        mvcc::encode_live(&record.value),
                    )
                };
                let lower = match &start {
                    Bound::Included(s) => &key >= s,
                    Bound::Excluded(s) => &key > s,
                    Bound::Unbounded => true,
                };
                let upper = match &end {
                    Bound::Included(e) => &key <= e,
                    Bound::Excluded(e) => &key < e,
                    Bound::Unbounded => true,
                };
                if lower && upper {
                    return Some(Ok((key, value)));
                }
            }
        })))
    }
}

/// V2 has no native kernel rows. Its verification summary carries the source
/// domain and snapshot timestamp (schema); SQL and roles are validated on disk.
pub(super) fn verify(path: &Path) -> Result<Snapshot, CoreError> {
    ExportRead::open(path)?.verify()
}

pub(super) fn restore(
    export: &Path,
    path: &Path,
    limits: Limits,
    point: RecoveryPoint,
) -> Result<Admin, CoreError> {
    if limits.admitted == 0 || limits.selected_keys == 0 {
        return Err(CoreError::Limit);
    }
    let read = ExportRead::open(export)?;
    read.verify()?;
    if read.features & (engine::meta::REPLICATED | engine::meta::EXPORT_REFUSED) != 0 {
        return Err(CoreError::Unsupported);
    }
    fs::create_dir(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            CoreError::Exists
        } else {
            io(e)
        }
    })?;
    let kv = engine::kv::Kv::new(Arc::new(engine::kv_rocksdb::RocksKv::open(
        path,
        64 * 1024 * 1024,
    )?));
    // Keep the directory fail-closed until every streamed batch is durable and
    // checked. Existing destinations are never initialized or replaced.
    recovery(
        point,
        RecoveryPoint::InstallCreated,
        RecoveryPoint::CrashInstallCreated,
    )?;
    let mut offset = HEADER;
    let mut batch = WriteBatch::default();
    while let Some(record) = read.record(&mut offset)? {
        let ks = match record.tag {
            0 => continue,
            1 => Keyspace::Catalog,
            2 => Keyspace::Rows,
            3 => Keyspace::Index,
            4 => Keyspace::Seq,
            _ => return Err(corrupt("record tag")),
        };
        let (key, value) = if ks == Keyspace::Seq {
            (record.key, record.value)
        } else {
            (
                codec::user_key_seek_at(&record.key, read.ts),
                mvcc::encode_live(&record.value),
            )
        };
        batch.put(ks, key, value);
        if batch.byte_size() >= 1024 * 1024 {
            kv.write(std::mem::take(&mut batch), Durability::NoSync)?;
        }
    }
    let mut domain = [0; 16];
    getrandom::fill(&mut domain).map_err(io)?;
    // Match legacy restore: provenance names the immediate source, not its origin.
    let mut kernel = empty(u128::from_be_bytes(domain));
    kernel.origin = Some(u128::from_be_bytes(read.domain));
    let metadata = records(&kernel)?.remove(&vec![0]).expect("metadata record");
    batch.put(
        Keyspace::Kernel,
        codec::user_key_seek_at(&[0], read.ts),
        mvcc::encode_live(&metadata),
    );
    for (key, value) in [
        (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
        (
            b"subversion",
            engine::meta::INITIAL_SUBVERSION.to_be_bytes().to_vec(),
        ),
        (b"domain", domain.to_vec()),
        (b"engine", b"rocksdb".to_vec()),
    ] {
        batch.put(Keyspace::Meta, key.to_vec(), value);
    }
    engine::meta::put_features(&mut batch, read.features).map_err(v2::format_error)?;
    batch.put(
        Keyspace::Meta,
        b"last_commit_ts".to_vec(),
        read.ts.0.to_be_bytes().to_vec(),
    );
    kv.write(batch, Durability::Sync)?;
    read.verify()?;
    engine::meta::open_checks(&kv).map_err(v2::format_error)?;
    let mut marker = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.join("GIGGLES_FORMAT.tmp"))
        .map_err(io)?;
    marker.write_all(b"giggles-v2\n").map_err(io)?;
    marker.sync_all().map_err(io)?;
    drop(marker);
    recovery(
        point,
        RecoveryPoint::InstallStaged,
        RecoveryPoint::CrashInstallStaged,
    )?;
    fs::rename(path.join("GIGGLES_FORMAT.tmp"), path.join("GIGGLES_FORMAT")).map_err(io)?;
    recovery(
        point,
        RecoveryPoint::InstallCommitted,
        RecoveryPoint::CrashInstallCommitted,
    )?;
    drop(kv);
    Admin::open(path, limits)
}

#[cfg(test)]
#[path = "v2_export_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "v2_export_native_tests.rs"]
mod native_tests;

#[cfg(test)]
#[path = "v2_export_task_tests.rs"]
mod task_tests;
