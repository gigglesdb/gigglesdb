//! Track A owns this module.
//! Range export and install (GD-0075 R5-04, R5-05; R5-00d).
//!
//! Export writes, from ONE RocksDB snapshot, one SST file per keyspace interval the range owns
//! (keys.rs `cf_interval`) and a manifest holding the range's local Meta keys, its descriptor
//! and its applied position at that snapshot.
//!
//! Install replaces the range with an export: (1) a synced pending-install marker
//! `Meta/range/<id>/install`; (2) per keyspace a synced delete of the exported descriptor's
//! interval, then ingestion of its SST (move_files=false, allow_global_seqno=true); (3) one
//! synced batch that replaces the range's local Meta keys with the exported ones (which sets the
//! applied position), ORs the range's features into the node's `Meta/features` and clears the
//! marker. Step 2 is not atomic across keyspaces, so `RangeStore::open` finds a marker and
//! completes the install from the source directory when it is still intact, or else rolls the
//! range back to empty: descriptor kept, no applied position, and a marker with an empty source
//! that makes `apply` refuse the range until a new snapshot is installed.
//!
//! Keys of an older, wider descriptor of this range that fall outside the exported descriptor
//! are not deleted: after a split they belong to the sibling range.
//!
//! Install refuses (before writing anything) an export of another range or another database
//! (domain), one written by a newer format subversion, one whose position is behind the range's
//! current one, and one whose bounds overlap another range stored on this node.
//!
//! Manifest file `range.manifest` (transfer format, version 1, strict):
//! `[ver=1][range id BE8][domain 16 bytes][subversion BE4][applied index BE8][applied term BE8]
//!  [descriptor: len BE4 + bytes]
//!  [meta: len BE4 + WriteBatch::encode of Put(Meta, key, value) for every local key except
//!  install][n BE2][(space tag u8, keys BE8, bytes BE8, file flag u8, file: len BE2 + UTF-8)]*n`
//! with one entry per range space the descriptor owns, in `SPACES` order.
//! Install marker value (persisted, version 1, strict):
//! `[ver=1][descriptor: len BE4 + bytes][source dir: len BE4 + UTF-8]`; an empty source means
//! "rolled back, needs a snapshot".
#[cfg(test)]
thread_local! {
    static CHECK_SCRATCH_PATHS: std::cell::RefCell<Vec<std::path::PathBuf>> = const { std::cell::RefCell::new(Vec::new()) };
}

use std::{
    ops::Bound,
    path::{Path, PathBuf},
};

use super::keys::{RawInterval, cf_interval, keyspace_of};
use super::local::{decode_applied, local_interval};
use super::store::{RangeStore, read_applied, read_descriptor, union_node_features};
use crate::CoreError;
use crate::storage::engine::{
    kv::{Direction, Durability, Keyspace, KvBackend, KvError, KvRead, Op, WriteBatch},
    kv_rocksdb::{SstInstall, SstPart},
    meta::{RangeLocalKey, check_range_value, parse_range_local_key, range_local_key},
};
use crate::storage::range_api::{RangeDescriptor, RangeId, RangeSpace};
use crate::storage::replicated::AppliedPosition;

struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const MANIFEST: &str = "range.manifest";
const VERSION: u8 = 1;
const SPACES: [RangeSpace; 6] = [
    RangeSpace::Catalog,
    RangeSpace::Kernel,
    RangeSpace::Seq,
    RangeSpace::Gc,
    RangeSpace::Rows,
    RangeSpace::Index,
];

fn corrupt(message: impl std::fmt::Display) -> CoreError {
    CoreError::Corrupt(format!("range transfer: {message}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Part {
    space: RangeSpace,
    keys: u64,
    bytes: u64,
    file: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Manifest {
    range: RangeId,
    domain: [u8; 16],
    subversion: u32,
    applied: AppliedPosition,
    descriptor: RangeDescriptor,
    /// Puts of every local Meta key except `install`.
    meta: WriteBatch,
    parts: Vec<Part>,
}

/// The spaces a descriptor owns and their raw intervals, in `SPACES` order.
pub(super) fn intervals(descriptor: &RangeDescriptor) -> Vec<(RangeSpace, RawInterval)> {
    SPACES
        .into_iter()
        .filter_map(|space| cf_interval(descriptor, space).map(|interval| (space, interval)))
        .collect()
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CoreError> {
        if self.0.len() < n {
            return Err(corrupt("truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, CoreError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CoreError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, CoreError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, CoreError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<&'a [u8], CoreError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn finish(&self) -> Result<(), CoreError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(corrupt("trailing bytes"))
        }
    }
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![VERSION];
        out.extend_from_slice(&self.range.0.to_be_bytes());
        out.extend_from_slice(&self.domain);
        out.extend_from_slice(&self.subversion.to_be_bytes());
        out.extend_from_slice(&self.applied.index.to_be_bytes());
        out.extend_from_slice(&self.applied.term.to_be_bytes());
        put_bytes(
            &mut out,
            &self.descriptor.encode().expect("validated descriptor"),
        );
        put_bytes(&mut out, &self.meta.encode());
        out.extend_from_slice(&(self.parts.len() as u16).to_be_bytes());
        for part in &self.parts {
            out.push(part.space.tag());
            out.extend_from_slice(&part.keys.to_be_bytes());
            out.extend_from_slice(&part.bytes.to_be_bytes());
            match &part.file {
                None => out.push(0),
                Some(name) => {
                    out.push(1);
                    out.extend_from_slice(&(name.len() as u16).to_be_bytes());
                    out.extend_from_slice(name.as_bytes());
                }
            }
        }
        out
    }

    /// Strict decode plus every consistency rule install relies on.
    fn decode(bytes: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader(bytes);
        if r.u8()? != VERSION {
            return Err(corrupt("unknown manifest version"));
        }
        let range = RangeId(r.u64()?);
        let domain = r.take(16)?.try_into().unwrap();
        let subversion = r.u32()?;
        let applied = AppliedPosition {
            index: r.u64()?,
            term: r.u64()?,
        };
        let descriptor = RangeDescriptor::decode(r.bytes()?).map_err(corrupt)?;
        let meta = WriteBatch::decode(r.bytes()?).map_err(corrupt)?;
        let mut parts = Vec::new();
        for _ in 0..r.u16()? {
            let space = RangeSpace::from_tag(r.u8()?).ok_or_else(|| corrupt("unknown space"))?;
            let keys = r.u64()?;
            let bytes = r.u64()?;
            let file = match r.u8()? {
                0 => None,
                1 => {
                    let n = r.u16()? as usize;
                    let name = std::str::from_utf8(r.take(n)?).map_err(corrupt)?;
                    if name.is_empty()
                        || name.contains(['/', '\\', ':'])
                        || name.starts_with('.')
                        || name == MANIFEST
                    {
                        return Err(corrupt("bad file name"));
                    }
                    Some(name.to_owned())
                }
                _ => return Err(corrupt("bad file flag")),
            };
            parts.push(Part {
                space,
                keys,
                bytes,
                file,
            });
        }
        r.finish()?;
        let manifest = Self {
            range,
            domain,
            subversion,
            applied,
            descriptor,
            meta,
            parts,
        };
        manifest.check()?;
        Ok(manifest)
    }

    fn check(&self) -> Result<(), CoreError> {
        if self.descriptor.id != self.range {
            return Err(corrupt("descriptor of another range"));
        }
        let spaces: Vec<RangeSpace> = intervals(&self.descriptor)
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        if spaces != self.parts.iter().map(|p| p.space).collect::<Vec<_>>() {
            return Err(corrupt("parts do not match the descriptor"));
        }
        let mut names: Vec<&str> = self
            .parts
            .iter()
            .filter_map(|p| p.file.as_deref())
            .collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err(corrupt("one file in two parts"));
        }
        let mut applied = None;
        let mut descriptor = None;
        let mut previous: Option<&[u8]> = None;
        for op in self.meta.ops() {
            let Op::Put {
                ks: Keyspace::Meta,
                key,
                value,
            } = op
            else {
                return Err(corrupt("meta part holds a non-Meta put"));
            };
            if previous.is_some_and(|p| p >= key.as_slice()) {
                return Err(corrupt("meta keys out of order"));
            }
            previous = Some(key);
            if let Some((id, sub)) = parse_range_local_key(key)
                && id == self.range
            {
                check_range_value(id, &sub, value, u64::MAX).map_err(corrupt)?;
            }
            match parse_range_local_key(key) {
                Some((id, RangeLocalKey::Install)) if id == self.range => {
                    return Err(corrupt("meta part holds an install marker"));
                }
                Some((id, RangeLocalKey::Applied)) if id == self.range => {
                    applied = decode_applied(value);
                }
                Some((id, RangeLocalKey::Descriptor)) if id == self.range => {
                    descriptor = Some(value.clone());
                }
                Some((id, _)) if id == self.range => {}
                _ => return Err(corrupt("meta part holds another range's key")),
            }
        }
        if applied != Some(self.applied) {
            return Err(corrupt("applied position mismatch"));
        }
        if descriptor.as_deref() != Some(self.descriptor.encode().expect("decoded").as_slice()) {
            return Err(corrupt("descriptor mismatch"));
        }
        Ok(())
    }

    /// Validate all row SSTs before even writing an install marker. RocksDB's
    /// Rust API has no SST reader: ingest into an isolated scratch DB, then
    /// stream it with a bounded cache, retaining just one entry at a time.
    /// This also runs when recovery reads the manifest again.
    fn check_row_files(&self, source: &Path) -> Result<(), CoreError> {
        let wide = self.meta.ops().iter().any(|op| {
            matches!(op,
            Op::Put { key, value, .. }
                if matches!(parse_range_local_key(key), Some((_, RangeLocalKey::Features)))
                && value.as_slice().try_into().map(u64::from_be_bytes).unwrap_or(0)
                    & crate::storage::engine::meta::WIDE_ROWS != 0)
        });
        if wide {
            return Ok(());
        }
        for part in &self.parts {
            if part.space != RangeSpace::Rows {
                continue;
            }
            let Some(file) = &part.file else {
                continue;
            };
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).map_err(corrupt)?;
            let scratch = Scratch(std::env::temp_dir().join(format!(
                "range-sst-check-{:032x}",
                u128::from_be_bytes(nonce)
            )));
            #[cfg(test)]
            CHECK_SCRATCH_PATHS.with_borrow_mut(|paths| paths.push(scratch.0.clone()));
            let kv = crate::storage::engine::kv_rocksdb::RocksKv::open(&scratch.0, 1024 * 1024)
                .map_err(kv_error)?;
            kv.ingest_sst(&[(
                Keyspace::Rows,
                Bound::Unbounded,
                Bound::Unbounded,
                Some(source.join(file)),
            )])
            .map_err(kv_error)?;
            for item in kv.range(
                Keyspace::Rows,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
            )? {
                let (_, value) = item?;
                if !crate::storage::engine::codec::range_stored_row_fits(&value) {
                    return Err(corrupt("row payload exceeds range limit without WIDE_ROWS"));
                }
            }
        }
        Ok(())
    }

    fn read(source: &Path) -> Result<Self, CoreError> {
        let bytes = std::fs::read(source.join(MANIFEST)).map_err(crate::storage::io)?;
        let manifest = Self::decode(&bytes)?;
        for part in &manifest.parts {
            if let Some(name) = &part.file {
                let size = std::fs::metadata(source.join(name)).map(|m| m.len()).ok();
                if size != Some(part.bytes) {
                    return Err(corrupt(format!("missing or resized file {name}")));
                }
            }
        }
        manifest.check_row_files(source)?;
        Ok(manifest)
    }
}

fn encode_marker(descriptor: &RangeDescriptor, source: &Path) -> Result<Vec<u8>, CoreError> {
    let source = source
        .to_str()
        .ok_or_else(|| corrupt("source path is not UTF-8"))?;
    let mut out = vec![VERSION];
    put_bytes(&mut out, &descriptor.encode().map_err(corrupt)?);
    put_bytes(&mut out, source.as_bytes());
    Ok(out)
}

fn decode_marker(bytes: &[u8]) -> Result<(RangeDescriptor, PathBuf), CoreError> {
    let mut r = Reader(bytes);
    if r.u8()? != VERSION {
        return Err(corrupt("unknown install marker version"));
    }
    let descriptor = RangeDescriptor::decode(r.bytes()?).map_err(corrupt)?;
    let source = std::str::from_utf8(r.bytes()?).map_err(corrupt)?.into();
    r.finish()?;
    Ok((descriptor, source))
}

fn kv_error(error: KvError) -> CoreError {
    error.into()
}

impl RangeStore {
    /// Export `range` into the empty or new directory `target`; returns the applied position
    /// the export is consistent with.
    pub fn export(&self, range: RangeId, target: &Path) -> Result<AppliedPosition, CoreError> {
        if std::fs::read_dir(target).is_ok_and(|mut entries| entries.next().is_some()) {
            return Err(corrupt("export target is not empty"));
        }
        let descriptor = self
            .descriptor(range)?
            .ok_or_else(|| corrupt(format!("unknown range {}", range.0)))?;
        let spaces = intervals(&descriptor);
        let parts: Vec<SstPart> = spaces
            .iter()
            .map(|(space, (lo, hi))| (keyspace_of(*space), lo.clone(), hi.clone()))
            .collect();
        let export = self.kv().export_sst(target, &parts, |snapshot| {
            // The parts were chosen from a descriptor read before the snapshot: refuse when an
            // apply changed it in between (the caller retries).
            let at = read_descriptor(snapshot, range).map_err(to_kv)?;
            if at.as_ref() != Some(&descriptor) {
                return Err(KvError::Corrupt(
                    "range descriptor changed during export".into(),
                ));
            }
            let applied = read_applied(snapshot, range)
                .map_err(to_kv)?
                .ok_or_else(|| KvError::Corrupt("range has no applied position".into()))?;
            let (lo, hi) = local_interval(range);
            let mut meta = WriteBatch::default();
            for item in snapshot.range(
                Keyspace::Meta,
                Bound::Included(&lo),
                Bound::Excluded(&hi),
                Direction::Forward,
            )? {
                let (key, value) = item?;
                if matches!(
                    parse_range_local_key(&key),
                    Some((_, RangeLocalKey::Install))
                ) {
                    return Err(KvError::Corrupt("range has an unfinished install".into()));
                }
                meta.put(Keyspace::Meta, key, value);
            }
            let subversion = node_subversion(snapshot).map_err(to_kv)?;
            Ok((applied, meta, subversion))
        });
        let export = export.map_err(kv_error)?;
        let (applied, meta, subversion) = export.at_snapshot;
        let parts = spaces
            .iter()
            .zip(&export.files)
            .map(|((space, _), file)| Part {
                space: *space,
                keys: file.keys,
                bytes: file.bytes,
                file: file
                    .path
                    .as_ref()
                    .and_then(|p| p.file_name())
                    .map(|n| n.to_string_lossy().into_owned()),
            })
            .collect();
        let manifest = Manifest {
            range,
            domain: self.domain(),
            subversion,
            applied,
            descriptor,
            meta,
            parts,
        };
        write_synced(&target.join(MANIFEST), &manifest.encode())?;
        Ok(applied)
    }

    /// Replace `range` with the export in `source`; returns the installed applied position.
    /// The caller keeps the range's leader engine closed (no writer may touch the range).
    pub fn install(&self, range: RangeId, source: &Path) -> Result<AppliedPosition, CoreError> {
        let source = &std::path::absolute(source).map_err(crate::storage::io)?;
        let manifest = Manifest::read(source)?;
        let lock = self.range_lock(range)?;
        let _guard = lock
            .lock()
            .map_err(|_| CoreError::Corrupt("range apply lock poisoned".into()))?;
        let _installs = self.installs_lock()?;
        self.can_install(range, &manifest)?;
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            range_local_key(range, &RangeLocalKey::Install),
            encode_marker(&manifest.descriptor, source)?,
        );
        self.kv().write(batch, Durability::Sync)?;
        self.finish_install(&manifest, source, Step::Done)?;
        Ok(manifest.applied)
    }

    /// GD-0075 P9-2c: store `descriptor` as a range that needs a snapshot when no range of
    /// its id is stored here: its descriptor plus an install marker with an empty source (the
    /// rolled-back state of `recover_installs`), so `apply` refuses the range until an install
    /// of its snapshot clears the marker. For a split child this replica learned of only from
    /// its parent's snapshot (the parent's lineage record). No overlap fence: the range holds
    /// no data or position yet (its interval was the parent's until the parent's install
    /// narrowed it), and its own install fences against every stored range. Returns false,
    /// writing nothing, when the range is already stored.
    pub fn create_needs_snapshot(&self, descriptor: &RangeDescriptor) -> Result<bool, CoreError> {
        let lock = self.range_lock(descriptor.id)?;
        let _guard = lock
            .lock()
            .map_err(|_| CoreError::Corrupt("range apply lock poisoned".into()))?;
        let _installs = self.installs_lock()?;
        if self.descriptor(descriptor.id)?.is_some() {
            return Ok(false);
        }
        let mut batch = replace_local(descriptor.id);
        batch.put(
            Keyspace::Meta,
            range_local_key(descriptor.id, &RangeLocalKey::Descriptor),
            descriptor.encode().map_err(corrupt)?,
        );
        batch.put(
            Keyspace::Meta,
            range_local_key(descriptor.id, &RangeLocalKey::Install),
            encode_marker(descriptor, Path::new(""))?,
        );
        self.kv().write(batch, Durability::Sync)?;
        Ok(true)
    }

    /// Whether this range is fenced while waiting for a snapshot install.
    pub fn needs_snapshot(&self, range: RangeId) -> Result<bool, CoreError> {
        Ok(self
            .kv()
            .get(
                Keyspace::Meta,
                &range_local_key(range, &RangeLocalKey::Install),
            )?
            .is_some())
    }

    /// The install fences; run under the range's apply lock before the marker is written.
    fn can_install(&self, range: RangeId, manifest: &Manifest) -> Result<(), CoreError> {
        if manifest.range != range {
            return Err(corrupt("export of another range"));
        }
        if manifest.domain != self.domain() {
            return Err(corrupt("export of another database"));
        }
        if manifest.subversion > node_subversion(self.kv().as_ref())? {
            return Err(corrupt("export needs a newer format subversion"));
        }
        if self
            .applied(range)?
            .is_some_and(|current| manifest.applied.index < current.index)
        {
            return Err(corrupt("export is behind the range's applied position"));
        }
        for other in self.ranges()? {
            if other != range
                && let Some(descriptor) = self.descriptor(other)?
                && overlaps(&descriptor, &manifest.descriptor)
            {
                return Err(corrupt(format!("export overlaps range {}", other.0)));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn install_until(
        &self,
        range: RangeId,
        source: &Path,
        stop: Step,
    ) -> Result<(), CoreError> {
        let manifest = Manifest::read(source)?;
        self.can_install(range, &manifest)?;
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            range_local_key(range, &RangeLocalKey::Install),
            encode_marker(&manifest.descriptor, source)?,
        );
        self.kv().write(batch, Durability::Sync)?;
        self.finish_install(&manifest, source, stop)
    }

    /// Steps 2 and 3 of install; repeating them converges.
    fn finish_install(
        &self,
        manifest: &Manifest,
        source: &Path,
        #[cfg_attr(not(test), allow(unused_variables))] stop: Step,
    ) -> Result<(), CoreError> {
        let mut files = manifest.parts.iter();
        let mut data: Vec<SstInstall> = Vec::new();
        for (space, (lo, hi)) in intervals(&manifest.descriptor) {
            let part = files.next().expect("checked parts");
            data.push((
                keyspace_of(space),
                lo,
                hi,
                part.file.as_ref().map(|n| source.join(n)),
            ));
        }
        #[cfg(test)]
        if stop == Step::Marker {
            return Ok(());
        }
        for part in &data {
            self.kv()
                .ingest_sst(std::slice::from_ref(part))
                .map_err(kv_error)?;
            #[cfg(test)]
            if stop == Step::FirstPart {
                return Ok(());
            }
        }
        #[cfg(test)]
        if stop == Step::Data {
            return Ok(());
        }
        let mut batch = replace_local(manifest.range);
        let mut features = 0;
        for op in manifest.meta.ops() {
            if let Op::Put { key, value, .. } = op {
                if matches!(
                    parse_range_local_key(key),
                    Some((_, RangeLocalKey::Features))
                ) {
                    features |= value
                        .as_slice()
                        .try_into()
                        .map(u64::from_be_bytes)
                        .map_err(|_| corrupt("features length"))?;
                }
                batch.put(Keyspace::Meta, key.clone(), value.clone());
            }
        }
        let _features = self.features_lock()?;
        union_node_features(self.kv().as_ref(), &mut batch, features)?;
        Ok(self.kv().write(batch, Durability::Sync)?)
    }

    /// Finish or roll back every install a crash interrupted (called by `open`).
    pub(super) fn recover_installs(&self) -> Result<(), CoreError> {
        let mut marked = Vec::new();
        for item in self.kv().range(
            Keyspace::Meta,
            Bound::Included(b"range/".as_slice()),
            Bound::Excluded(b"range0".as_slice()),
            Direction::Forward,
        )? {
            let (key, value) = item?;
            if let Some((id, RangeLocalKey::Install)) = parse_range_local_key(&key) {
                marked.push((id, value));
            }
        }
        for (range, value) in marked {
            let (descriptor, source) = decode_marker(&value)?;
            if source.as_os_str().is_empty() {
                // Rolled back earlier: the range waits for a new snapshot.
                continue;
            }
            match Manifest::read(&source) {
                Ok(manifest) if manifest.range == range && manifest.descriptor == descriptor => {
                    if let Err(error) = self.finish_install(&manifest, &source, Step::Done) {
                        eprintln!(
                            "range {}: install recovery failed, rolling back: {error:?}",
                            range.0
                        );
                        self.roll_back_install(&descriptor)?;
                    }
                }
                _ => self.roll_back_install(&descriptor)?,
            }
        }
        Ok(())
    }

    /// Empty the range's data and local keys and keep its descriptor plus a marker with no
    /// source: the range needs a new snapshot. `apply` refuses it until an install clears the
    /// marker, so log entries never re-apply over the rolled-back state.
    fn roll_back_install(&self, descriptor: &RangeDescriptor) -> Result<(), CoreError> {
        let data: Vec<SstInstall> = intervals(descriptor)
            .into_iter()
            .map(|(space, (lo, hi))| (keyspace_of(space), lo, hi, None))
            .collect();
        self.kv().ingest_sst(&data).map_err(kv_error)?;
        let mut batch = replace_local(descriptor.id);
        batch.put(
            Keyspace::Meta,
            range_local_key(descriptor.id, &RangeLocalKey::Descriptor),
            descriptor.encode().map_err(corrupt)?,
        );
        batch.put(
            Keyspace::Meta,
            range_local_key(descriptor.id, &RangeLocalKey::Install),
            encode_marker(descriptor, Path::new(""))?,
        );
        Ok(self.kv().write(batch, Durability::Sync)?)
    }
}

/// A batch deleting every local key of `range`, the install marker included: the batch that
/// carries it is the install's commit point.
fn replace_local(range: RangeId) -> WriteBatch {
    let (lo, hi) = local_interval(range);
    let mut batch = WriteBatch::default();
    batch.delete_range(Keyspace::Meta, lo, hi);
    batch
}

/// Whether two descriptors' range-key intervals intersect.
fn overlaps(a: &RangeDescriptor, b: &RangeDescriptor) -> bool {
    let below = |start: &[u8], end: &Option<Vec<u8>>| end.as_deref().is_none_or(|end| start < end);
    below(&a.start, &b.end) && below(&b.start, &a.end)
}

/// The node's format subversion (`Meta/subversion`, u32 BE).
fn node_subversion(kv: &dyn KvRead) -> Result<u32, CoreError> {
    let bytes = kv
        .get(Keyspace::Meta, b"subversion")?
        .ok_or_else(|| corrupt("node subversion missing"))?;
    Ok(u32::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| corrupt("node subversion length"))?,
    ))
}

fn to_kv(error: CoreError) -> KvError {
    KvError::Corrupt(format!("{error:?}"))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), CoreError> {
    use std::io::Write;
    let mut file = std::fs::File::create(path).map_err(crate::storage::io)?;
    file.write_all(bytes).map_err(crate::storage::io)?;
    file.sync_all().map_err(crate::storage::io)
}

/// Where a test install stops (simulated crash points).
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Step {
    Marker,
    FirstPart,
    Data,
    Done,
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
