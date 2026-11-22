//! UNPUBLISHED candidate payload v1, independent of every storage envelope.
//! No namespace or outer indexed version is made readable by this codec.
//!
//! Wire: `GD42IDX!`, LE u32 revision, LE u32 descriptor count; descriptors
//! (LE u64 id/table, u32-framed UTF-8 name, u32 column count, u64 columns,
//! u32 key encoding); u32 map count; maps (u64 id, u32 entry count, pairs of
//! u32-framed key/PK bytes). IDs and keys strictly increase, columns retain
//! descriptor order. All frames are direct, no serde intermediate values.
//!
//! decode_pending admits structure and exposes only immutable descriptors.
//! Only consuming validate_complete (or decode_validated) discloses usable state.
//! Its trusted complete rows/catalog
//! have the parent module's proof obligations, including stable ID ownership,
//! null/scalar admissibility and canonical key encoding. A truncated SELECT page
//! is never authoritative. Unsupported Numeric/Real/DoublePrecision remain out.
//! Payload structure cannot prove snapshot completeness or authorize a caller.
use super::*;

const MAGIC: &[u8; 8] = b"GD42IDX!";
pub(crate) const CANDIDATE_REVISION: u32 = 1;
const UNIQUE_REVISION: u32 = 2;
/// Includes all framing, descriptors and entries, unlike the entry-only quota.
pub(crate) const MAX_PAYLOAD_BYTES: usize = MAX_BYTES;

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = add(self.pos, n)?;
        let value = self.bytes.get(self.pos..end).ok_or(Error::Corrupt)?;
        self.pos = end;
        Ok(value)
    }
    fn u32(&mut self) -> Result<u32> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }
    fn u64(&mut self) -> Result<u64> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }
    fn count(&mut self, max: usize) -> Result<usize> {
        let n = usize::try_from(self.u32()?).map_err(|_| Error::Limit)?;
        cap(n, max)?;
        Ok(n)
    }
    fn frame(&mut self, max: usize) -> Result<&'a [u8]> {
        let n = self.count(max)?;
        self.take(n)
    }
}
// Length-based conservative work reservation precedes any comparison, UTF-8
// traversal, copy or owned allocation. Covers two parsing passes, BTree insert
// comparisons and payload equality. Complete-base validation separately admits
// MAX_WORK, making the whole operation at most 2*MAX_WORK. Allocator internals
// and bounded/non-reentrant caller callbacks are outside this model.
fn admit(bytes: usize, count: usize, byte_limit: usize, work_limit: usize) -> Result<()> {
    cap(bytes, byte_limit.min(MAX_PAYLOAD_BYTES))?;
    cap(
        add(mul(bytes, 64)?, mul(mul(count, tree_comparisons(count))?, 2 * MAX_NAME)?)?,
        work_limit.min(MAX_WORK),
    )
}

// The first pass retains only descriptor IDs and borrowed names. It bounds
// their allocation by the bytes present before reading any descriptors.
// The complete owned payload is built only after this pass succeeds.
fn parse(bytes: &[u8], owned: bool, cancel: &mut impl FnMut() -> bool) -> Result<Sidecar> {
    tick(cancel)?;
    let mut r = Reader { bytes, pos: 0 };
    if r.take(8)? != MAGIC {
        return Err(Error::Corrupt);
    }
    let revision = r.u32()?;
    if ![CANDIDATE_REVISION, UNIQUE_REVISION].contains(&revision) {
        return Err(Error::Corrupt);
    }
    let mut any_unique = false;
    // Each descriptor needs at least 37 bytes, plus a 12-byte map header.
    let count = r.count((bytes.len() - r.pos) / 49)?;
    admit(bytes.len(), count, MAX_PAYLOAD_BYTES, MAX_WORK)?;
    let mut ids = Vec::new();
    let mut names = BTreeSet::new();
    let mut sidecar = Sidecar::default();
    for _ in 0..count {
        tick(cancel)?;
        let id = r.u64()?;
        let table = r.u64()?;
        let name = r.frame(MAX_NAME)?;
        if id == 0 || table == 0 || ids.last().is_some_and(|prior| id <= *prior) || name.is_empty() {
            return Err(Error::Descriptor);
        }
        let name_str = std::str::from_utf8(name).map_err(|_| Error::Descriptor)?;
        if !names.insert(name) {
            return Err(Error::Descriptor);
        }
        ids.push(id);
        let arity = r.count(MAX_ARITY)?;
        if arity == 0 {
            return Err(Error::Descriptor);
        }
        let mut columns = [0u64; MAX_ARITY];
        for j in 0..arity {
            tick(cancel)?;
            let col = r.u64()?;
            if col == 0 || columns[..j].contains(&col) {
                return Err(Error::Descriptor);
            }
            columns[j] = col;
        }
        let key_encoding = r.u32()?;
        if key_encoding != 1 {
            return Err(Error::Descriptor);
        }
        let unique = if revision == UNIQUE_REVISION {
            match r.take(1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(Error::Corrupt),
            }
        } else {
            false
        };
        any_unique |= unique;
        if owned {
            tick(cancel)?;
            sidecar.descriptors.push(Descriptor {
                id,
                table,
                name: name_str.to_owned(),
                columns: columns[..arity].to_vec(),
                key_encoding,
                unique,
            });
        }
    }
    if revision == UNIQUE_REVISION && !any_unique {
        return Err(Error::Corrupt);
    }
    if r.count(count)? != count {
        return Err(Error::Corrupt);
    }
    let mut candidates = 0;
    let mut total = 0;
    for id in ids {
        tick(cancel)?;
        if r.u64()? != id {
            return Err(Error::Corrupt);
        }
        let n = r.count(MAX_CANDIDATES)?;
        candidates = add(candidates, n)?;
        cap(candidates, MAX_CANDIDATES)?;
        let mut map = BTreeMap::new();
        let mut prior: Option<&[u8]> = None;
        for _ in 0..n {
            tick(cancel)?;
            let key = r.frame(MAX_KEY)?;
            let pk = r.frame(MAX_COMPONENT)?;
            component(pk)?;
            // Exact key shape and suffix semantics are rechecked against rows.
            if key.is_empty() {
                return Err(Error::Corrupt);
            }
            let size = add(key.len(), pk.len())?;
            cap(size, MAX_ENTRY)?;
            total = add(total, size)?;
            cap(total, MAX_BYTES)?;
            tick(cancel)?;
            if prior.is_some_and(|p| p >= key) {
                return Err(Error::Corrupt);
            }
            prior = Some(key);
            if owned {
                tick(cancel)?;
                map.insert(key.to_vec(), pk.to_vec());
            }
        }
        if owned {
            sidecar.entries.insert(id, map);
        }
    }
    if r.pos != bytes.len() {
        return Err(Error::Corrupt);
    }
    tick(cancel)?;
    Ok(sidecar)
}

/// Structurally admitted payload, deliberately without Clone, serde, Deref,
/// entry access or lookup. Descriptors describe projection, never base authority.
pub(crate) struct PendingDecode {
    candidate: Sidecar,
}
impl PendingDecode {
    // Names are metadata only: IDs and encoded index entries stay unchanged.
    // Complete catalog/row validation still runs after this decode-only upgrade.
    pub(crate) fn normalize_r1_names<'a>(&mut self, schema: impl Fn(u64) -> &'a str) {
        for descriptor in &mut self.candidate.descriptors {
            if !descriptor.name.contains('\0') {
                descriptor.name = super::super::schema_binding::relation_key(
                    schema(descriptor.table),
                    &descriptor.name,
                );
            }
        }
    }

    pub(crate) fn descriptors(&self) -> &[Descriptor] {
        self.candidate.descriptors()
    }

    /// The caller supplies ALL authoritative rows for ALL referenced tables,
    /// with canonical tuples in admitted descriptor order, plus its catalog.
    pub(crate) fn validate_complete(
        self,
        rows: &[BaseRow<'_>],
        catalog: &impl Catalog,
        mut cancel: impl FnMut() -> bool,
    ) -> Result<Sidecar> {
        self.candidate.validate(rows, catalog, &mut cancel)?;
        tick(&mut cancel)?;
        Ok(self.candidate)
    }
}

/// The entire allocation-free pass must succeed before any owned payload copy.
/// Late malformed entries and trailing data cannot disclose even descriptors.
pub(crate) fn decode_pending(
    bytes: &[u8],
    mut cancel: impl FnMut() -> bool,
) -> Result<PendingDecode> {
    cap(bytes.len(), MAX_PAYLOAD_BYTES)?;
    parse(bytes, false, &mut cancel)?;
    tick(&mut cancel)?;
    let candidate = parse(bytes, true, &mut cancel)?;
    tick(&mut cancel)?;
    Ok(PendingDecode { candidate })
}

/// Decode private state only after bounded structural admission and exact
/// recomputation against ALL authoritative base rows. No unchecked Sidecar is
/// returned, including when cancellation occurs at the final disclosure fence.
pub(crate) fn decode_validated(
    bytes: &[u8],
    rows: &[BaseRow<'_>],
    catalog: &impl Catalog,
    mut cancel: impl FnMut() -> bool,
) -> Result<Sidecar> {
    decode_pending(bytes, &mut cancel)?.validate_complete(rows, catalog, &mut cancel)
}

fn put32(out: &mut Vec<u8>, n: usize) -> Result<()> {
    out.extend_from_slice(&u32::try_from(n).map_err(|_| Error::Limit)?.to_le_bytes());
    Ok(())
}
fn frame(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    put32(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}
// Exact byte sizing reads lengths only. No clones, sorting or output allocation.
fn encoded_len(sidecar: &Sidecar, cancel: &mut impl FnMut() -> bool) -> Result<usize> {
    sidecar.stored_preflight()?;
    let unique = sidecar.descriptors.iter().any(|d| d.unique);
    let mut size = 20usize;
    for d in &sidecar.descriptors {
        tick(cancel)?;
        cap(d.name.len(), MAX_NAME)?;
        cap(d.columns.len(), MAX_ARITY)?;
        size = add(
            size,
            add(
                28 + usize::from(unique),
                add(d.name.len(), mul(d.columns.len(), 8)?)?,
            )?,
        )?;
    }
    for map in sidecar.entries.values() {
        tick(cancel)?;
        size = add(size, 12)?;
        for (key, pk) in map {
            tick(cancel)?;
            size = add(size, add(8, add(key.len(), pk.len())?)?)?;
        }
    }
    Ok(size)
}
/// Encode canonical private state after exact framing-inclusive sizing BEFORE
/// owned output allocation. This accepts only structurally canonical state;
/// callers still validate against their complete authoritative base snapshot.
pub(crate) fn encode(sidecar: &Sidecar, mut cancel: impl FnMut() -> bool) -> Result<Vec<u8>> {
    encode_with_limits(sidecar, &mut cancel, MAX_PAYLOAD_BYTES, MAX_WORK)
}
fn encode_with_limits(
    sidecar: &Sidecar,
    cancel: &mut impl FnMut() -> bool,
    byte_limit: usize,
    work_limit: usize,
) -> Result<Vec<u8>> {
    let size = encoded_len(sidecar, cancel)?;
    admit(size, sidecar.descriptors.len(), byte_limit, work_limit)?;
    tick(cancel)?;
    // Reject malformed model state before copying; serde remains a test model,
    // never an alternate untrusted codec API.
    let mut prior_id = 0;
    let mut names = BTreeSet::new();
    for d in &sidecar.descriptors {
        tick(cancel)?;
        if d.id <= prior_id
            || d.table == 0
            || d.name.is_empty()
            || d.columns.is_empty()
            || d.key_encoding != 1
            || !sidecar.entries.contains_key(&d.id)
        {
            return Err(Error::Descriptor);
        }
        prior_id = d.id;
        if !names.insert(&d.name) {
            return Err(Error::Descriptor);
        }
        for (j, col) in d.columns.iter().enumerate() {
            tick(cancel)?;
            if *col == 0 || d.columns[..j].contains(col) {
                return Err(Error::Descriptor);
            }
        }
    }
    if sidecar.entries.len() != sidecar.descriptors.len() {
        return Err(Error::Corrupt);
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(MAGIC);
    let unique = sidecar.descriptors.iter().any(|d| d.unique);
    put32(
        &mut out,
        if unique {
            UNIQUE_REVISION
        } else {
            CANDIDATE_REVISION
        } as usize,
    )?;
    put32(&mut out, sidecar.descriptors.len())?;
    for d in &sidecar.descriptors {
        tick(cancel)?;
        out.extend_from_slice(&d.id.to_le_bytes());
        out.extend_from_slice(&d.table.to_le_bytes());
        frame(&mut out, d.name.as_bytes())?;
        put32(&mut out, d.columns.len())?;
        for col in &d.columns {
            out.extend_from_slice(&col.to_le_bytes());
        }
        put32(&mut out, d.key_encoding as usize)?;
        if unique {
            out.push(u8::from(d.unique));
        }
    }
    put32(&mut out, sidecar.entries.len())?;
    for (id, map) in &sidecar.entries {
        tick(cancel)?;
        out.extend_from_slice(&id.to_le_bytes());
        put32(&mut out, map.len())?;
        for (key, pk) in map {
            tick(cancel)?;
            frame(&mut out, key)?;
            frame(&mut out, pk)?;
        }
    }
    if out.len() != size {
        return Err(Error::Corrupt);
    }
    parse(&out, false, cancel)?;
    tick(cancel)?;
    Ok(out)
}
