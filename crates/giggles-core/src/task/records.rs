//! Kernel namespaces `0x20..=0x2B`: key constructors and value codecs for the
//! protected task records (GD-0061 design section 6, table D-A, with the R4
//! ruling: `20/01`, `2A`, `2B` from section 3; `27`, `28`, `29` from section 7 D7.2).
//!
//! All integers are big-endian. Every value starts with version byte `01`.
//! Decoders bound every length against the remaining input before allocating,
//! reject trailing bytes and bad lengths (`CoreError::Corrupt`), and report an
//! unknown version as `CoreError::Unsupported`. Every key is fixed width except
//! membership (`21`), which ends in prefix-free primary-key bytes.
//!
//! Encoders take values that satisfy the bounds their decoders enforce (string
//! and byte lengths); a value outside them is a caller bug and panics rather
//! than writing bytes a reader would misparse.
//!
//! | Ns | User key | Value after `01` |
//! |---|---|---|
//! | 20/00 | `20 task 00` (10) | def_version, distinct_rows, transitions, next_seq, next_preview, denials, last_decided_ms u64; stored_previews u32 (61 bytes) |
//! | 20/01 | `20 task 01` (10) | rows_read, rows_returned, bytes_returned, next_read_seq, withheld_values u64 (41 bytes) |
//! | 21 | `21 task table pk` (17 + pk) | first_seq, last_seq u64; transitions u32 (21 bytes) |
//! | 22 | `22 sha256(domain16 ‖ identity)` (33) | kind, flags u8; identity B16; request_fp, accepted_fp [32]; decided_lower_ms, decided_upper_ms, retain_until_ms u64; payload B32 |
//! | 23 | `23 task seq` (17) | decision, request_kind, flags u8; task, seq, def_version, catalog_version, decided_ms u64; request_fp, effects_digest [32]; rows_affected, distinct_charged, transitions_charged, noop_rows u32; distinct_after, transitions_after, preview u64; undo_mode u8; restored, conflicts, superseded u32; op_id B8; issuer, executor, approver S8 |
//! | 24 | `24 task seq` (17) | flags, kind u8; outcome_key [32]; table u64; row_count u32; salt [16]; effects_digest [32]; decided_ms u64 (103 bytes) |
//! | 25 | `25 task seq row_no` (21) | flags u8; table u64; pk B16; version_before, version_written u64; before_hash, after_hash [32]; before B32 |
//! | 26 | `26 00` (2) | once_upper_watermark_s, once_identities u64; migrated_v1 u8 (18 bytes) |
//! | 27 | `27 task preview` (17) | kind, state u8; created_ms, expires_ms, def_version, table u64; schema_fp [32]; clock_pin i64; digest [32]; counts 4 x u32; applied_seq u64; creator B32; scope u8 + [32] (kind 02 only); surface u8; text B32; params B32 |
//! | 28 | `28 task preview row_no` (21) | class u8; table u64; pk B32; version_read, aux u64; after B32 |
//! | 29 | `29 task preview` (17) | state u8; approver B32; approver_oid u32; digest [32]; approved_ms, consumed_seq u64 |
//! | 2A | `2A task read_seq` (17) | the section 3 read-log field list (see [`ReadLogRecord`]) |
//! | 2B | `2B task read_seq page_no` (21) | wall_ms, rows_read, rows_returned, bytes, reserved_rows, reserved_bytes u64 (49 bytes) |
//!
//! "S8"/"S16" is a u8/u16 length followed by UTF-8; "B8"/"B16"/"B32" a length
//! followed by raw bytes.
use crate::CoreError;
use sha2::{Digest, Sha256};

/// Catalog object id of a task, allocated from the shared catalog id space.
/// The one definition; `task::definition` re-exports it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskObjectId(pub u64);

pub(crate) const NS_MIN: u8 = 0x20;
pub(crate) const NS_MAX: u8 = 0x2B;
pub(crate) const NS_AUTHORITY: u8 = 0x20;
pub(crate) const NS_MEMBERSHIP: u8 = 0x21;
pub(crate) const NS_OUTCOME: u8 = 0x22;
pub(crate) const NS_RECEIPT: u8 = 0x23;
pub(crate) const NS_CHANGE_HEADER: u8 = 0x24;
pub(crate) const NS_CHANGE_ROW: u8 = 0x25;
pub(crate) const NS_LEDGER: u8 = 0x26;
pub(crate) const NS_PREVIEW: u8 = 0x27;
pub(crate) const NS_PREVIEW_ROW: u8 = 0x28;
pub(crate) const NS_APPROVAL: u8 = 0x29;
pub(crate) const NS_READ_LOG: u8 = 0x2A;
pub(crate) const NS_DEBIT: u8 = 0x2B;

pub(crate) const AUTHORITY_MUTATION: u8 = 0;
pub(crate) const AUTHORITY_DISCLOSURE: u8 = 1;
pub(crate) const LEDGER_HEADER_KEY: [u8; 2] = [NS_LEDGER, 0];

/// Version byte that starts every value.
pub(crate) const VERSION: u8 = 1;

pub(crate) const MAX_IDENTITY: usize = 512;
pub(crate) const MAX_OUTCOME_PAYLOAD: usize = 2 * 1024 * 1024;
pub(crate) const MAX_OPERATION_ID: usize = 128;
pub(crate) const MAX_STATEMENT_TEXT: usize = 64 * 1024;
pub(crate) const MAX_READ_LOG_TEXT: usize = 1024;

/// Outcome flag bit 0: the once payload was blanked by retention.
pub(crate) const OUTCOME_PAYLOAD_BLANKED: u8 = 1;
/// Change-header flag bit 0: before-values were blanked by retention.
pub(crate) const CHANGE_BEFORE_BLANKED: u8 = 1;
/// Change-row flag bit 0: the row was a no-op.
pub(crate) const ROW_NOOP: u8 = 1;
/// Change-row flag bit 1: this row's before-value was blanked.
pub(crate) const ROW_BEFORE_BLANKED: u8 = 2;

/// Which record a Kernel key in `0x20..=0x2B` names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Namespace {
    MutationHead,
    DisclosureHead,
    Membership,
    Outcome,
    Receipt,
    ChangeHeader,
    ChangeRow,
    LedgerHeader,
    PreviewHeader,
    PreviewRow,
    Approval,
    ReadLog,
    Debit,
}

fn corrupt(what: &str, why: &str) -> CoreError {
    CoreError::Corrupt(format!("task record {what}: {why}"))
}

// ---------------------------------------------------------------- keys

fn task_key(ns: u8, task: TaskObjectId, tail: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::with_capacity(9 + tail.iter().map(|t| t.len()).sum::<usize>());
    key.push(ns);
    key.extend_from_slice(&task.0.to_be_bytes());
    for part in tail {
        key.extend_from_slice(part);
    }
    key
}

/// `20 | task | which`; `which` is [`AUTHORITY_MUTATION`] or [`AUTHORITY_DISCLOSURE`].
pub(crate) fn authority_key(task: TaskObjectId, which: u8) -> Vec<u8> {
    assert!(which <= AUTHORITY_DISCLOSURE, "authority head selector");
    task_key(NS_AUTHORITY, task, &[&[which]])
}
pub(crate) fn mutation_head_key(task: TaskObjectId) -> Vec<u8> {
    authority_key(task, AUTHORITY_MUTATION)
}
pub(crate) fn disclosure_head_key(task: TaskObjectId) -> Vec<u8> {
    authority_key(task, AUTHORITY_DISCLOSURE)
}
/// `21 | task | table | pk`; `pk` is the non-empty, prefix-free primary-key encoding.
pub(crate) fn membership_key(task: TaskObjectId, table: u64, pk: &[u8]) -> Vec<u8> {
    assert!(!pk.is_empty(), "membership primary key");
    task_key(NS_MEMBERSHIP, task, &[&table.to_be_bytes(), pk])
}
/// `22 | sha256(domain ‖ identity)`.
pub(crate) fn outcome_key(domain: [u8; 16], identity: &[u8]) -> Vec<u8> {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(identity);
    let mut key = Vec::with_capacity(33);
    key.push(NS_OUTCOME);
    key.extend_from_slice(&hash.finalize());
    key
}
pub(crate) fn receipt_key(task: TaskObjectId, seq: u64) -> Vec<u8> {
    task_key(NS_RECEIPT, task, &[&seq.to_be_bytes()])
}
pub(crate) fn change_header_key(task: TaskObjectId, seq: u64) -> Vec<u8> {
    task_key(NS_CHANGE_HEADER, task, &[&seq.to_be_bytes()])
}
pub(crate) fn change_row_key(task: TaskObjectId, seq: u64, row_no: u32) -> Vec<u8> {
    task_key(NS_CHANGE_ROW, task, &[&seq.to_be_bytes(), &row_no.to_be_bytes()])
}
pub(crate) fn preview_key(task: TaskObjectId, preview: u64) -> Vec<u8> {
    task_key(NS_PREVIEW, task, &[&preview.to_be_bytes()])
}
pub(crate) fn preview_row_key(task: TaskObjectId, preview: u64, row_no: u32) -> Vec<u8> {
    task_key(NS_PREVIEW_ROW, task, &[&preview.to_be_bytes(), &row_no.to_be_bytes()])
}
pub(crate) fn approval_key(task: TaskObjectId, preview: u64) -> Vec<u8> {
    task_key(NS_APPROVAL, task, &[&preview.to_be_bytes()])
}
pub(crate) fn read_log_key(task: TaskObjectId, read_seq: u64) -> Vec<u8> {
    task_key(NS_READ_LOG, task, &[&read_seq.to_be_bytes()])
}
pub(crate) fn debit_key(task: TaskObjectId, read_seq: u64, page_no: u32) -> Vec<u8> {
    task_key(NS_DEBIT, task, &[&read_seq.to_be_bytes(), &page_no.to_be_bytes()])
}

/// `[start, end)` covering every key of one task in a task-keyed namespace.
/// For the namespaces that are not keyed by task (`22`, `26`) it covers the
/// whole namespace.
pub(crate) fn task_prefix(ns: u8, task: TaskObjectId) -> (Vec<u8>, Vec<u8>) {
    assert!((NS_MIN..=NS_MAX).contains(&ns), "protected namespace");
    if matches!(ns, NS_OUTCOME | NS_LEDGER) {
        return (vec![ns], vec![ns + 1]);
    }
    let start = task_key(ns, task, &[]);
    let end = match task.0.checked_add(1) {
        Some(next) => task_key(ns, TaskObjectId(next), &[]),
        None => vec![ns + 1],
    };
    (start, end)
}

/// Classify a Kernel user key by namespace and exact length.
pub(crate) fn classify_key(key: &[u8]) -> Result<Namespace, CoreError> {
    let bad = |why| Err(corrupt("key", why));
    let Some(&ns) = key.first() else {
        return bad("empty");
    };
    let (namespace, ok) = match ns {
        NS_AUTHORITY => match (key.len(), key.last()) {
            (10, Some(&AUTHORITY_MUTATION)) => (Namespace::MutationHead, true),
            (10, Some(&AUTHORITY_DISCLOSURE)) => (Namespace::DisclosureHead, true),
            _ => (Namespace::MutationHead, false),
        },
        NS_MEMBERSHIP => (Namespace::Membership, key.len() > 17),
        NS_OUTCOME => (Namespace::Outcome, key.len() == 33),
        NS_RECEIPT => (Namespace::Receipt, key.len() == 17),
        NS_CHANGE_HEADER => (Namespace::ChangeHeader, key.len() == 17),
        NS_CHANGE_ROW => (Namespace::ChangeRow, key.len() == 21),
        NS_LEDGER => (Namespace::LedgerHeader, key == LEDGER_HEADER_KEY),
        NS_PREVIEW => (Namespace::PreviewHeader, key.len() == 17),
        NS_PREVIEW_ROW => (Namespace::PreviewRow, key.len() == 21),
        NS_APPROVAL => (Namespace::Approval, key.len() == 17),
        NS_READ_LOG => (Namespace::ReadLog, key.len() == 17),
        NS_DEBIT => (Namespace::Debit, key.len() == 21),
        _ => return bad("not a protected namespace"),
    };
    if ok { Ok(namespace) } else { bad("namespace length") }
}

/// Classify the key and fully decode the value as that namespace's record.
pub(crate) fn validate(key: &[u8], value: &[u8]) -> Result<(), CoreError> {
    match classify_key(key)? {
        Namespace::MutationHead => MutationHead::decode(value).map(drop),
        Namespace::DisclosureHead => DisclosureHead::decode(value).map(drop),
        Namespace::Membership => Membership::decode(value).map(drop),
        Namespace::Outcome => OutcomeRecord::decode(value).map(drop),
        Namespace::Receipt => {
            let receipt = ReceiptRecord::decode(value)?;
            if key != receipt_key(receipt.task, receipt.seq) {
                return Err(corrupt("receipt", "key does not match task and seq"));
            }
            Ok(())
        }
        Namespace::ChangeHeader => ChangeHeader::decode(value).map(drop),
        Namespace::ChangeRow => ChangeRow::decode(value).map(drop),
        Namespace::LedgerHeader => LedgerHeader::decode(value).map(drop),
        Namespace::PreviewHeader => PreviewHeader::decode(value).map(drop),
        Namespace::PreviewRow => PreviewRow::decode(value).map(drop),
        Namespace::Approval => ApprovalRecord::decode(value).map(drop),
        Namespace::ReadLog => ReadLogRecord::decode(value).map(drop),
        Namespace::Debit => DebitRecord::decode(value).map(drop),
    }
}

// ---------------------------------------------------------------- primitives

struct Writer(Vec<u8>);
impl Writer {
    fn new() -> Self {
        Self(vec![VERSION])
    }
    fn u8(&mut self, v: u8) -> &mut Self {
        self.0.push(v);
        self
    }
    fn u16(&mut self, v: u16) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn u32(&mut self, v: u32) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn u64(&mut self, v: u64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn i64(&mut self, v: i64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn raw(&mut self, v: &[u8]) -> &mut Self {
        self.0.extend_from_slice(v);
        self
    }
    fn b8(&mut self, v: &[u8]) -> &mut Self {
        let n = u8::try_from(v.len()).expect("task record field exceeds u8 length");
        self.u8(n).raw(v)
    }
    fn b16(&mut self, v: &[u8]) -> &mut Self {
        let n = u16::try_from(v.len()).expect("task record field exceeds u16 length");
        self.u16(n).raw(v)
    }
    fn b32(&mut self, v: &[u8]) -> &mut Self {
        let n = u32::try_from(v.len()).expect("task record field exceeds u32 length");
        self.u32(n).raw(v)
    }
    fn done(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.0)
    }
}

struct Reader<'a> {
    what: &'static str,
    rest: &'a [u8],
}
impl<'a> Reader<'a> {
    /// Checks the version byte. An absent value is corrupt; any version but
    /// [`VERSION`] is unsupported.
    fn new(what: &'static str, value: &'a [u8]) -> Result<Self, CoreError> {
        match value.split_first() {
            None => Err(corrupt(what, "empty value")),
            Some((&VERSION, rest)) => Ok(Self { what, rest }),
            Some(_) => Err(CoreError::Unsupported),
        }
    }
    fn err(&self, why: &str) -> CoreError {
        corrupt(self.what, why)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], CoreError> {
        if n > self.rest.len() {
            return Err(self.err("short value"));
        }
        let (head, tail) = self.rest.split_at(n);
        self.rest = tail;
        Ok(head)
    }
    fn arr<const N: usize>(&mut self) -> Result<[u8; N], CoreError> {
        Ok(self.take(N)?.try_into().expect("exact length"))
    }
    fn u8(&mut self) -> Result<u8, CoreError> {
        Ok(self.arr::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16, CoreError> {
        self.arr().map(u16::from_be_bytes)
    }
    fn u32(&mut self) -> Result<u32, CoreError> {
        self.arr().map(u32::from_be_bytes)
    }
    fn u64(&mut self) -> Result<u64, CoreError> {
        self.arr().map(u64::from_be_bytes)
    }
    fn i64(&mut self) -> Result<i64, CoreError> {
        self.arr().map(i64::from_be_bytes)
    }
    /// Length-prefixed bytes; the length is checked against `min..=max` and the
    /// remaining input before anything is allocated.
    fn sized(&mut self, len: usize, min: usize, max: usize) -> Result<Vec<u8>, CoreError> {
        if len < min || len > max {
            return Err(self.err("field length out of range"));
        }
        Ok(self.take(len)?.to_vec())
    }
    fn b8(&mut self, min: usize, max: usize) -> Result<Vec<u8>, CoreError> {
        let len = self.u8()? as usize;
        self.sized(len, min, max)
    }
    fn b16(&mut self, min: usize, max: usize) -> Result<Vec<u8>, CoreError> {
        let len = self.u16()? as usize;
        self.sized(len, min, max)
    }
    fn b32(&mut self, min: usize, max: usize) -> Result<Vec<u8>, CoreError> {
        let len = self.u32()? as usize;
        self.sized(len, min, max)
    }
    fn text(&self, bytes: Vec<u8>) -> Result<String, CoreError> {
        String::from_utf8(bytes).map_err(|_| self.err("invalid UTF-8"))
    }
    fn s8(&mut self) -> Result<String, CoreError> {
        let bytes = self.b8(0, u8::MAX as usize)?;
        self.text(bytes)
    }
    fn finish(self) -> Result<(), CoreError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(self.err("trailing bytes"))
        }
    }
}

// ---------------------------------------------------------------- 20/00, 20/01, 21

/// Mutation authority head, `20 | task | 00`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MutationHead {
    pub(crate) def_version: u64,
    pub(crate) distinct_rows: u64,
    pub(crate) transitions: u64,
    /// The first seq is 1.
    pub(crate) next_seq: u64,
    pub(crate) next_preview: u64,
    pub(crate) denials: u64,
    pub(crate) last_decided_ms: u64,
    pub(crate) stored_previews: u32,
}
impl MutationHead {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u64(self.def_version)
            .u64(self.distinct_rows)
            .u64(self.transitions)
            .u64(self.next_seq)
            .u64(self.next_preview)
            .u64(self.denials)
            .u64(self.last_decided_ms)
            .u32(self.stored_previews)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("mutation head", value)?;
        let out = Self {
            def_version: r.u64()?,
            distinct_rows: r.u64()?,
            transitions: r.u64()?,
            next_seq: r.u64()?,
            next_preview: r.u64()?,
            denials: r.u64()?,
            last_decided_ms: r.u64()?,
            stored_previews: r.u32()?,
        };
        r.finish()?;
        Ok(out)
    }
}

/// Disclosure authority head, `20 | task | 01`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DisclosureHead {
    pub(crate) rows_read: u64,
    pub(crate) rows_returned: u64,
    pub(crate) bytes_returned: u64,
    pub(crate) next_read_seq: u64,
    pub(crate) withheld_values: u64,
}
impl DisclosureHead {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u64(self.rows_read)
            .u64(self.rows_returned)
            .u64(self.bytes_returned)
            .u64(self.next_read_seq)
            .u64(self.withheld_values)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("disclosure head", value)?;
        let out = Self {
            rows_read: r.u64()?,
            rows_returned: r.u64()?,
            bytes_returned: r.u64()?,
            next_read_seq: r.u64()?,
            withheld_values: r.u64()?,
        };
        r.finish()?;
        Ok(out)
    }
}

/// Row membership, `21 | task | table | pk`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Membership {
    pub(crate) first_seq: u64,
    pub(crate) last_seq: u64,
    pub(crate) transitions: u32,
}
impl Membership {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u64(self.first_seq)
            .u64(self.last_seq)
            .u32(self.transitions)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("membership", value)?;
        let out = Self {
            first_seq: r.u64()?,
            last_seq: r.u64()?,
            transitions: r.u32()?,
        };
        r.finish()?;
        if out.first_seq > out.last_seq {
            return Err(corrupt("membership", "first_seq after last_seq"));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------- 22

/// Outcome payload; its variant is the persisted `kind` byte.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OutcomePayload {
    /// Kind 1: native `once` GDOU v1 bytes; empty when blanked.
    Once(Vec<u8>),
    /// Kind 2.
    Committed { task: TaskObjectId, seq: u64 },
    /// Kind 3; `rule` is `DenyRule::code()`.
    Denied { task: TaskObjectId, rule: u16, denial_no: u64 },
    /// Kind 4; `budget` is `Budget::code()`.
    BudgetExceeded { task: TaskObjectId, budget: u16, denial_no: u64 },
}
impl OutcomePayload {
    pub(crate) fn kind(&self) -> u8 {
        match self {
            Self::Once(_) => 1,
            Self::Committed { .. } => 2,
            Self::Denied { .. } => 3,
            Self::BudgetExceeded { .. } => 4,
        }
    }
    fn bytes(&self) -> Vec<u8> {
        let mut w = Vec::new();
        match self {
            Self::Once(bytes) => w.extend_from_slice(bytes),
            Self::Committed { task, seq } => {
                w.extend_from_slice(&task.0.to_be_bytes());
                w.extend_from_slice(&seq.to_be_bytes());
            }
            Self::Denied { task, rule: code, denial_no }
            | Self::BudgetExceeded { task, budget: code, denial_no } => {
                w.extend_from_slice(&task.0.to_be_bytes());
                w.extend_from_slice(&code.to_be_bytes());
                w.extend_from_slice(&denial_no.to_be_bytes());
            }
        }
        w
    }
    fn parse(kind: u8, bytes: Vec<u8>) -> Result<Self, CoreError> {
        let fixed = |n: usize| {
            if bytes.len() == n {
                Ok(())
            } else {
                Err(corrupt("outcome", "payload length for kind"))
            }
        };
        let u64_at = |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap());
        Ok(match kind {
            1 => Self::Once(bytes),
            2 => {
                fixed(16)?;
                Self::Committed { task: TaskObjectId(u64_at(0)), seq: u64_at(8) }
            }
            3 | 4 => {
                fixed(18)?;
                let task = TaskObjectId(u64_at(0));
                let code = u16::from_be_bytes([bytes[8], bytes[9]]);
                let denial_no = u64_at(10);
                if kind == 3 {
                    Self::Denied { task, rule: code, denial_no }
                } else {
                    Self::BudgetExceeded { task, budget: code, denial_no }
                }
            }
            _ => return Err(corrupt("outcome", "unknown kind")),
        })
    }
}

/// Operation outcome, `22 | sha256(domain ‖ identity)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutcomeRecord {
    /// Bit 0 ([`OUTCOME_PAYLOAD_BLANKED`]) is valid for kind 1 only.
    pub(crate) flags: u8,
    /// 1..=512 bytes.
    pub(crate) identity: Vec<u8>,
    pub(crate) request_fp: [u8; 32],
    pub(crate) accepted_fp: [u8; 32],
    pub(crate) decided_lower_ms: u64,
    pub(crate) decided_upper_ms: u64,
    pub(crate) retain_until_ms: u64,
    pub(crate) payload: OutcomePayload,
}
impl OutcomeRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        assert!(
            (1..=MAX_IDENTITY).contains(&self.identity.len()),
            "outcome identity length"
        );
        Writer::new()
            .u8(self.payload.kind())
            .u8(self.flags)
            .b16(&self.identity)
            .raw(&self.request_fp)
            .raw(&self.accepted_fp)
            .u64(self.decided_lower_ms)
            .u64(self.decided_upper_ms)
            .u64(self.retain_until_ms)
            .b32(&self.payload.bytes())
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("outcome", value)?;
        let kind = r.u8()?;
        let flags = r.u8()?;
        let identity = r.b16(1, MAX_IDENTITY)?;
        let request_fp = r.arr()?;
        let accepted_fp = r.arr()?;
        let decided_lower_ms = r.u64()?;
        let decided_upper_ms = r.u64()?;
        let retain_until_ms = r.u64()?;
        let payload = r.b32(0, MAX_OUTCOME_PAYLOAD)?;
        r.finish()?;
        if flags & !OUTCOME_PAYLOAD_BLANKED != 0 || (kind != 1 && flags != 0) {
            return Err(corrupt("outcome", "flags"));
        }
        if kind == 1 && (flags & OUTCOME_PAYLOAD_BLANKED != 0) != payload.is_empty() {
            return Err(corrupt("outcome", "payload empty iff blanked"));
        }
        if decided_lower_ms > decided_upper_ms {
            return Err(corrupt("outcome", "decision interval"));
        }
        Ok(Self {
            flags,
            identity,
            request_fp,
            accepted_fp,
            decided_lower_ms,
            decided_upper_ms,
            retain_until_ms,
            payload: OutcomePayload::parse(kind, payload)?,
        })
    }
}

// ---------------------------------------------------------------- 23

/// Receipt, `23 | task | seq`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReceiptRecord {
    pub(crate) decision: u8,
    pub(crate) request_kind: u8,
    pub(crate) flags: u8,
    pub(crate) task: TaskObjectId,
    pub(crate) seq: u64,
    pub(crate) def_version: u64,
    pub(crate) catalog_version: u64,
    pub(crate) decided_ms: u64,
    pub(crate) request_fp: [u8; 32],
    pub(crate) effects_digest: [u8; 32],
    pub(crate) rows_affected: u32,
    pub(crate) distinct_charged: u32,
    pub(crate) transitions_charged: u32,
    pub(crate) noop_rows: u32,
    pub(crate) distinct_after: u64,
    pub(crate) transitions_after: u64,
    /// 0 = none.
    pub(crate) preview: u64,
    /// Undo fields (R4); zero for a non-undo receipt.
    pub(crate) undo_mode: u8,
    pub(crate) restored: u32,
    pub(crate) conflicts: u32,
    pub(crate) superseded: u32,
    /// 1..=128 bytes.
    pub(crate) op_id: Vec<u8>,
    pub(crate) issuer: String,
    pub(crate) executor: String,
    /// Persisted as an empty S8; `Some("")` encodes as `None`.
    pub(crate) approver: Option<String>,
}
impl ReceiptRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        assert!(
            (1..=MAX_OPERATION_ID).contains(&self.op_id.len()),
            "receipt operation id length"
        );
        Writer::new()
            .u8(self.decision)
            .u8(self.request_kind)
            .u8(self.flags)
            .u64(self.task.0)
            .u64(self.seq)
            .u64(self.def_version)
            .u64(self.catalog_version)
            .u64(self.decided_ms)
            .raw(&self.request_fp)
            .raw(&self.effects_digest)
            .u32(self.rows_affected)
            .u32(self.distinct_charged)
            .u32(self.transitions_charged)
            .u32(self.noop_rows)
            .u64(self.distinct_after)
            .u64(self.transitions_after)
            .u64(self.preview)
            .u8(self.undo_mode)
            .u32(self.restored)
            .u32(self.conflicts)
            .u32(self.superseded)
            .b8(&self.op_id)
            .b8(self.issuer.as_bytes())
            .b8(self.executor.as_bytes())
            .b8(self.approver.as_deref().unwrap_or("").as_bytes())
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("receipt", value)?;
        let out = Self {
            decision: r.u8()?,
            request_kind: r.u8()?,
            flags: r.u8()?,
            task: TaskObjectId(r.u64()?),
            seq: r.u64()?,
            def_version: r.u64()?,
            catalog_version: r.u64()?,
            decided_ms: r.u64()?,
            request_fp: r.arr()?,
            effects_digest: r.arr()?,
            rows_affected: r.u32()?,
            distinct_charged: r.u32()?,
            transitions_charged: r.u32()?,
            noop_rows: r.u32()?,
            distinct_after: r.u64()?,
            transitions_after: r.u64()?,
            preview: r.u64()?,
            undo_mode: r.u8()?,
            restored: r.u32()?,
            conflicts: r.u32()?,
            superseded: r.u32()?,
            op_id: r.b8(1, MAX_OPERATION_ID)?,
            issuer: r.s8()?,
            executor: r.s8()?,
            approver: Some(r.s8()?).filter(|a| !a.is_empty()),
        };
        r.finish()?;
        Ok(out)
    }
}

// ---------------------------------------------------------------- 24, 25

/// Change-record header, `24 | task | seq`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ChangeHeader {
    /// Bit 0 ([`CHANGE_BEFORE_BLANKED`]).
    pub(crate) flags: u8,
    /// 1 change, 2 undo.
    pub(crate) kind: u8,
    /// The 32 hash bytes of the operation's `22` key.
    pub(crate) outcome_key: [u8; 32],
    pub(crate) table: u64,
    pub(crate) row_count: u32,
    pub(crate) salt: [u8; 16],
    pub(crate) effects_digest: [u8; 32],
    pub(crate) decided_ms: u64,
}
impl ChangeHeader {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u8(self.flags)
            .u8(self.kind)
            .raw(&self.outcome_key)
            .u64(self.table)
            .u32(self.row_count)
            .raw(&self.salt)
            .raw(&self.effects_digest)
            .u64(self.decided_ms)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("change header", value)?;
        let out = Self {
            flags: r.u8()?,
            kind: r.u8()?,
            outcome_key: r.arr()?,
            table: r.u64()?,
            row_count: r.u32()?,
            salt: r.arr()?,
            effects_digest: r.arr()?,
            decided_ms: r.u64()?,
        };
        r.finish()?;
        if out.flags & !CHANGE_BEFORE_BLANKED != 0 {
            return Err(corrupt("change header", "flags"));
        }
        if !matches!(out.kind, 1 | 2) {
            return Err(corrupt("change header", "kind"));
        }
        Ok(out)
    }
}

/// Change-record row, `25 | task | seq | row_no`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ChangeRow {
    /// Bit 0 ([`ROW_NOOP`]), bit 1 ([`ROW_BEFORE_BLANKED`]).
    pub(crate) flags: u8,
    pub(crate) table: u64,
    pub(crate) pk: Vec<u8>,
    pub(crate) version_before: u64,
    pub(crate) version_written: u64,
    pub(crate) before_hash: [u8; 32],
    pub(crate) after_hash: [u8; 32],
    /// Row payload encoding; empty iff [`ROW_BEFORE_BLANKED`].
    pub(crate) before: Vec<u8>,
}
impl ChangeRow {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u8(self.flags)
            .u64(self.table)
            .b16(&self.pk)
            .u64(self.version_before)
            .u64(self.version_written)
            .raw(&self.before_hash)
            .raw(&self.after_hash)
            .b32(&self.before)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("change row", value)?;
        let out = Self {
            flags: r.u8()?,
            table: r.u64()?,
            pk: r.b16(1, u16::MAX as usize)?,
            version_before: r.u64()?,
            version_written: r.u64()?,
            before_hash: r.arr()?,
            after_hash: r.arr()?,
            before: r.b32(0, crate::relational::MAX_ROW_BYTES)?,
        };
        r.finish()?;
        if out.flags & !(ROW_NOOP | ROW_BEFORE_BLANKED) != 0 {
            return Err(corrupt("change row", "flags"));
        }
        if (out.flags & ROW_BEFORE_BLANKED != 0) != out.before.is_empty() {
            return Err(corrupt("change row", "before empty iff blanked"));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------- 26

/// Ledger header, `26 00`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LedgerHeader {
    pub(crate) once_upper_watermark_s: u64,
    pub(crate) once_identities: u64,
    pub(crate) migrated_v1: bool,
}
impl LedgerHeader {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u64(self.once_upper_watermark_s)
            .u64(self.once_identities)
            .u8(u8::from(self.migrated_v1))
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("ledger header", value)?;
        let once_upper_watermark_s = r.u64()?;
        let once_identities = r.u64()?;
        let migrated_v1 = match r.u8()? {
            0 => false,
            1 => true,
            _ => return Err(corrupt("ledger header", "migrated_v1")),
        };
        r.finish()?;
        Ok(Self { once_upper_watermark_s, once_identities, migrated_v1 })
    }
}

// ---------------------------------------------------------------- 27, 28, 29

/// Undo preview scope: 00 task, 01 one operation (with its outcome-key hash).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PreviewScope {
    pub(crate) kind: u8,
    pub(crate) outcome: [u8; 32],
}

/// Stored preview header, `27 | task | preview`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PreviewHeader {
    /// 01 change, 02 undo.
    pub(crate) kind: u8,
    /// 00 open, 01 applied.
    pub(crate) state: u8,
    pub(crate) created_ms: u64,
    pub(crate) expires_ms: u64,
    pub(crate) def_version: u64,
    pub(crate) table: u64,
    pub(crate) schema_fp: [u8; 32],
    /// `i64::MIN` = none.
    pub(crate) clock_pin: i64,
    pub(crate) digest: [u8; 32],
    /// Class 0, 1, 2 and settled.
    pub(crate) counts: [u32; 4],
    pub(crate) applied_seq: u64,
    /// Role name, UTF-8.
    pub(crate) creator: String,
    /// Present iff `kind == 2`.
    pub(crate) scope: Option<PreviewScope>,
    /// 0 SQL, 1 native.
    pub(crate) surface: u8,
    /// At most 64 KiB.
    pub(crate) text: String,
    /// Row payload encoding of the parameters.
    pub(crate) params: Vec<u8>,
}
impl PreviewHeader {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u8(self.kind)
            .u8(self.state)
            .u64(self.created_ms)
            .u64(self.expires_ms)
            .u64(self.def_version)
            .u64(self.table)
            .raw(&self.schema_fp)
            .i64(self.clock_pin)
            .raw(&self.digest);
        for count in self.counts {
            w.u32(count);
        }
        w.u64(self.applied_seq).b32(self.creator.as_bytes());
        if let Some(scope) = &self.scope {
            w.u8(scope.kind).raw(&scope.outcome);
        }
        w.u8(self.surface)
            .b32(self.text.as_bytes())
            .b32(&self.params)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("preview header", value)?;
        let kind = r.u8()?;
        if !matches!(kind, 1 | 2) {
            return Err(corrupt("preview header", "kind"));
        }
        let state = r.u8()?;
        if state > 1 {
            return Err(corrupt("preview header", "state"));
        }
        let created_ms = r.u64()?;
        let expires_ms = r.u64()?;
        let def_version = r.u64()?;
        let table = r.u64()?;
        let schema_fp = r.arr()?;
        let clock_pin = r.i64()?;
        let digest = r.arr()?;
        let counts = [r.u32()?, r.u32()?, r.u32()?, r.u32()?];
        let applied_seq = r.u64()?;
        let creator = r.b32(0, usize::MAX)?;
        let creator = r.text(creator)?;
        let scope = if kind == 2 {
            let scope = PreviewScope { kind: r.u8()?, outcome: r.arr()? };
            if scope.kind > 1 {
                return Err(corrupt("preview header", "scope"));
            }
            Some(scope)
        } else {
            None
        };
        let surface = r.u8()?;
        if surface > 1 {
            return Err(corrupt("preview header", "surface"));
        }
        let text = r.b32(0, MAX_STATEMENT_TEXT)?;
        let text = r.text(text)?;
        let params = r.b32(0, usize::MAX)?;
        r.finish()?;
        Ok(Self {
            kind,
            state,
            created_ms,
            expires_ms,
            def_version,
            table,
            schema_fp,
            clock_pin,
            digest,
            counts,
            applied_seq,
            creator,
            scope,
            surface,
            text,
            params,
        })
    }
}

/// Stored preview row, `28 | task | preview | row_no`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PreviewRow {
    /// 00 change or restore, 01 conflict, 02 superseded.
    pub(crate) class: u8,
    pub(crate) table: u64,
    pub(crate) pk: Vec<u8>,
    pub(crate) version_read: u64,
    /// Conflict reason, or superseded seq.
    pub(crate) aux: u64,
    /// Full row payload; class 00 only (empty otherwise).
    pub(crate) after: Vec<u8>,
}
impl PreviewRow {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u8(self.class)
            .u64(self.table)
            .b32(&self.pk)
            .u64(self.version_read)
            .u64(self.aux)
            .b32(&self.after)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("preview row", value)?;
        let out = Self {
            class: r.u8()?,
            table: r.u64()?,
            pk: r.b32(1, usize::MAX)?,
            version_read: r.u64()?,
            aux: r.u64()?,
            after: r.b32(0, crate::relational::MAX_ROW_BYTES)?,
        };
        r.finish()?;
        if out.class > 2 {
            return Err(corrupt("preview row", "class"));
        }
        if out.class != 0 && !out.after.is_empty() {
            return Err(corrupt("preview row", "after-value outside class 00"));
        }
        Ok(out)
    }
}

/// Approval, `29 | task | preview`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ApprovalRecord {
    /// 01 active, 02 revoked, 03 consumed.
    pub(crate) state: u8,
    pub(crate) approver: String,
    pub(crate) approver_oid: u32,
    pub(crate) digest: [u8; 32],
    pub(crate) approved_ms: u64,
    pub(crate) consumed_seq: u64,
}
impl ApprovalRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u8(self.state)
            .b32(self.approver.as_bytes())
            .u32(self.approver_oid)
            .raw(&self.digest)
            .u64(self.approved_ms)
            .u64(self.consumed_seq)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("approval", value)?;
        let state = r.u8()?;
        if !(1..=3).contains(&state) {
            return Err(corrupt("approval", "state"));
        }
        let approver = r.b32(0, usize::MAX)?;
        let out = Self {
            state,
            approver: r.text(approver)?,
            approver_oid: r.u32()?,
            digest: r.arr()?,
            approved_ms: r.u64()?,
            consumed_seq: r.u64()?,
        };
        r.finish()?;
        Ok(out)
    }
}

// ---------------------------------------------------------------- 2A, 2B

/// One referenced column in a read-log record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadColumn {
    pub(crate) column_id: u64,
    pub(crate) position_bits: u16,
}

/// Read log, `2A | task | read_seq` (design section 3, part 6).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadLogRecord {
    pub(crate) wall_ms: u64,
    pub(crate) snapshot_ts: u64,
    pub(crate) credential_fingerprint: [u8; 32],
    pub(crate) def_version: u64,
    pub(crate) catalog_version: u64,
    /// 0 pgwire simple, 1 pgwire extended, 2 native text, 3 http, 4 grpc.
    pub(crate) entry: u8,
    pub(crate) class: u8,
    pub(crate) statement_sha256: [u8; 32],
    /// Length of the full statement text.
    pub(crate) text_len: u32,
    /// The first at most 1,024 bytes of the text, cut at a character boundary.
    pub(crate) text: String,
    pub(crate) param_count: u16,
    pub(crate) table_id: u64,
    pub(crate) columns: Vec<ReadColumn>,
    pub(crate) rows_scanned: u64,
    pub(crate) charged_rows_read: u64,
    pub(crate) charged_rows_returned: u64,
    pub(crate) charged_bytes: u64,
    pub(crate) released_rows: u64,
    pub(crate) released_bytes: u64,
    pub(crate) masked_values: u64,
    pub(crate) withheld_untrusted: u64,
    pub(crate) untrusted_released: u64,
    pub(crate) pages: u32,
    /// 0 open, 1 complete, 2 error, 3 denied, 4 cancelled, 5 budget.
    pub(crate) outcome: u8,
    pub(crate) sqlstate: [u8; 5],
    pub(crate) left_rows_read: u64,
    pub(crate) left_rows_returned: u64,
    pub(crate) left_bytes: u64,
}
impl ReadLogRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        assert!(self.text.len() <= MAX_READ_LOG_TEXT, "read-log text length");
        let mut w = Writer::new();
        w.u64(self.wall_ms)
            .u64(self.snapshot_ts)
            .raw(&self.credential_fingerprint)
            .u64(self.def_version)
            .u64(self.catalog_version)
            .u8(self.entry)
            .u8(self.class)
            .raw(&self.statement_sha256)
            .u32(self.text_len)
            .b16(self.text.as_bytes())
            .u16(self.param_count)
            .u64(self.table_id)
            .u16(u16::try_from(self.columns.len()).expect("read-log column count"));
        for column in &self.columns {
            w.u64(column.column_id).u16(column.position_bits);
        }
        w.u64(self.rows_scanned)
            .u64(self.charged_rows_read)
            .u64(self.charged_rows_returned)
            .u64(self.charged_bytes)
            .u64(self.released_rows)
            .u64(self.released_bytes)
            .u64(self.masked_values)
            .u64(self.withheld_untrusted)
            .u64(self.untrusted_released)
            .u32(self.pages)
            .u8(self.outcome)
            .raw(&self.sqlstate)
            .u64(self.left_rows_read)
            .u64(self.left_rows_returned)
            .u64(self.left_bytes)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("read log", value)?;
        let wall_ms = r.u64()?;
        let snapshot_ts = r.u64()?;
        let credential_fingerprint = r.arr()?;
        let def_version = r.u64()?;
        let catalog_version = r.u64()?;
        let entry = r.u8()?;
        if entry > 4 {
            return Err(corrupt("read log", "entry"));
        }
        let class = r.u8()?;
        let statement_sha256 = r.arr()?;
        let text_len = r.u32()?;
        let text = r.b16(0, MAX_READ_LOG_TEXT)?;
        let text = r.text(text)?;
        if text.len() as u64 > u64::from(text_len) {
            return Err(corrupt("read log", "stored text longer than the statement"));
        }
        let param_count = r.u16()?;
        let table_id = r.u64()?;
        let count = r.u16()? as usize;
        // Bound the column list by the remaining input before allocating it.
        if count * 10 > r.rest.len() {
            return Err(corrupt("read log", "short value"));
        }
        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            columns.push(ReadColumn { column_id: r.u64()?, position_bits: r.u16()? });
        }
        let out = Self {
            wall_ms,
            snapshot_ts,
            credential_fingerprint,
            def_version,
            catalog_version,
            entry,
            class,
            statement_sha256,
            text_len,
            text,
            param_count,
            table_id,
            columns,
            rows_scanned: r.u64()?,
            charged_rows_read: r.u64()?,
            charged_rows_returned: r.u64()?,
            charged_bytes: r.u64()?,
            released_rows: r.u64()?,
            released_bytes: r.u64()?,
            masked_values: r.u64()?,
            withheld_untrusted: r.u64()?,
            untrusted_released: r.u64()?,
            pages: r.u32()?,
            outcome: r.u8()?,
            sqlstate: r.arr()?,
            left_rows_read: r.u64()?,
            left_rows_returned: r.u64()?,
            left_bytes: r.u64()?,
        };
        r.finish()?;
        if out.outcome > 5 {
            return Err(corrupt("read log", "outcome"));
        }
        Ok(out)
    }
}

/// Disclosure debit, `2B | task | read_seq | page_no`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DebitRecord {
    pub(crate) wall_ms: u64,
    pub(crate) rows_read: u64,
    pub(crate) rows_returned: u64,
    pub(crate) bytes: u64,
    pub(crate) reserved_rows: u64,
    pub(crate) reserved_bytes: u64,
}
impl DebitRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        Writer::new()
            .u64(self.wall_ms)
            .u64(self.rows_read)
            .u64(self.rows_returned)
            .u64(self.bytes)
            .u64(self.reserved_rows)
            .u64(self.reserved_bytes)
            .done()
    }
    pub(crate) fn decode(value: &[u8]) -> Result<Self, CoreError> {
        let mut r = Reader::new("debit", value)?;
        let out = Self {
            wall_ms: r.u64()?,
            rows_read: r.u64()?,
            rows_returned: r.u64()?,
            bytes: r.u64()?,
            reserved_rows: r.u64()?,
            reserved_bytes: r.u64()?,
        };
        r.finish()?;
        Ok(out)
    }
}

#[cfg(test)]
#[path = "records_tests.rs"]
mod records_tests;
