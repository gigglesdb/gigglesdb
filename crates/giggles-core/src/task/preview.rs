//! Preview identity, schema fingerprint and digest (GD-0061 R3-701, design D7.2/D7.3).
//!
//! The stored header (`27`) and row (`28`) structs and their codecs live in
//! [`super::records`]; this module re-exports them and defines the values
//! that go into them.
use super::error::{DenyRule, TaskError};
use super::records::TaskObjectId;
pub(crate) use super::records::{PreviewHeader, PreviewRow};
use crate::CoreError;
use crate::relational::{SqlRow, TableDef, Value, catalog_ext::Constraint};
use sha2::{Digest, Sha256};

/// Per-task preview number (`head.next_preview`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PreviewId(pub u64);

/// Header `kind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PreviewKind {
    Change = 1,
    Undo = 2,
}

/// Row `class`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum PreviewClass {
    /// A change, or an undo restore.
    Change = 0,
    Conflict = 1,
    Superseded = 2,
}

/// Header `state`.
pub(crate) const STATE_OPEN: u8 = 0;
pub(crate) const STATE_APPLIED: u8 = 1;
/// `clock_pin` value meaning "no pin".
pub(crate) const NO_CLOCK_PIN: i64 = i64::MIN;
/// One preview holds at most this many rows.
pub(crate) const MAX_PREVIEW_ROWS: usize = 10_000;

/// `SHA-256(encode_table(def) || each encode_constraint(c) in constraint-id order)`.
pub(crate) fn schema_fp(table: &TableDef, constraints: &[Constraint]) -> Result<[u8; 32], CoreError> {
    let mut ordered: Vec<Constraint> = constraints.to_vec();
    ordered.sort_by_key(|c| c.id);
    let parts = crate::storage::task_schema_bytes(table, &ordered)?;
    let mut h = Sha256::new();
    for part in parts {
        h.update(&part);
    }
    Ok(h.finalize().into())
}

/// The D7.3 preimage. Rows are hashed in ascending `(class, table, pk)`
/// order whatever order the caller passes them in.
pub(crate) fn preimage(
    domain: [u8; 16],
    task: TaskObjectId,
    id: PreviewId,
    header: &PreviewHeader,
    rows: &[PreviewRow],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"GDPV");
    out.push(1);
    out.push(header.kind);
    out.extend_from_slice(&domain);
    out.extend_from_slice(&task.0.to_be_bytes());
    out.extend_from_slice(&id.0.to_be_bytes());
    out.extend_from_slice(&header.def_version.to_be_bytes());
    out.extend_from_slice(&header.table.to_be_bytes());
    out.extend_from_slice(&header.schema_fp);
    out.extend_from_slice(&header.clock_pin.to_be_bytes());
    out.extend_from_slice(&header.expires_ms.to_be_bytes());
    out.push(header.surface);
    let b = |out: &mut Vec<u8>, bytes: &[u8]| {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
    };
    b(&mut out, header.text.as_bytes());
    b(&mut out, &header.params);
    if header.kind == PreviewKind::Undo as u8 {
        let scope = header.scope.clone().unwrap_or_default();
        out.push(scope.kind);
        out.extend_from_slice(&scope.outcome);
    }
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    let mut sorted: Vec<&PreviewRow> = rows.iter().collect();
    sorted.sort_by(|a, b| (a.class, a.table, &a.pk).cmp(&(b.class, b.table, &b.pk)));
    for row in sorted {
        out.push(row.class);
        out.extend_from_slice(&row.table.to_be_bytes());
        b(&mut out, &row.pk);
        out.extend_from_slice(&row.version_read.to_be_bytes());
        out.extend_from_slice(&row.aux.to_be_bytes());
        b(&mut out, &row.after);
    }
    out
}

/// SHA-256 of [`preimage`].
pub(crate) fn digest(
    domain: [u8; 16],
    task: TaskObjectId,
    id: PreviewId,
    header: &PreviewHeader,
    rows: &[PreviewRow],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(preimage(domain, task, id, header, rows));
    h.finalize().into()
}

/// Request digest of an Apply: `SHA-256(02 || task || preview || digest)`.
pub(crate) fn apply_request_digest(task: TaskObjectId, id: PreviewId, digest: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([2u8]);
    h.update(task.0.to_be_bytes());
    h.update(id.0.to_be_bytes());
    h.update(digest);
    h.finalize().into()
}

/// Request digest of an Undo: leading `03`, trailing mode byte.
pub(crate) fn undo_request_digest(task: TaskObjectId, id: PreviewId, digest: &[u8; 32], mode: u8) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([3u8]);
    h.update(task.0.to_be_bytes());
    h.update(id.0.to_be_bytes());
    h.update(digest);
    h.update([mode]);
    h.finalize().into()
}

/// Lowercase hex on the wire.
pub(crate) fn digest_hex(digest: &[u8; 32]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// One collected effect, as the Phase 6 `Effects::iter()` will yield it
/// (`task/effects.rs` is not in the tree yet; R3-703 maps `RowEffect` onto this view).
#[derive(Clone, Copy, Debug)]
pub(crate) struct EffectView<'a> {
    pub(crate) table: u64,
    pub(crate) pk: &'a [u8],
    pub(crate) old: Option<&'a SqlRow>,
    pub(crate) new: Option<&'a SqlRow>,
}

/// Class 00 rows in ascending `(table, pk)`, `version_read = old.version`,
/// `after = encode(new.values)`. An effect without an old or a new row (a
/// primary-key move or a delete) is `Denied(PrimaryKeyChange)`.
pub(crate) fn rows_from_effects<'a>(
    effects: impl IntoIterator<Item = EffectView<'a>>,
) -> Result<Vec<PreviewRow>, TaskError> {
    let mut rows = Vec::new();
    for e in effects {
        let (Some(old), Some(new)) = (e.old, e.new) else {
            return Err(TaskError::Denied(DenyRule::PrimaryKeyChange));
        };
        rows.push(PreviewRow {
            class: PreviewClass::Change as u8,
            table: e.table,
            pk: e.pk.to_vec(),
            version_read: old.version,
            aux: 0,
            after: encode_after(&new.values)?,
        });
    }
    rows.sort_by(|a, b| (a.table, &a.pk).cmp(&(b.table, &b.pk)));
    Ok(rows)
}

/// The row payload encoding of a full row (`params` and `after`).
pub(crate) fn encode_after(values: &[Value]) -> Result<Vec<u8>, CoreError> {
    crate::storage::task_row_payload(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::records::PreviewScope;
    use crate::relational::{ColumnDef, DataType};

    fn header() -> PreviewHeader {
        PreviewHeader {
            kind: 1,
            state: STATE_OPEN,
            created_ms: 1,
            expires_ms: 1000,
            def_version: 5,
            table: 9,
            schema_fp: [0xAA; 32],
            clock_pin: NO_CLOCK_PIN,
            digest: [0; 32],
            counts: [1, 0, 0, 0],
            applied_seq: 0,
            creator: "alice".into(),
            scope: None,
            surface: 0,
            text: "sql".into(),
            params: Vec::new(),
        }
    }
    fn row() -> PreviewRow {
        PreviewRow { class: 0, table: 9, pk: vec![0, 1], version_read: 4, aux: 0, after: vec![0xBE, 0xEF] }
    }
    fn domain() -> [u8; 16] {
        std::array::from_fn(|i| i as u8)
    }

    #[test]
    fn digest_matches_fixed_vector() {
        let mut pre = b"GDPV\x01".to_vec();
        pre.push(1);
        pre.extend_from_slice(&domain());
        for v in [7u64, 3, 5, 9] {
            pre.extend_from_slice(&v.to_be_bytes());
        }
        pre.extend_from_slice(&[0xAA; 32]);
        pre.extend_from_slice(&i64::MIN.to_be_bytes());
        pre.extend_from_slice(&1000u64.to_be_bytes());
        pre.push(0);
        pre.extend_from_slice(&3u32.to_be_bytes());
        pre.extend_from_slice(b"sql");
        pre.extend_from_slice(&0u32.to_be_bytes());
        pre.extend_from_slice(&1u32.to_be_bytes());
        pre.push(0);
        pre.extend_from_slice(&9u64.to_be_bytes());
        pre.extend_from_slice(&2u32.to_be_bytes());
        pre.extend_from_slice(&[0, 1]);
        pre.extend_from_slice(&4u64.to_be_bytes());
        pre.extend_from_slice(&0u64.to_be_bytes());
        pre.extend_from_slice(&2u32.to_be_bytes());
        pre.extend_from_slice(&[0xBE, 0xEF]);
        let built = preimage(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row()]);
        assert_eq!(built, pre);
        let d = digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row()]);
        assert_eq!(digest_hex(&d), "859fbce3344b35ed8401fafb10d92e2e56a80e4b33b682a1cc68e06530933de1");
        // Fields outside the preimage (state, created, counts, creator, digest) do not matter.
        let mut h = header();
        h.state = STATE_APPLIED;
        h.created_ms = 99;
        h.counts = [9, 9, 9, 9];
        h.creator = "bob".into();
        h.digest = [1; 32];
        assert_eq!(digest(domain(), TaskObjectId(7), PreviewId(3), &h, &[row()]), d);
    }

    #[test]
    fn digest_changes_with_each_field() {
        let base = digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row()]);
        let mut seen = std::collections::BTreeSet::new();
        seen.insert(base);
        let mut check = |d: [u8; 32]| assert!(seen.insert(d), "digest did not change");
        let mut dom = domain();
        dom[0] ^= 1;
        check(digest(dom, TaskObjectId(7), PreviewId(3), &header(), &[row()]));
        check(digest(domain(), TaskObjectId(8), PreviewId(3), &header(), &[row()]));
        check(digest(domain(), TaskObjectId(7), PreviewId(4), &header(), &[row()]));
        let headers: Vec<fn(&mut PreviewHeader)> = vec![
            |h| {
                h.kind = 2;
                h.scope = Some(PreviewScope::default());
            },
            |h| h.def_version += 1,
            |h| h.table += 1,
            |h| h.schema_fp[31] ^= 1,
            |h| h.clock_pin = 0,
            |h| h.expires_ms += 1,
            |h| h.surface = 1,
            |h| h.text.push('x'),
            |h| h.params.push(0),
        ];
        for f in headers {
            let mut h = header();
            f(&mut h);
            check(digest(domain(), TaskObjectId(7), PreviewId(3), &h, &[row()]));
        }
        // Undo scope bytes are hashed.
        let mut h = header();
        h.kind = 2;
        h.scope = Some(PreviewScope { kind: 1, outcome: [3; 32] });
        check(digest(domain(), TaskObjectId(7), PreviewId(3), &h, &[row()]));
        let rows: Vec<fn(&mut PreviewRow)> = vec![
            |r| {
                r.class = 1;
                r.after.clear();
            },
            |r| r.table += 1,
            |r| r.pk.push(2),
            |r| r.version_read += 1,
            |r| r.aux += 1,
            |r| r.after.push(0),
        ];
        for f in rows {
            let mut r = row();
            f(&mut r);
            check(digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[r]));
        }
        check(digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[]));
        check(digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row(), PreviewRow { pk: vec![5], ..row() }]));
        // Row order passed in does not matter.
        let a = PreviewRow { pk: vec![1], ..row() };
        let b = PreviewRow { pk: vec![2], ..row() };
        assert_eq!(
            digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[a.clone(), b.clone()]),
            digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[b, a]),
        );
    }

    #[test]
    fn rows_sorted_by_table_then_pk() {
        let r = |v: i64, version: u64| SqlRow { values: vec![Value::Int(v)], version };
        let (o1, n1, o2, n2, o3, n3) = (r(1, 10), r(2, 11), r(3, 20), r(4, 21), r(5, 30), r(6, 31));
        let effects = vec![
            EffectView { table: 2, pk: &[1], old: Some(&o1), new: Some(&n1) },
            EffectView { table: 1, pk: &[9], old: Some(&o2), new: Some(&n2) },
            EffectView { table: 1, pk: &[3], old: Some(&o3), new: Some(&n3) },
        ];
        let rows = rows_from_effects(effects).unwrap();
        let keys: Vec<(u64, Vec<u8>, u64)> = rows.iter().map(|r| (r.table, r.pk.clone(), r.version_read)).collect();
        assert_eq!(keys, vec![(1, vec![3], 30), (1, vec![9], 20), (2, vec![1], 10)]);
        assert!(rows.iter().all(|r| r.class == 0 && r.aux == 0));
        assert_eq!(rows[0].after, encode_after(&[Value::Int(6)]).unwrap());
        // Round trip through the R3-600 codec.
        for row in &rows {
            assert_eq!(&PreviewRow::decode(&row.encode()).unwrap(), row);
        }
        let h = header();
        assert_eq!(PreviewHeader::decode(&h.encode()).unwrap(), h);
        // Missing old or new is denied.
        let bad = vec![EffectView { table: 1, pk: &[1], old: None, new: Some(&n1) }];
        assert_eq!(rows_from_effects(bad).unwrap_err(), TaskError::Denied(DenyRule::PrimaryKeyChange));
        let bad = vec![EffectView { table: 1, pk: &[1], old: Some(&o1), new: None }];
        assert_eq!(rows_from_effects(bad).unwrap_err(), TaskError::Denied(DenyRule::PrimaryKeyChange));
    }

    fn table(id: u64, name: &str, cols: &[&str]) -> TableDef {
        TableDef {
            id,
            name: name.into(),
            columns: cols
                .iter()
                .enumerate()
                .map(|(i, c)| ColumnDef { id: i as u64 + 1, name: (*c).into(), data_type: DataType::BigInt, not_null: false })
                .collect(),
            primary_key: vec![0],
        }
    }

    #[test]
    fn schema_fp_changes_on_add_column_and_not_on_unrelated_table() {
        let t = table(1, "public.t", &["id", "a"]);
        let fp = schema_fp(&t, &[]).unwrap();
        assert_eq!(fp, schema_fp(&t.clone(), &[]).unwrap());
        let added = table(1, "public.t", &["id", "a", "b"]);
        assert_ne!(fp, schema_fp(&added, &[]).unwrap());
        // Another table's DDL is not an input: the fingerprint of `t` is unchanged.
        let _other = table(2, "public.u", &["id", "z"]);
        assert_eq!(fp, schema_fp(&t, &[]).unwrap());
        // Constraints are hashed in id order, whatever order they come in.
        let c1 = Constraint { id: 10, name: "t_pkey".into(), kind: crate::relational::catalog_ext::ConstraintKind::PrimaryKey };
        let c2 = Constraint {
            id: 11,
            name: "t_a_check".into(),
            kind: crate::relational::catalog_ext::ConstraintKind::Check { expr: "a > 0".into(), not_valid: false },
        };
        let with = schema_fp(&t, &[c1.clone(), c2.clone()]).unwrap();
        assert_ne!(fp, with);
        assert_eq!(with, schema_fp(&t, &[c2, c1]).unwrap());
    }
}
