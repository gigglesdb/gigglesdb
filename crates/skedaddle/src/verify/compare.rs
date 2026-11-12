use std::{cmp::Ordering, collections::BTreeSet};

use async_trait::async_trait;

use crate::mapping::Value;

use super::{
    ContentDigest, Coverage, Key, Projection, RangePlan, Result, Row, VerifyError,
    hash::{empty_digest, key_bytes, parent},
    range_digest, row_hash,
};

/// Actual unique-key INDEX ordering, including collation and ASC/DESC options.
/// Equality must mean the same typed canonical key; unsupported cross-endpoint
/// equivalences must be rejected by preflight rather than guessed here.
pub trait KeyOrder {
    fn compare_keys(&self, left: &Key, right: &Key) -> Result<Ordering>;
}

#[async_trait]
pub trait RowSource: KeyOrder + Send {
    /// One independent read, ordered by the agreed key. Bounds are [lo, hi).
    /// Select only key and projection columns. Never return excluded columns.
    async fn fetch(&mut self, range: &RangePlan, projection: &Projection) -> Result<Vec<Row>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDifference {
    pub column: String,
    pub source: Value,
    pub destination: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Difference {
    Missing {
        key: Key,
    },
    Extra {
        key: Key,
    },
    Changed {
        key: Key,
        columns: Vec<ColumnDifference>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Comparison {
    pub source_count: u64,
    pub destination_count: u64,
    pub source_digest: ContentDigest,
    pub destination_digest: ContentDigest,
    pub coverage: Coverage,
    pub differences: Vec<Difference>,
    pub busy: Vec<Key>,
    /// Number of Merkle node pairs examined during bisection (not build work).
    pub compared_nodes: u64,
}

pub(super) fn compare_key(
    order: &(impl KeyOrder + ?Sized),
    left: &Key,
    right: &Key,
) -> Result<Ordering> {
    let comparison = order.compare_keys(left, right)?;
    if comparison == Ordering::Equal && key_bytes(left)? != key_bytes(right)? {
        return Err(VerifyError::Invalid(
            "key order equates distinct canonical keys",
        ));
    }
    Ok(comparison)
}

pub(super) fn validate_range(order: &(impl KeyOrder + ?Sized), range: &RangePlan) -> Result<()> {
    if range.id.table.is_empty() {
        return Err(VerifyError::Invalid("missing table identity"));
    }
    for key in [&range.lo, &range.hi].into_iter().flatten() {
        key_bytes(key)?;
    }
    if let (Some(lo), Some(hi)) = (&range.lo, &range.hi)
        && compare_key(order, lo, hi)? != Ordering::Less
    {
        return Err(VerifyError::Invalid("empty or inverted key range"));
    }
    Ok(())
}

pub(super) fn validate_key(
    order: &(impl KeyOrder + ?Sized),
    range: &RangePlan,
    key: &Key,
) -> Result<()> {
    key_bytes(key)?;
    if let Some(lo) = &range.lo
        && compare_key(order, key, lo)? == Ordering::Less
    {
        return Err(VerifyError::Invalid("key below range"));
    }
    if let Some(hi) = &range.hi
        && compare_key(order, key, hi)? != Ordering::Less
    {
        return Err(VerifyError::Invalid("key at or above range end"));
    }
    Ok(())
}

fn validate_rows(
    order: &(impl KeyOrder + ?Sized),
    range: &RangePlan,
    rows: &[Row],
    projection: &Projection,
) -> Result<()> {
    for (index, row) in rows.iter().enumerate() {
        validate_key(order, range, &row.key)?;
        if index > 0 && compare_key(order, &rows[index - 1].key, &row.key)? != Ordering::Less {
            return Err(VerifyError::Invalid(
                "rows not strictly ordered by unique key",
            ));
        }
        if row.columns.len() != projection.columns().len()
            || projection
                .columns()
                .iter()
                .any(|name| !row.columns.contains_key(name))
        {
            return Err(VerifyError::Invalid("row columns differ from projection"));
        }
    }
    Ok(())
}

struct Entry<'a> {
    key: &'a Key,
    source: Option<&'a Row>,
    destination: Option<&'a Row>,
}

fn align<'a>(
    order: &(impl KeyOrder + ?Sized),
    source: &'a [Row],
    destination: &'a [Row],
) -> Result<Vec<Entry<'a>>> {
    let (mut left, mut right) = (0, 0);
    let mut entries = Vec::with_capacity(source.len().max(destination.len()));
    while left < source.len() || right < destination.len() {
        let comparison = match (source.get(left), destination.get(right)) {
            (Some(a), Some(b)) => compare_key(order, &a.key, &b.key)?,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => unreachable!(),
        };
        match comparison {
            Ordering::Less => {
                entries.push(Entry {
                    key: &source[left].key,
                    source: Some(&source[left]),
                    destination: None,
                });
                left += 1;
            }
            Ordering::Greater => {
                entries.push(Entry {
                    key: &destination[right].key,
                    source: None,
                    destination: Some(&destination[right]),
                });
                right += 1;
            }
            Ordering::Equal => {
                entries.push(Entry {
                    key: &source[left].key,
                    source: Some(&source[left]),
                    destination: Some(&destination[right]),
                });
                left += 1;
                right += 1;
            }
        }
    }
    Ok(entries)
}

/// Merge historical keys without discarding rows which vanished on retry.
fn include_known<'a>(
    order: &(impl KeyOrder + ?Sized),
    entries: Vec<Entry<'a>>,
    known: &'a [Key],
) -> Result<Vec<Entry<'a>>> {
    let mut result = Vec::with_capacity(entries.len() + known.len());
    let mut known = known.iter().peekable();
    for entry in entries {
        while let Some(key) = known.peek() {
            match compare_key(order, key, entry.key)? {
                Ordering::Less => {
                    result.push(Entry {
                        key: known.next().expect("peeked key"),
                        source: None,
                        destination: None,
                    });
                }
                Ordering::Equal => {
                    known.next();
                    break;
                }
                Ordering::Greater => break,
            }
        }
        result.push(entry);
    }
    result.extend(known.map(|key| Entry {
        key,
        source: None,
        destination: None,
    }));
    Ok(result)
}

struct Node {
    source: ContentDigest,
    destination: ContentDigest,
    start: usize,
    children: Option<(Box<Node>, Box<Node>)>,
}

impl Node {
    fn build(entries: &[Entry<'_>], start: usize, projection: &Projection) -> Result<Self> {
        if entries.len() == 1 {
            return Ok(Self {
                source: entries[0]
                    .source
                    .map(|row| row_hash(row, projection))
                    .transpose()?
                    .unwrap_or_else(empty_digest),
                destination: entries[0]
                    .destination
                    .map(|row| row_hash(row, projection))
                    .transpose()?
                    .unwrap_or_else(empty_digest),
                start,
                children: None,
            });
        }
        let middle = entries.len() / 2;
        let left = Box::new(Self::build(&entries[..middle], start, projection)?);
        let right = Box::new(Self::build(&entries[middle..], start + middle, projection)?);
        Ok(Self {
            source: parent(left.source, right.source),
            destination: parent(left.destination, right.destination),
            start,
            children: Some((left, right)),
        })
    }

    fn drill(
        &self,
        entries: &[Entry<'_>],
        projection: &Projection,
        busy: &BTreeSet<Vec<u8>>,
        result: &mut Comparison,
    ) -> Result<()> {
        result.compared_nodes += 1;
        if self.source == self.destination {
            return Ok(());
        }
        if let Some((left, right)) = &self.children {
            left.drill(entries, projection, busy, result)?;
            right.drill(entries, projection, busy, result)?;
            return Ok(());
        }
        let entry = &entries[self.start];
        if busy.contains(&key_bytes(entry.key)?) {
            return Ok(());
        }
        let key = entry.key.clone();
        result
            .differences
            .push(match (entry.source, entry.destination) {
                (Some(_), None) => Difference::Missing { key },
                (None, Some(_)) => Difference::Extra { key },
                (Some(source), Some(destination)) => Difference::Changed {
                    key,
                    columns: projection
                        .columns()
                        .iter()
                        .filter(|name| source.columns.get(*name) != destination.columns.get(*name))
                        .map(|name| ColumnDifference {
                            column: name.clone(),
                            source: source.columns[name].clone(),
                            destination: destination.columns[name].clone(),
                        })
                        .collect(),
                },
                (None, None) => unreachable!("two empty leaves have identical digests"),
            });
        Ok(())
    }
}

/// Compare a stable, already comparable pair of independent reads. This helper
/// does not establish a live boundary; use VerificationSession for live data.
pub fn compare_rows(
    order: &(impl KeyOrder + ?Sized),
    range: &RangePlan,
    projection: &Projection,
    source: &[Row],
    destination: &[Row],
) -> Result<Comparison> {
    compare_with_changes(order, range, projection, source, destination, &[], &[])
}

pub(super) fn compare_with_changes(
    order: &(impl KeyOrder + ?Sized),
    range: &RangePlan,
    projection: &Projection,
    source: &[Row],
    destination: &[Row],
    busy: &[Key],
    known: &[Key],
) -> Result<Comparison> {
    validate_range(order, range)?;
    validate_rows(order, range, source, projection)?;
    validate_rows(order, range, destination, projection)?;
    for keys in [busy, known] {
        for (i, key) in keys.iter().enumerate() {
            validate_key(order, range, key)?;
            if i > 0 && compare_key(order, &keys[i - 1], key)? != Ordering::Less {
                return Err(VerifyError::Invalid("change keys not strictly ordered"));
            }
        }
    }
    let entries = include_known(
        order,
        include_known(order, align(order, source, destination)?, known)?,
        busy,
    )?;
    let mut result = Comparison {
        source_count: source.len() as u64,
        destination_count: destination.len() as u64,
        source_digest: range_digest(source, projection)?,
        destination_digest: range_digest(destination, projection)?,
        coverage: Coverage {
            total: entries.len() as u64,
            busy: busy.len() as u64,
            ..Coverage::default()
        },
        differences: Vec::new(),
        busy: busy.to_vec(),
        compared_nodes: 0,
    };
    if !entries.is_empty() {
        let tree = Node::build(&entries, 0, projection)?;
        let busy = busy
            .iter()
            .map(key_bytes)
            .collect::<Result<BTreeSet<_>>>()?;
        tree.drill(&entries, projection, &busy, &mut result)?;
    }
    result.coverage.mismatched = result.differences.len() as u64;
    result.coverage.verified =
        result.coverage.total - result.coverage.busy - result.coverage.mismatched;
    Ok(result)
}

/// Two fetches regardless of row count or number of mismatches. The cached
/// bottom-up trees localize mismatches by bisecting only unequal subtrees.
/// Requires stable data or a boundary established by the caller. Live
/// verification must use VerificationSession to classify concurrent changes.
pub async fn compare_sources(
    source: &mut (impl RowSource + ?Sized),
    destination: &mut (impl RowSource + ?Sized),
    range: &RangePlan,
    projection: &Projection,
) -> Result<Comparison> {
    validate_range(source, range)?;
    let source_rows = source.fetch(range, projection).await?;
    let destination_rows = destination.fetch(range, projection).await?;
    compare_rows(source, range, projection, &source_rows, &destination_rows)
}
