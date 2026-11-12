use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use skedaddle::{
    mapping::{EncodedBytes, Numeric, Value},
    pg::chunk::ChunkId,
    verify::{
        Applier, BoundarySource, BoundaryState, ChangeEvidence, Coverage, Difference, Key,
        KeyOrder, Lsn, Projection, RangePlan, Result, ResultSlot, Row, RowSource, SourceRead,
        TableCoverage, VerificationReport, VerificationSession, VerifyError, compare_rows,
        compare_sources, range_digest, row_hash,
    },
};

fn key(value: i64) -> Key {
    vec![Value::Int8(value)]
}

fn row(id: i64, value: i64) -> Row {
    Row {
        key: key(id),
        columns: BTreeMap::from([
            ("amount".into(), Value::Int8(value)),
            ("payload".into(), Value::Bytea(vec![1, 2, 3])),
        ]),
    }
}

fn projection() -> Projection {
    Projection::new(vec!["amount".into(), "payload".into()]).unwrap()
}

fn range() -> RangePlan {
    RangePlan {
        id: ChunkId {
            table: "public.items".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        lo: None,
        hi: None,
    }
}

type Trace = Arc<Mutex<Vec<&'static str>>>;

#[derive(Default)]
struct Memory {
    rows: Vec<Row>,
    snapshots: VecDeque<SourceRead>,
    changes: VecDeque<ChangeEvidence>,
    evidence_unavailable: bool,
    fetches: usize,
    trace: Trace,
    descending: bool,
}

impl Memory {
    fn new(rows: Vec<Row>) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    fn projected(
        &self,
        rows: &[Row],
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<Vec<Row>> {
        let mut result = Vec::new();
        for row in rows {
            if range
                .lo
                .as_ref()
                .map(|lo| self.compare_keys(&row.key, lo))
                .transpose()?
                .is_some_and(|cmp| cmp == Ordering::Less)
                || range
                    .hi
                    .as_ref()
                    .map(|hi| self.compare_keys(&row.key, hi))
                    .transpose()?
                    .is_some_and(|cmp| cmp != Ordering::Less)
            {
                continue;
            }
            result.push(Row {
                key: row.key.clone(),
                columns: projection
                    .columns()
                    .iter()
                    .map(|name| {
                        Ok((
                            name.clone(),
                            row.columns
                                .get(name)
                                .ok_or(VerifyError::Invalid("missing test column"))?
                                .clone(),
                        ))
                    })
                    .collect::<Result<_>>()?,
            });
        }
        Ok(result)
    }
}

impl KeyOrder for Memory {
    fn compare_keys(&self, left: &Key, right: &Key) -> Result<Ordering> {
        if left.len() != right.len() || left.is_empty() {
            return Err(VerifyError::Invalid("test key arity"));
        }
        for (a, b) in left.iter().zip(right) {
            let (Value::Int8(a), Value::Int8(b)) = (a, b) else {
                return Err(VerifyError::Invalid("test key type"));
            };
            let comparison = if self.descending { b.cmp(a) } else { a.cmp(b) };
            if comparison != Ordering::Equal {
                return Ok(comparison);
            }
        }
        Ok(Ordering::Equal)
    }
}

#[async_trait]
impl RowSource for Memory {
    async fn fetch(&mut self, range: &RangePlan, projection: &Projection) -> Result<Vec<Row>> {
        self.trace.lock().unwrap().push("destination");
        self.fetches += 1;
        self.projected(&self.rows, range, projection)
    }
}

#[async_trait]
impl BoundarySource for Memory {
    async fn read_source(
        &mut self,
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<SourceRead> {
        self.trace.lock().unwrap().push("source");
        self.fetches += 1;
        let read = self.snapshots.pop_front().unwrap_or(SourceRead {
            rows: self.rows.clone(),
            lsn: 10,
        });
        Ok(SourceRead {
            rows: self.projected(&read.rows, range, projection)?,
            lsn: read.lsn,
        })
    }

    async fn changed_since(&mut self, _range: &RangePlan, lsn: Lsn) -> Result<ChangeEvidence> {
        self.trace.lock().unwrap().push("changes");
        if self.evidence_unavailable {
            return Err(VerifyError::Read("change evidence unavailable".into()));
        }
        Ok(self.changes.pop_front().unwrap_or(ChangeEvidence {
            through_lsn: lsn,
            keys: vec![],
        }))
    }
}

#[derive(Default)]
struct Applied {
    trace: Trace,
    returned_lsn: Option<Lsn>,
}

#[async_trait]
impl Applier for Applied {
    async fn wait_until(&mut self, lsn: Lsn) -> Result<Lsn> {
        self.trace.lock().unwrap().push("wait");
        Ok(self.returned_lsn.unwrap_or(lsn))
    }
}

async fn compare(source: Vec<Row>, destination: Vec<Row>) -> skedaddle::verify::Comparison {
    compare_sources(
        &mut Memory::new(source),
        &mut Memory::new(destination),
        &range(),
        &projection(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn detects_missing_row() {
    let result = compare(
        vec![row(1, 10), row(2, 20), row(3, 30)],
        vec![row(1, 10), row(3, 30)],
    )
    .await;
    assert_eq!(
        result.differences,
        vec![Difference::Missing { key: key(2) }]
    );
    assert_eq!(
        result.coverage,
        Coverage {
            total: 3,
            verified: 2,
            mismatched: 1,
            ..Coverage::default()
        }
    );
}

#[tokio::test]
async fn detects_extra_row() {
    let result = compare(
        vec![row(1, 10), row(3, 30)],
        vec![row(1, 10), row(2, 20), row(3, 30)],
    )
    .await;
    assert_eq!(result.differences, vec![Difference::Extra { key: key(2) }]);
    assert_eq!(result.coverage.total, 3);
}

#[tokio::test]
async fn detects_single_column_corruption() {
    let result = compare(vec![row(1, 10)], vec![row(1, 11)]).await;
    let Difference::Changed {
        key: actual,
        columns,
    } = &result.differences[0]
    else {
        panic!("expected changed row");
    };
    assert_eq!(*actual, key(1));
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].column, "amount");
    assert_eq!(columns[0].source, Value::Int8(10));
    assert_eq!(columns[0].destination, Value::Int8(11));
}

#[tokio::test]
async fn counts_equal_but_content_differs_detected() {
    let result = compare(vec![row(1, 10), row(2, 20)], vec![row(1, 999), row(3, 20)]).await;
    assert_eq!(result.source_count, result.destination_count);
    assert_ne!(result.source_digest, result.destination_digest);
    assert_eq!(result.differences.len(), 3);
    assert_eq!(result.coverage.verified, 0);
    assert!(!result.coverage.is_complete());
}

#[tokio::test]
async fn bisection_fetch_count_bounded() {
    let rows: Vec<_> = (0..100_000).map(|i| row(i, i)).collect();
    let mut source = Memory::new(rows.clone());
    let mut destination = Memory::new(rows);
    destination.rows[73_123]
        .columns
        .insert("amount".into(), Value::Int8(-1));
    let result = compare_sources(&mut source, &mut destination, &range(), &projection())
        .await
        .unwrap();
    assert_eq!(source.fetches + destination.fetches, 2);
    assert!(
        result.compared_nodes <= 2 * 17 + 1,
        "{} node pairs",
        result.compared_nodes
    );
    assert_eq!(result.coverage.verified, 99_999);
    let Difference::Changed { key: actual, .. } = &result.differences[0] else {
        panic!("changed row");
    };
    assert_eq!(*actual, key(73_123));
}

#[tokio::test]
async fn equal_ranges_prune_at_root() {
    let rows: Vec<_> = (0..101).map(|i| row(i, i)).collect();
    let result = compare(rows.clone(), rows).await;
    assert!(result.coverage.is_complete());
    assert_eq!(result.compared_nodes, 1);
    assert_eq!(result.source_digest, result.destination_digest);
}

#[tokio::test]
async fn all_missing_extra_and_empty_ranges() {
    let missing = compare(vec![row(1, 10), row(2, 20)], vec![]).await;
    assert!(
        missing
            .differences
            .iter()
            .all(|d| matches!(d, Difference::Missing { .. }))
    );
    let extra = compare(vec![], vec![row(1, 10), row(2, 20)]).await;
    assert!(
        extra
            .differences
            .iter()
            .all(|d| matches!(d, Difference::Extra { .. }))
    );
    let empty = compare(vec![], vec![]).await;
    assert!(empty.coverage.is_complete());
    assert_eq!(empty.compared_nodes, 0);
    assert_eq!(empty.source_digest, empty.destination_digest);
}

#[test]
fn typed_hash_distinguishes_null_empty_and_types() {
    let mut input = row(1, 10);
    let mut hashes = std::collections::BTreeSet::new();
    for value in [
        Value::Null,
        Value::Bytea(vec![]),
        Value::Int8(1),
        Value::Int4(1),
        Value::Text(EncodedBytes {
            bytes: vec![],
            encoding: "UTF8".into(),
        }),
        Value::Numeric(Numeric::parse("1.0").unwrap()),
        Value::Numeric(Numeric::parse("1.00").unwrap()),
    ] {
        input.columns.insert("amount".into(), value);
        assert!(hashes.insert(row_hash(&input, &projection()).unwrap()));
    }
}

#[test]
fn canonical_row_hash_v1_vector() {
    // Independently framed from P4's SKVA v1 canonical bytes using Python hashlib.
    let actual = row_hash(&row(1, 10), &projection()).unwrap();
    let hex = actual
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        hex,
        "2773051a5cd6bfecd9ef8be403ee8f13a65a38b417e8a5e95d3e3b8a51f705e4"
    );
}

#[tokio::test]
async fn missing_change_evidence_cannot_verify_equal_rows() {
    let mut source = Memory {
        rows: vec![row(1, 10)],
        evidence_unavailable: true,
        ..Memory::default()
    };
    let mut session = VerificationSession::new(range(), projection(), 0).unwrap();
    assert!(
        session
            .run(
                &mut source,
                &mut Memory::new(vec![row(1, 10)]),
                &mut Applied::default()
            )
            .await
            .is_err()
    );
    assert_eq!(
        session.state(),
        &BoundaryState::CheckChanges { source_lsn: 10 }
    );
    assert!(session.result().is_none());
}

#[tokio::test]
async fn table_coverage_aggregates_complete_plan_without_double_counting() {
    let mut lower = range();
    lower.hi = Some(key(2));
    let mut upper = range();
    upper.lo = Some(key(2));
    upper.id.ordinal = 1;
    let lower_result = VerificationSession::new(lower, projection(), 0)
        .unwrap()
        .run(
            &mut Memory::new(vec![row(1, 10)]),
            &mut Memory::new(vec![row(1, 10)]),
            &mut Applied::default(),
        )
        .await
        .unwrap()
        .clone();
    let mut source = Memory::new(vec![row(2, 20), row(3, 30)]);
    source.changes.push_back(ChangeEvidence {
        through_lsn: 10,
        keys: vec![key(3)],
    });
    let upper_result = VerificationSession::new(upper, projection(), 0)
        .unwrap()
        .run(
            &mut source,
            &mut Memory::new(vec![row(2, 999), row(3, 30)]),
            &mut Applied::default(),
        )
        .await
        .unwrap()
        .clone();
    let results = vec![lower_result.clone(), upper_result.clone()];
    let coverage = TableCoverage::from_ranges(&Memory::default(), &results).unwrap();
    assert_eq!(
        coverage.rows,
        Coverage {
            total: 3,
            verified: 1,
            mismatched: 1,
            busy: 1,
            excluded: 0
        }
    );
    assert!(TableCoverage::from_ranges(&Memory::default(), &[]).is_err());
    assert!(
        TableCoverage::from_ranges(&Memory::default(), std::slice::from_ref(&lower_result))
            .is_err()
    );
    assert!(
        TableCoverage::from_ranges(
            &Memory::default(),
            &[
                lower_result.clone(),
                lower_result.clone(),
                upper_result.clone()
            ]
        )
        .is_err()
    );
    let mut gap = upper_result.clone();
    gap.range.lo = Some(key(3));
    assert!(TableCoverage::from_ranges(&Memory::default(), &[lower_result.clone(), gap]).is_err());
    let mut other_plan = upper_result;
    other_plan.range.id.plan = "b".repeat(64);
    assert!(TableCoverage::from_ranges(&Memory::default(), &[lower_result, other_plan]).is_err());
}

#[test]
fn hash_includes_key_column_identity_and_order() {
    let original = row(1, 10);
    let original_hash = row_hash(&original, &projection()).unwrap();
    assert_ne!(original_hash, row_hash(&row(2, 10), &projection()).unwrap());
    let reversed = Projection::new(vec!["payload".into(), "amount".into()]).unwrap();
    assert_ne!(original_hash, row_hash(&original, &reversed).unwrap());
    let mut renamed = original.clone();
    let amount = renamed.columns.remove("amount").unwrap();
    renamed.columns.insert("renamed".into(), amount);
    let renamed_projection = Projection::new(vec!["renamed".into(), "payload".into()]).unwrap();
    assert_ne!(
        original_hash,
        row_hash(&renamed, &renamed_projection).unwrap()
    );
    let a = row(1, 1);
    let b = row(2, 2);
    assert_ne!(
        range_digest(&[a.clone(), b.clone()], &projection()).unwrap(),
        range_digest(&[b, a], &projection()).unwrap()
    );
    assert_ne!(
        range_digest(&[], &projection()).unwrap(),
        range_digest(&[original], &projection()).unwrap()
    );
}

#[tokio::test]
async fn excluded_columns_are_never_hashed_or_requested() {
    let mut a = row(1, 10);
    a.columns.insert("secret".into(), Value::Int8(1));
    let mut b = a.clone();
    b.columns.insert("secret".into(), Value::Null);
    assert_eq!(
        row_hash(&a, &projection()).unwrap(),
        row_hash(&b, &projection()).unwrap()
    );
    assert!(
        compare(vec![a.clone()], vec![b.clone()])
            .await
            .coverage
            .is_complete()
    );
    // Unprojected adapters fail closed instead of silently exposing excluded data.
    assert!(compare_rows(&Memory::default(), &range(), &projection(), &[a], &[b]).is_err());
}

#[test]
fn excluded_tables_visible_in_coverage() {
    let report = VerificationReport {
        tables: vec![TableCoverage::excluded(
            "public.private",
            42,
            "operator exclusion",
        )],
        ..VerificationReport::default()
    };
    assert_eq!(report.tables[0].rows.total, 42);
    assert_eq!(report.tables[0].rows.excluded, 42);
    assert!(!report.tables[0].rows.is_complete());
    assert_eq!(
        report.tables[0].exclusion_reason.as_deref(),
        Some("operator exclusion")
    );
    assert!(matches!(report.schema, ResultSlot::NotCollected));
    assert!(matches!(report.sequences, ResultSlot::NotCollected));
    assert!(matches!(report.constraints, ResultSlot::NotCollected));
    assert!(matches!(report.indexes, ResultSlot::NotCollected));
    assert!(matches!(report.foreign_keys, ResultSlot::NotCollected));
}

#[tokio::test]
async fn comparable_boundary_state_machine_orders_reads() {
    let trace = Trace::default();
    let mut source = Memory {
        rows: vec![row(1, 10)],
        trace: trace.clone(),
        ..Memory::default()
    };
    let mut destination = Memory {
        rows: source.rows.clone(),
        trace: trace.clone(),
        ..Memory::default()
    };
    let mut applied = Applied {
        trace: trace.clone(),
        ..Applied::default()
    };
    let mut session = VerificationSession::new(range(), projection(), 0).unwrap();
    assert_eq!(session.state(), &BoundaryState::ReadSource);
    assert!(session.result().is_none());
    assert_eq!(
        session
            .advance(&mut source, &mut destination, &mut applied)
            .await
            .unwrap(),
        &BoundaryState::WaitForApplier { source_lsn: 10 }
    );
    assert_eq!(destination.fetches, 0);
    assert_eq!(
        session
            .advance(&mut source, &mut destination, &mut applied)
            .await
            .unwrap(),
        &BoundaryState::ReadDestination { source_lsn: 10 }
    );
    assert_eq!(destination.fetches, 0);
    assert_eq!(
        session
            .advance(&mut source, &mut destination, &mut applied)
            .await
            .unwrap(),
        &BoundaryState::CheckChanges { source_lsn: 10 }
    );
    let result = session
        .run(&mut source, &mut destination, &mut applied)
        .await
        .unwrap();
    assert!(result.comparison.coverage.is_complete());
    assert_eq!(result.table_coverage().table, "public.items");
    assert_eq!(
        *trace.lock().unwrap(),
        ["source", "wait", "destination", "changes"]
    );
    session
        .advance(&mut source, &mut destination, &mut applied)
        .await
        .unwrap();
    assert_eq!(trace.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn busy_rows_stay_in_denominator() {
    let mut source = Memory::new(vec![row(1, 10), row(2, 20)]);
    source.changes.push_back(ChangeEvidence {
        through_lsn: 11,
        keys: vec![key(2), key(3)],
    });
    let mut destination = Memory::new(vec![row(1, 10), row(2, 21)]);
    let result = VerificationSession::new(range(), projection(), 0)
        .unwrap()
        .run(&mut source, &mut destination, &mut Applied::default())
        .await
        .unwrap()
        .clone();
    assert_eq!(
        result.comparison.coverage,
        Coverage {
            total: 3,
            verified: 1,
            busy: 2,
            ..Coverage::default()
        }
    );
    assert!(result.comparison.differences.is_empty());
    assert_eq!(result.comparison.busy, vec![key(2), key(3)]);
    assert!(!result.comparison.coverage.is_complete());
}

#[tokio::test]
async fn busy_even_when_values_match_or_change_was_reverted() {
    let mut source = Memory::new(vec![row(1, 10)]);
    source.changes.push_back(ChangeEvidence {
        through_lsn: 12,
        keys: vec![key(1)],
    });
    let result = VerificationSession::new(range(), projection(), 0)
        .unwrap()
        .run(
            &mut source,
            &mut Memory::new(vec![row(1, 10)]),
            &mut Applied::default(),
        )
        .await
        .unwrap()
        .clone();
    assert_eq!(result.comparison.coverage.busy, 1);
    assert_eq!(result.comparison.coverage.verified, 0);
}

#[tokio::test]
async fn busy_requeued_with_budget_and_eventually_verified() {
    let mut source = Memory::new(vec![row(1, 10)]);
    source.snapshots = VecDeque::from([
        SourceRead {
            rows: vec![row(1, 9)],
            lsn: 10,
        },
        SourceRead {
            rows: vec![row(1, 10)],
            lsn: 12,
        },
    ]);
    source.changes.push_back(ChangeEvidence {
        through_lsn: 11,
        keys: vec![key(1)],
    });
    let mut destination = Memory::new(vec![row(1, 10)]);
    let mut applied = Applied::default();
    let mut session = VerificationSession::new(range(), projection(), 1).unwrap();
    for _ in 0..4 {
        session
            .advance(&mut source, &mut destination, &mut applied)
            .await
            .unwrap();
    }
    assert_eq!(
        session.state(),
        &BoundaryState::Requeued {
            busy: 1,
            retries_remaining: 1
        }
    );
    assert!(session.result().is_none());
    let result = session
        .run(&mut source, &mut destination, &mut applied)
        .await
        .unwrap();
    assert_eq!(result.attempts, 2);
    assert!(result.comparison.coverage.is_complete());
    assert_eq!(source.fetches + destination.fetches, 4);
}

#[tokio::test]
async fn retry_budget_exhaustion_retains_busy_rows() {
    let mut source = Memory {
        snapshots: (10..=12)
            .map(|lsn| SourceRead { rows: vec![], lsn })
            .collect(),
        changes: (10..=12)
            .map(|through_lsn| ChangeEvidence {
                through_lsn,
                keys: vec![key(5)],
            })
            .collect(),
        ..Memory::default()
    };
    let mut destination = Memory::default();
    let result = VerificationSession::new(range(), projection(), 2)
        .unwrap()
        .run(&mut source, &mut destination, &mut Applied::default())
        .await
        .unwrap()
        .clone();
    assert_eq!(result.attempts, 3);
    assert_eq!(result.comparison.coverage.total, 1);
    assert_eq!(result.comparison.coverage.busy, 1);
    assert_eq!(source.fetches + destination.fetches, 6);
}

#[tokio::test]
async fn vanished_busy_row_remains_in_denominator_after_retry() {
    let mut source = Memory {
        snapshots: VecDeque::from([
            SourceRead {
                rows: vec![row(1, 10)],
                lsn: 10,
            },
            SourceRead {
                rows: vec![],
                lsn: 12,
            },
        ]),
        ..Memory::default()
    };
    source.changes.push_back(ChangeEvidence {
        through_lsn: 11,
        keys: vec![key(1)],
    });
    let result = VerificationSession::new(range(), projection(), 1)
        .unwrap()
        .run(&mut source, &mut Memory::default(), &mut Applied::default())
        .await
        .unwrap()
        .clone();
    assert_eq!(result.comparison.coverage.total, 1);
    assert_eq!(result.comparison.coverage.verified, 1); // verified absent on both sides
    assert_eq!(result.comparison.coverage.busy, 0);
}

#[tokio::test]
async fn stable_corruption_next_to_busy_row_is_still_mismatched() {
    let mut source = Memory::new(vec![row(1, 10), row(2, 20)]);
    source.changes.push_back(ChangeEvidence {
        through_lsn: 11,
        keys: vec![key(2)],
    });
    let result = VerificationSession::new(range(), projection(), 0)
        .unwrap()
        .run(
            &mut source,
            &mut Memory::new(vec![row(1, 99)]),
            &mut Applied::default(),
        )
        .await
        .unwrap()
        .clone();
    assert_eq!(result.comparison.coverage.busy, 1);
    assert_eq!(result.comparison.coverage.mismatched, 1);
    assert_eq!(result.comparison.differences.len(), 1);
}

#[tokio::test]
async fn behind_applier_never_reads_destination() {
    let mut source = Memory::new(vec![row(1, 10)]);
    let mut destination = Memory::default();
    let mut applied = Applied {
        returned_lsn: Some(9),
        ..Applied::default()
    };
    let mut session = VerificationSession::new(range(), projection(), 0).unwrap();
    assert!(
        session
            .run(&mut source, &mut destination, &mut applied)
            .await
            .is_err()
    );
    assert_eq!(
        session.state(),
        &BoundaryState::WaitForApplier { source_lsn: 10 }
    );
    assert_eq!(destination.fetches, 0);
    assert!(session.result().is_none());
    applied.returned_lsn = Some(10);
    assert!(
        session
            .run(&mut source, &mut destination, &mut applied)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn rejects_stale_change_evidence_and_regressing_source_lsn() {
    let mut source = Memory::default();
    source.changes.push_back(ChangeEvidence {
        through_lsn: 9,
        keys: vec![],
    });
    let mut session = VerificationSession::new(range(), projection(), 0).unwrap();
    assert!(
        session
            .run(&mut source, &mut Memory::default(), &mut Applied::default())
            .await
            .is_err()
    );
    assert!(session.result().is_none());
    let mut source = Memory::default();
    source.changes.push_back(ChangeEvidence {
        through_lsn: 11,
        keys: vec![key(1)],
    });
    let mut session = VerificationSession::new(range(), projection(), 1).unwrap();
    assert!(
        session
            .run(&mut source, &mut Memory::default(), &mut Applied::default())
            .await
            .is_err()
    );
    assert_eq!(session.state(), &BoundaryState::ReadSource);
}

#[test]
fn rejects_invalid_ranges_unsorted_duplicate_and_unprojected_rows() {
    let order = Memory::default();
    let p = projection();
    assert!(compare_rows(&order, &range(), &p, &[row(2, 2), row(1, 1)], &[]).is_err());
    assert!(compare_rows(&order, &range(), &p, &[row(1, 2), row(1, 1)], &[]).is_err());
    let mut bounded = range();
    bounded.lo = Some(key(2));
    bounded.hi = Some(key(4));
    assert!(compare_rows(&order, &bounded, &p, &[row(1, 1)], &[]).is_err());
    assert!(compare_rows(&order, &bounded, &p, &[], &[row(4, 4)]).is_err());
    bounded.hi = Some(key(2));
    assert!(compare_rows(&order, &bounded, &p, &[], &[]).is_err());
    let mut null = row(1, 1);
    null.key = vec![Value::Null];
    assert!(compare_rows(&order, &range(), &p, &[null], &[]).is_err());
    assert!(Projection::new(vec!["amount".into(), "amount".into()]).is_err());
    assert!(VerificationSession::new(range(), p, u32::MAX).is_err());
}

#[tokio::test]
async fn chunk_bounds_and_descending_compound_key_order_are_shared() {
    let rows = (1..=5)
        .rev()
        .map(|i| {
            let mut r = row(i, i);
            r.key.push(Value::Int8(0));
            r
        })
        .collect::<Vec<_>>();
    let mut source = Memory {
        rows: rows.clone(),
        descending: true,
        ..Memory::default()
    };
    let mut destination = Memory {
        rows,
        descending: true,
        ..Memory::default()
    };
    let mut bounded = range();
    bounded.lo = Some(vec![Value::Int8(4), Value::Int8(0)]);
    bounded.hi = Some(vec![Value::Int8(2), Value::Int8(0)]);
    let result = compare_sources(&mut source, &mut destination, &bounded, &projection())
        .await
        .unwrap();
    assert_eq!(result.coverage.total, 2); // 4 and 3, following INDEX ordering
    assert!(result.coverage.is_complete());
}

#[tokio::test]
async fn change_keys_must_be_unique_ordered_and_in_range() {
    for keys in [vec![key(2), key(1)], vec![key(1), key(1)], vec![key(4)]] {
        let mut source = Memory::default();
        source.changes.push_back(ChangeEvidence {
            through_lsn: 11,
            keys,
        });
        let mut bounded = range();
        bounded.hi = Some(key(4));
        assert!(
            VerificationSession::new(bounded, projection(), 0)
                .unwrap()
                .run(&mut source, &mut Memory::default(), &mut Applied::default())
                .await
                .is_err()
        );
    }
}

#[test]
fn verifier_never_reads_buffer() {
    fn scan(path: &Path, count: &mut usize) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                scan(&path, count);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                *count += 1;
                let source = std::fs::read_to_string(&path).unwrap();
                // Ban the dependency token anywhere, covering grouped imports,
                // aliases, pub use, and fully qualified references alike.
                assert!(
                    !source
                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                        .any(|word| word == "buffer"),
                    "forbidden dependency in {}",
                    path.display()
                );
            }
        }
    }
    let mut count = 0;
    scan(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src/verify"),
        &mut count,
    );
    assert!(count >= 1);
}
