use std::cmp::Ordering;

use async_trait::async_trait;

use super::{
    Comparison, Key, KeyOrder, NotVerifiableReason, Projection, RangePlan, Result, Row, RowSource,
    TableCoverage, VerifyError,
    compare::{compare_key, compare_with_changes, validate_range},
};

/// PostgreSQL WAL byte position in the single source lineage bound to a session.
/// Adapters must bind the source, applier and evidence to that same lineage.
pub type Lsn = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceRead {
    pub rows: Vec<Row>,
    pub lsn: Lsn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeEvidence {
    pub through_lsn: Lsn,
    /// Complete, strictly key-ordered set, including inserts, deletes, key
    /// changes (old AND new key), and changes later reverted to the old value.
    pub keys: Vec<Key>,
}

#[async_trait]
pub trait BoundarySource: RowSource {
    /// Read a consistent source range and record its source LSN. The adapter
    /// must ensure every change not visible to this snapshot is included by
    /// changed_since, including transactions in flight at snapshot creation.
    /// A separately sampled WAL position without that guarantee is insufficient.
    async fn read_source(
        &mut self,
        range: &RangePlan,
        projection: &Projection,
    ) -> Result<SourceRead>;

    /// Called AFTER the destination read completes. Evidence must cover the
    /// entire source-read/destination-read interval, not merely the wait target.
    /// Missing evidence is an error, never an empty set or a count heuristic.
    async fn changed_since(&mut self, range: &RangePlan, source_lsn: Lsn)
    -> Result<ChangeEvidence>;
}

#[async_trait]
pub trait Applier: Send {
    /// Wait using applied source positions, not a destination WAL position.
    /// May return an error/timeout; a below-target return is rejected as well.
    async fn wait_until(&mut self, source_lsn: Lsn) -> Result<Lsn>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundaryState {
    ReadSource,
    WaitForApplier {
        source_lsn: Lsn,
    },
    ReadDestination {
        source_lsn: Lsn,
    },
    CheckChanges {
        source_lsn: Lsn,
    },
    Requeued {
        busy: u64,
        retries_remaining: u32,
    },
    /// No comparison exists for this range. In particular, an unknown or empty
    /// row set must not contribute a successful 0=0 comparison to coverage.
    NotVerifiable {
        range: crate::pg::chunk::ChunkId,
        reason: NotVerifiableReason,
    },
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verification {
    pub range: RangePlan,
    pub comparison: Comparison,
    pub attempts: u32,
}

impl Verification {
    /// This range's contribution; use TableCoverage::from_ranges to aggregate
    /// and validate the complete table plan without double-counting retries.
    pub fn table_coverage(&self) -> TableCoverage {
        TableCoverage {
            table: self.range.id.table.clone(),
            rows: self.comparison.coverage.clone(),
            exclusion_reason: None,
        }
    }
}

/// An explicit, resumable-in-memory boundary protocol for one planned range.
/// `max_retries` counts extra attempts after the first. Requeue retries the
/// whole range, preserving every observed key in the coverage denominator.
/// Cancellation and infrastructure errors retain the current state. Evidence
/// loss/timeout makes the range explicitly NotVerifiable and returns its typed
/// error; retry requires a fresh session/evidence interval. Complete is idempotent.
pub struct VerificationSession {
    range: RangePlan,
    projection: Projection,
    max_retries: u32,
    attempts: u32,
    state: BoundaryState,
    source: Option<SourceRead>,
    destination: Vec<Row>,
    known_keys: Vec<Key>,
    last_lsn: Option<Lsn>,
    result: Option<Verification>,
}

impl VerificationSession {
    pub fn new(range: RangePlan, projection: Projection, max_retries: u32) -> Result<Self> {
        if max_retries == u32::MAX {
            return Err(VerifyError::Invalid(
                "retry budget overflows attempt counter",
            ));
        }
        Ok(Self {
            range,
            projection,
            max_retries,
            attempts: 0,
            state: BoundaryState::ReadSource,
            source: None,
            destination: Vec::new(),
            known_keys: Vec::new(),
            last_lsn: None,
            result: None,
        })
    }

    pub fn state(&self) -> &BoundaryState {
        &self.state
    }

    /// Intermediate results remain unavailable while busy work is queued.
    pub fn result(&self) -> Option<&Verification> {
        if self.state == BoundaryState::Complete {
            self.result.as_ref()
        } else {
            None
        }
    }

    pub async fn advance(
        &mut self,
        source: &mut (impl BoundarySource + ?Sized),
        destination: &mut (impl RowSource + ?Sized),
        applier: &mut (impl Applier + ?Sized),
    ) -> Result<&BoundaryState> {
        if let Err(error) = Box::pin(self.advance_attempt(source, destination, applier)).await {
            self.record_error(&error);
            return Err(error);
        }
        Ok(&self.state)
    }

    fn record_error(&mut self, error: &VerifyError) {
        if let Some(reason) = error.not_verifiable_reason(&self.range.id) {
            self.result = None;
            self.source = None;
            self.destination.clear();
            self.state = BoundaryState::NotVerifiable {
                range: self.range.id.clone(),
                reason,
            };
        }
    }

    async fn advance_attempt(
        &mut self,
        source: &mut (impl BoundarySource + ?Sized),
        destination: &mut (impl RowSource + ?Sized),
        applier: &mut (impl Applier + ?Sized),
    ) -> Result<()> {
        match self.state.clone() {
            BoundaryState::ReadSource => {
                validate_range(source, &self.range)?;
                let read = source.read_source(&self.range, &self.projection).await?;
                if self.last_lsn.is_some_and(|previous| read.lsn < previous) {
                    return Err(VerifyError::Invalid("source LSN regressed"));
                }
                self.state = BoundaryState::WaitForApplier {
                    source_lsn: read.lsn,
                };
                self.source = Some(read);
                self.attempts += 1;
            }
            BoundaryState::WaitForApplier { source_lsn } => {
                if applier.wait_until(source_lsn).await? < source_lsn {
                    return Err(VerifyError::Invalid("applier has not reached source LSN"));
                }
                self.state = BoundaryState::ReadDestination { source_lsn };
            }
            BoundaryState::ReadDestination { source_lsn } => {
                self.destination = destination.fetch(&self.range, &self.projection).await?;
                self.state = BoundaryState::CheckChanges { source_lsn };
            }
            BoundaryState::CheckChanges { source_lsn } => {
                let changes = source.changed_since(&self.range, source_lsn).await?;
                if changes.through_lsn < source_lsn {
                    return Err(VerifyError::Invalid(
                        "change evidence predates source boundary",
                    ));
                }
                let read = self.source.as_ref().expect("source read precedes changes");
                let comparison = compare_with_changes(
                    source,
                    &self.range,
                    &self.projection,
                    &read.rows,
                    &self.destination,
                    &changes.keys,
                    &self.known_keys,
                )?;
                let source_keys: Vec<_> = read.rows.iter().map(|row| row.key.clone()).collect();
                let destination_keys: Vec<_> =
                    self.destination.iter().map(|row| row.key.clone()).collect();
                let known = union(source, &self.known_keys, &source_keys)?;
                let known = union(source, &known, &destination_keys)?;
                self.known_keys = union(source, &known, &changes.keys)?;
                self.last_lsn = Some(changes.through_lsn);
                let busy = comparison.coverage.busy;
                self.result = Some(Verification {
                    range: self.range.clone(),
                    comparison,
                    attempts: self.attempts,
                });
                if busy > 0 && self.attempts <= self.max_retries {
                    self.state = BoundaryState::Requeued {
                        busy,
                        retries_remaining: self.max_retries - self.attempts + 1,
                    };
                } else {
                    self.state = BoundaryState::Complete;
                }
            }
            BoundaryState::Requeued { .. } => {
                self.source = None;
                self.destination.clear();
                self.result = None;
                self.state = BoundaryState::ReadSource;
            }
            BoundaryState::Complete => {}
            BoundaryState::NotVerifiable { range, reason } => {
                return Err(reason.into_error(range));
            }
        }
        Ok(())
    }

    pub async fn run(
        &mut self,
        source: &mut (impl BoundarySource + ?Sized),
        destination: &mut (impl RowSource + ?Sized),
        applier: &mut (impl Applier + ?Sized),
    ) -> Result<&Verification> {
        while self.state != BoundaryState::Complete {
            self.advance(source, destination, applier).await?;
        }
        Ok(self.result.as_ref().expect("complete comparison"))
    }
}

fn union(order: &(impl KeyOrder + ?Sized), left: &[Key], right: &[Key]) -> Result<Vec<Key>> {
    let mut result = Vec::with_capacity(left.len() + right.len());
    let (mut a, mut b) = (0, 0);
    while a < left.len() || b < right.len() {
        match (left.get(a), right.get(b)) {
            (Some(x), Some(y)) => match compare_key(order, x, y)? {
                Ordering::Less => {
                    result.push(x.clone());
                    a += 1;
                }
                Ordering::Greater => {
                    result.push(y.clone());
                    b += 1;
                }
                Ordering::Equal => {
                    result.push(x.clone());
                    a += 1;
                    b += 1;
                }
            },
            (Some(x), None) => {
                result.push(x.clone());
                a += 1;
            }
            (None, Some(y)) => {
                result.push(y.clone());
                b += 1;
            }
            (None, None) => unreachable!(),
        }
    }
    Ok(result)
}

#[cfg(test)]
mod operational_tests {
    use super::*;
    use crate::{
        pg::chunk::ChunkId,
        verify::{ApplierTerminalCause, EvidenceInvalidation, EvidenceLimit},
    };
    use std::time::Duration;

    // Trait-contract probes return typed faults without emulating any database
    // or capture behavior. Production producer qualification is separate.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FaultAt {
        Source,
        Wait,
        Changes,
    }

    struct ContractProbe {
        fault: Option<(FaultAt, VerifyError)>,
    }

    impl ContractProbe {
        fn take_fault(&mut self, at: FaultAt) -> Result<()> {
            if self.fault.as_ref().is_some_and(|(phase, _)| *phase == at) {
                return Err(self.fault.take().unwrap().1);
            }
            Ok(())
        }
    }

    impl KeyOrder for ContractProbe {
        fn compare_keys(&self, _: &Key, _: &Key) -> Result<std::cmp::Ordering> {
            panic!("contract probes never compare keys")
        }
    }

    #[async_trait]
    impl RowSource for ContractProbe {
        async fn fetch(&mut self, _: &RangePlan, _: &Projection) -> Result<Vec<Row>> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl BoundarySource for ContractProbe {
        async fn read_source(&mut self, _: &RangePlan, _: &Projection) -> Result<SourceRead> {
            self.take_fault(FaultAt::Source)?;
            Ok(SourceRead {
                rows: vec![],
                lsn: 10,
            })
        }

        async fn changed_since(&mut self, _: &RangePlan, _: Lsn) -> Result<ChangeEvidence> {
            self.take_fault(FaultAt::Changes)?;
            panic!("contract probe must fail before comparison")
        }
    }

    #[async_trait]
    impl Applier for ContractProbe {
        async fn wait_until(&mut self, source_lsn: Lsn) -> Result<Lsn> {
            self.take_fault(FaultAt::Wait)?;
            Ok(source_lsn)
        }
    }

    fn session() -> VerificationSession {
        VerificationSession::new(
            RangePlan {
                id: ChunkId {
                    table: "public.items".into(),
                    plan: "a".repeat(64),
                    ordinal: 0,
                },
                lo: None,
                hi: None,
            },
            Projection::new(vec!["value".into()]).unwrap(),
            1,
        )
        .unwrap()
    }

    #[test]
    fn overflow_before_snapshot_has_no_completed_empty_comparison() {
        let mut session = session();
        let error = VerifyError::EvidenceOverflow {
            range: session.range.id.clone(),
            limit: EvidenceLimit::Keys(4),
        };
        session.record_error(&error);
        assert_eq!(
            session.state(),
            &BoundaryState::NotVerifiable {
                range: session.range.id.clone(),
                reason: NotVerifiableReason::EvidenceOverflow {
                    limit: EvidenceLimit::Keys(4)
                },
            }
        );
        assert!(session.result().is_none());
        assert!(session.known_keys.is_empty());
        assert!(session.source.is_none());
        assert_eq!(session.attempts, 0);
    }

    #[test]
    fn overflow_for_another_range_does_not_demote_this_range() {
        let mut session = session();
        let mut affected = session.range.id.clone();
        affected.ordinal = 1;
        session.record_error(&VerifyError::EvidenceOverflow {
            range: affected,
            limit: EvidenceLimit::Keys(4),
        });
        assert_eq!(session.state(), &BoundaryState::ReadSource);
        assert!(session.result().is_none());
    }

    #[test]
    fn every_continuity_failure_invalidates_the_affected_range() {
        for reason in [
            EvidenceInvalidation::CaptureGap,
            EvidenceInvalidation::Reconnected,
            EvidenceInvalidation::LineageChanged,
            EvidenceInvalidation::SlotChanged,
        ] {
            let mut session = session();
            session.state = BoundaryState::CheckChanges { source_lsn: 10 };
            session.source = Some(SourceRead {
                rows: vec![],
                lsn: 10,
            });
            session.record_error(&VerifyError::EvidenceInvalidated { reason });
            assert_eq!(
                session.state(),
                &BoundaryState::NotVerifiable {
                    range: session.range.id.clone(),
                    reason: NotVerifiableReason::EvidenceInvalidated { reason },
                }
            );
            assert!(session.result().is_none());
            assert!(session.source.is_none());
        }
    }

    #[test]
    fn timeout_is_not_verifiable_and_retains_typed_wait_details() {
        let mut session = session();
        let reason = NotVerifiableReason::ApplierTimeout {
            waited_lsn: 42,
            elapsed: Duration::from_millis(27),
        };
        session.state = BoundaryState::WaitForApplier { source_lsn: 42 };
        let error = reason.clone().into_error(session.range.id.clone());
        session.record_error(&error);
        assert_eq!(
            session.state(),
            &BoundaryState::NotVerifiable {
                range: session.range.id.clone(),
                reason: reason.clone(),
            }
        );
        assert_eq!(error.not_verifiable_reason(&session.range.id), Some(reason));
        assert!(session.result().is_none());
    }

    #[test]
    fn terminal_failure_is_infrastructure_and_does_not_become_busy() {
        for cause in [
            ApplierTerminalCause::CaptureStopped,
            ApplierTerminalCause::ApplyStopped,
            ApplierTerminalCause::CheckpointRejected,
        ] {
            let mut session = session();
            session.state = BoundaryState::WaitForApplier { source_lsn: 42 };
            let error = VerifyError::ApplierTerminal { cause };
            session.record_error(&error);
            assert_eq!(
                session.state(),
                &BoundaryState::WaitForApplier { source_lsn: 42 }
            );
            assert!(error.not_verifiable_reason(&session.range.id).is_none());
            assert!(session.result().is_none());
        }
    }

    #[test]
    fn operational_reasons_round_trip_without_string_interpretation() {
        for reason in [
            NotVerifiableReason::EvidenceOverflow {
                limit: EvidenceLimit::Keys(64),
            },
            NotVerifiableReason::EvidenceInvalidated {
                reason: EvidenceInvalidation::CaptureGap,
            },
            NotVerifiableReason::ApplierTimeout {
                waited_lsn: 81,
                elapsed: Duration::from_millis(5),
            },
        ] {
            let session = session();
            let error = reason.clone().into_error(session.range.id.clone());
            assert_eq!(error.not_verifiable_reason(&session.range.id), Some(reason));
        }
    }

    #[tokio::test]
    async fn public_run_propagates_operational_errors_without_a_comparison() {
        for (phase, reason) in [
            (
                FaultAt::Source,
                NotVerifiableReason::EvidenceOverflow {
                    limit: EvidenceLimit::Keys(4),
                },
            ),
            (
                FaultAt::Wait,
                NotVerifiableReason::ApplierTimeout {
                    waited_lsn: 10,
                    elapsed: Duration::from_millis(3),
                },
            ),
            (
                FaultAt::Changes,
                NotVerifiableReason::EvidenceInvalidated {
                    reason: EvidenceInvalidation::CaptureGap,
                },
            ),
        ] {
            let mut session = session();
            let fault = Some((phase, reason.clone().into_error(session.range.id.clone())));
            let (source_fault, applier_fault) = if phase == FaultAt::Wait {
                (None, fault)
            } else {
                (fault, None)
            };
            let mut source = ContractProbe {
                fault: source_fault,
            };
            let mut destination = ContractProbe { fault: None };
            let mut applier = ContractProbe {
                fault: applier_fault,
            };
            let error = session
                .run(&mut source, &mut destination, &mut applier)
                .await
                .unwrap_err();
            assert_eq!(
                error.not_verifiable_reason(&session.range.id),
                Some(reason.clone())
            );
            assert_eq!(
                session.state(),
                &BoundaryState::NotVerifiable {
                    range: session.range.id.clone(),
                    reason: reason.clone(),
                }
            );
            assert!(session.result().is_none());
            // A repeat cannot turn an exhausted/unavailable interval into 0=0.
            let error = session
                .advance(&mut source, &mut destination, &mut applier)
                .await
                .unwrap_err();
            assert_eq!(error.not_verifiable_reason(&session.range.id), Some(reason));
            assert!(session.result().is_none());
        }
    }

    #[tokio::test]
    async fn public_run_returns_terminal_error_as_infrastructure() {
        let mut session = session();
        let mut source = ContractProbe { fault: None };
        let mut destination = ContractProbe { fault: None };
        let mut applier = ContractProbe {
            fault: Some((
                FaultAt::Wait,
                VerifyError::ApplierTerminal {
                    cause: ApplierTerminalCause::ApplyStopped,
                },
            )),
        };
        let error = session
            .run(&mut source, &mut destination, &mut applier)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            VerifyError::ApplierTerminal {
                cause: ApplierTerminalCause::ApplyStopped,
            }
        ));
        assert_eq!(
            session.state(),
            &BoundaryState::WaitForApplier { source_lsn: 10 }
        );
        assert!(session.result().is_none());
    }
}
