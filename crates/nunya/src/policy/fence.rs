//! Tightening classification and synchronous row release fencing.
use super::{JsonColumnFilter, Policy, Treatment};
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};

/// Classify a column change conservatively by disclosure and JSON filtering.
pub fn column_tightens(
    old: Option<&Treatment>,
    new: Option<&Treatment>,
    old_json: Option<&JsonColumnFilter>,
    new_json: Option<&JsonColumnFilter>,
) -> bool {
    if old_json != new_json {
        return true;
    }
    match (old, new) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(Treatment::Show), Some(Treatment::Show)) => false,
        (Some(Treatment::Masked { .. }), Some(Treatment::Show | Treatment::Blind { .. })) => false,
        (Some(Treatment::Blind { .. }), Some(Treatment::Show)) => false,
        (Some(Treatment::Blind { domain: a }), Some(Treatment::Blind { domain: b })) => a != b,
        (
            Some(Treatment::Masked { placeholder: a }),
            Some(Treatment::Masked { placeholder: b }),
        ) => a != b,
        _ => true,
    }
}

/// Whether any column, JSON filter, domain setting or retirement tightens the policy.
/// A domain added or dropped alongside a column change is judged by that column's
/// treatment; a changed setting on a domain present in both versions always tightens.
pub fn tightens(old: &Policy, new: &Policy) -> bool {
    old.domains.iter().any(|(d, settings)| new.domains.get(d).is_some_and(|n| n != settings))
        || old.retired_domains != new.retired_domains
        || old.columns.keys().chain(new.columns.keys())
            .chain(old.json_filters.keys()).chain(new.json_filters.keys()).any(|c| {
                column_tightens(old.treatment(c), new.treatment(c), old.json_filter(c), new.json_filter(c))
            })
}

/// Shared revision epoch and weak registry of connections releasing rows.
pub struct RevisionFence {
    epoch: AtomicU64,
    slots: Mutex<Vec<Weak<ReleaseSlot>>>,
}

impl Default for RevisionFence {
    fn default() -> Self {
        Self {
            epoch: AtomicU64::new(1),
            slots: Mutex::new(Vec::new()),
        }
    }
}
impl std::fmt::Debug for RevisionFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RevisionFence")
    }
}

/// Connection release state: zero for idle, otherwise the checked revision epoch.
pub struct ReleaseSlot {
    state: AtomicU64,
}
impl std::fmt::Debug for ReleaseSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReleaseSlot")
    }
}

impl RevisionFence {
    /// Read the current revision epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(SeqCst)
    }

    /// Register one connection or stream, pruning dropped registrations.
    pub fn register(&self) -> Arc<ReleaseSlot> {
        let slot = Arc::new(ReleaseSlot {
            state: AtomicU64::new(0),
        });
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        slots.retain(|s| s.strong_count() > 0);
        slots.push(Arc::downgrade(&slot));
        slot
    }

    /// Bump after storing the new policy, while still holding its write lock.
    /// A checker observing the new epoch must then read the newly stored policy.
    pub fn publish(&self, old: &Policy, new: &Policy) -> Option<u64> {
        tightens(old, new).then(|| self.epoch.fetch_add(1, SeqCst) + 1)
    }

    /// Count active releases checked before the supplied epoch, pruning dead slots.
    pub fn lagging(&self, epoch: u64) -> usize {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let mut count = 0;
        slots.retain(|s| {
            if let Some(s) = s.upgrade() {
                let state = s.state.load(SeqCst);
                if state > 0 && state < epoch {
                    count += 1;
                }
                true
            } else {
                false
            }
        });
        count
    }

    /// Wait until older releases leave or the timeout expires, returning their count.
    pub fn wait_released(&self, epoch: u64, timeout: Duration) -> usize {
        let start = Instant::now();
        loop {
            let count = self.lagging(epoch);
            if count == 0 || start.elapsed() >= timeout {
                return count;
            }
            std::thread::sleep(
                Duration::from_millis(1).min(timeout.saturating_sub(start.elapsed())),
            );
        }
    }
}

impl ReleaseSlot {
    /// Enter release and return the epoch the caller must check against its statement.
    /// Dekker ordering: the publisher bumps then scans, and the releaser stores then
    /// loads, all SeqCst. Either the releaser sees the bump or the scan sees its lag.
    pub fn begin(&self, fence: &RevisionFence) -> u64 {
        self.state.store(fence.epoch(), SeqCst);
        fence.epoch()
    }
    /// Leave the release region.
    pub fn end(&self) {
        self.state.store(0, SeqCst);
    }
    /// Record a successful statement check at a newer epoch.
    pub fn advance(&self, epoch: u64) {
        self.state.store(epoch, SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnRef;
    use crate::policy::{DomainName, ReadLogSink};
    use std::collections::BTreeMap;

    fn column(name: &str) -> ColumnRef {
        ColumnRef {
            schema: "public".into(),
            table: "t".into(),
            column: name.into(),
        }
    }
    fn policy(treatment: Treatment) -> Policy {
        Policy {
            columns: BTreeMap::from([(column("a"), treatment)]),
            domains: BTreeMap::new(),
            retired_domains: Default::default(),
            json_filters: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: ReadLogSink::Stdout,
        }
    }
    fn blind(name: &str) -> Treatment {
        Treatment::Blind {
            domain: DomainName(name.into()),
        }
    }

    #[test]
    fn treatment_matrix_and_absence() {
        let treatments = [
            Treatment::Show,
            blind("a"),
            blind("b"),
            Treatment::Masked { placeholder: None },
            Treatment::Masked {
                placeholder: Some("hidden".into()),
            },
        ];
        // Rows are old treatment, columns are new treatment.
        let expected = [
            [false, true, true, true, true],
            [false, false, true, true, true],
            [false, true, false, true, true],
            [false, false, false, false, true],
            [false, false, false, true, false],
        ];
        for (i, old) in treatments.iter().enumerate() {
            for (j, new) in treatments.iter().enumerate() {
                assert_eq!(
                    column_tightens(Some(old), Some(new), None, None),
                    expected[i][j]
                );
            }
            assert!(column_tightens(Some(old), None, None, None));
            assert!(!column_tightens(None, Some(old), None, None));
        }
        assert!(!column_tightens(None, None, None, None));
    }

    #[test]
    fn json_changes_always_tighten() {
        use crate::json::filter::JsonMode;
        let show = JsonColumnFilter {
            rules: vec![],
            mode: JsonMode::Show,
        };
        let tokenize = JsonColumnFilter {
            rules: vec![],
            mode: JsonMode::Tokenize,
        };
        for (old, new) in [
            (None, Some(&show)),
            (Some(&show), None),
            (Some(&show), Some(&tokenize)),
        ] {
            assert!(column_tightens(
                Some(&Treatment::Show),
                Some(&Treatment::Show),
                old,
                new
            ));
            assert!(column_tightens(None, None, old, new));
        }
        assert!(!column_tightens(
            Some(&Treatment::Show),
            Some(&Treatment::Show),
            Some(&show),
            Some(&show)
        ));
    }

    #[test]
    fn whole_policy_columns_and_retired_domains() {
        let old = policy(Treatment::Show);
        let mut new = policy(blind("a"));
        assert!(tightens(&old, &new));
        new.retired_domains.insert(DomainName("retired".into()));
        assert!(tightens(&old, &new));
        // Filter-only keys are also included in the union.
        new = old.clone();
        new.json_filters.insert(
            column("outside"),
            JsonColumnFilter {
                rules: vec![],
                mode: crate::json::filter::JsonMode::Show,
            },
        );
        assert!(tightens(&old, &new));
    }

    #[test]
    fn changed_domain_settings_tighten() {
        use crate::policy::{DomainSettings, TokenizeSetting, TypeRange};
        let old = policy(Treatment::Show);
        let mut new = old.clone();
        let domain = DomainName("a".into());
        new.domains.insert(domain.clone(), DomainSettings { tokenize: TokenizeSetting::Off, range: None });
        // Adding or dropping an unused domain discloses nothing by itself.
        assert!(!tightens(&old, &new));
        assert!(!tightens(&new, &old));
        let old = new.clone();
        new.domains.get_mut(&domain).unwrap().range = Some(TypeRange { min: 0, max: 10 });
        assert!(tightens(&old, &new));
        let old = new.clone();
        new.domains.get_mut(&domain).unwrap().tokenize = TokenizeSetting::On {
            limit_per_session: 1, limit_per_principal: 2, principal_window: Duration::from_secs(60),
        };
        assert!(tightens(&old, &new));
        assert!(!tightens(&new, &new));
    }

    #[test]
    fn epoch_changes_only_on_tightening() {
        let fence = RevisionFence::default();
        let show = policy(Treatment::Show);
        let blind = policy(blind("a"));
        assert_eq!(fence.epoch(), 1);
        assert_eq!(fence.publish(&show, &show), None);
        assert_eq!(fence.publish(&show, &blind), Some(2));
        assert_eq!(fence.publish(&blind, &show), None);
        assert_eq!(fence.epoch(), 2);
        assert_eq!(format!("{fence:?}"), "RevisionFence");
    }

    #[test]
    fn release_wait_end_advance_and_pruning() {
        let fence = RevisionFence::default();
        let slot = fence.register();
        assert_eq!(slot.begin(&fence), 1);
        let epoch = fence
            .publish(&policy(Treatment::Show), &policy(blind("a")))
            .unwrap();
        assert_eq!(fence.lagging(epoch), 1);
        assert_eq!(fence.wait_released(epoch, Duration::from_millis(3)), 1);
        slot.advance(epoch);
        assert_eq!(fence.wait_released(epoch, Duration::ZERO), 0);
        slot.advance(1);
        slot.end();
        assert_eq!(fence.wait_released(epoch, Duration::ZERO), 0);
        drop(slot);
        assert_eq!(fence.lagging(epoch), 0);
        assert!(fence.slots.lock().unwrap().is_empty());
        drop(fence.register());
        let live = fence.register();
        assert_eq!(fence.slots.lock().unwrap().len(), 1);
        assert_eq!(format!("{live:?}"), "ReleaseSlot");
    }

    #[test]
    fn concurrent_publication_never_loses_active_release() {
        use std::sync::Barrier;
        let fence = Arc::new(RevisionFence::default());
        let slot = fence.register();
        let barrier = Arc::new(Barrier::new(2));
        let seen = Arc::new(AtomicU64::new(0));
        let mut lost_lag = false;
        std::thread::scope(|scope| {
            let (child_fence, child_barrier, child_seen) =
                (fence.clone(), barrier.clone(), seen.clone());
            scope.spawn(move || {
                for _ in 0..100 {
                    child_barrier.wait();
                    child_seen.store(slot.begin(&child_fence), SeqCst);
                    child_barrier.wait(); // Keep the release active until the scan finishes.
                    child_barrier.wait();
                    slot.end();
                }
            });
            for _ in 0..100 {
                barrier.wait();
                let epoch = fence
                    .publish(&policy(Treatment::Show), &policy(blind("a")))
                    .unwrap();
                barrier.wait();
                lost_lag |= seen.load(SeqCst) < epoch && fence.lagging(epoch) == 0;
                barrier.wait();
            }
        });
        assert!(!lost_lag);
    }
}
