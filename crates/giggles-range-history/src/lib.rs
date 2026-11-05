//! External bounded multi-range process histories. See the package README.
#![forbid(unsafe_code)]
mod attribution;
mod process;
mod scenario;
pub use attribution::validate_trace;
use giggles_core::{Key, Row, RowId, Status, TenantId};
use giggles_history::{History, Observation, Verdict};
pub use scenario::run;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize)]
pub struct CaptureResult {
    pub artifacts: PathBuf,
    pub original_verdict: Verdict,
    pub verdict: Verdict,
    pub controls: BTreeMap<String, Verdict>,
    pub runtime_checks: BTreeMap<String, bool>,
}
pub(crate) fn combined_verdict(
    original: Verdict,
    controls: &BTreeMap<String, Verdict>,
    checks: &BTreeMap<String, bool>,
) -> Verdict {
    if original != Verdict::Pass {
        return original;
    }
    if checks.values().any(|v| !*v)
        || controls.len() != 3
        || controls.values().any(|v| *v == Verdict::Inconclusive)
    {
        return Verdict::Inconclusive;
    }
    if controls.values().any(|v| *v != Verdict::Fail) {
        return Verdict::Fail;
    }
    Verdict::Pass
}
pub(crate) fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}
pub(crate) fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
pub fn fresh(parent: &Path, label: &str) -> Result<PathBuf> {
    let p = parent.join(format!(
        "{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&p)?;
    Ok(p)
}
pub(crate) fn key(id: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(id),
    }
}
/// Balance encoding. Runtime-owned row versions are retained for CAS separately.
pub fn balance_row(balance: u32) -> Row {
    Row {
        status: Status::Failed,
        failed_at: 103,
        attempt_count: u64::from(balance),
        version: 0,
    }
}
/// Reject every row outside the advertised two-account encoding.
pub fn decode(rows: &[(Key, Row)]) -> Result<Vec<(i64, i64)>> {
    require(
        rows.windows(2).all(|w| w[0].0 < w[1].0),
        "noncanonical scan",
    )?;
    rows.iter()
        .map(|(k, r)| {
            require(
                k.tenant == TenantId(42) && [1, 60].contains(&k.row.0),
                "unsupported account",
            )?;
            require(
                r.status == Status::Failed && r.failed_at == 103,
                "unsupported row encoding",
            )?;
            Ok((
                i64::try_from(k.row.0)?,
                i64::from(u32::try_from(r.attempt_count)?),
            ))
        })
        .collect()
}
/// Source closure including every local path dependency used by this harness.
pub fn source_hashes() -> Result<BTreeMap<String, String>> {
    fn visit(p: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        for e in std::fs::read_dir(p)? {
            let p = e?.path();
            if p.is_dir() {
                visit(&p, files)?;
            } else {
                files.push(p);
            }
        }
        Ok(())
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("missing crates directory")?;
    let mut files = vec![];
    for name in [
        "giggles-core",
        "giggles-model",
        "giggles-membership",
        "giggles-transactions",
        "giggles-distributed",
        "giggles-history",
    ] {
        let base = root.join(name);
        visit(&base.join("src"), &mut files)?;
        for n in ["Cargo.toml", "Cargo.lock"] {
            let p = base.join(n);
            if p.exists() {
                files.push(p);
            }
        }
    }
    files.sort();
    files
        .into_iter()
        .map(|p| {
            Ok((
                p.strip_prefix(root)?.to_string_lossy().replace('\\', "/"),
                format!("{:x}", Sha256::digest(std::fs::read(&p)?)),
            ))
        })
        .collect()
}
/// Corrupt copied externally observed scans; never change the original history.
pub fn negative_controls(history: &History) -> Result<BTreeMap<String, History>> {
    let index = history
        .attempts
        .iter()
        .rposition(|a| matches!(a.observations.first(), Some(Observation::Scan { .. })))
        .ok_or("no scan witness")?;
    let amount = history.attempts[..index]
        .iter()
        .rev()
        .find_map(|a| {
            if a.outcome == giggles_history::Outcome::Committed {
                a.ops.iter().find_map(|op| match op {
                    giggles_history::Op::Transfer { amount, .. } => Some(*amount),
                    _ => None,
                })
            } else {
                None
            }
        })
        .ok_or("no acknowledged transfer witness")?;
    let mut out = BTreeMap::new();
    for kind in ["stale_read", "partial_write", "double_effect"] {
        let mut h = history.clone();
        let Observation::Scan { rows } = &mut h.attempts[index].observations[0] else {
            return Err("missing witness".into());
        };
        require(rows.len() == 2, "two-account witness required")?;
        match kind {
            "stale_read" => *rows = history.initial.iter().map(|(&k, &v)| (k, v)).collect(),
            "partial_write" => rows[0].1 += amount,
            _ => {
                rows[0].1 -= amount;
                rows[1].1 += amount;
            }
        }
        out.insert(kind.into(), h);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn encoding_roundtrip_boundaries() {
        for n in [0, 1, u32::MAX] {
            assert_eq!(
                decode(&[(key(1), balance_row(n))]).unwrap(),
                vec![(1, i64::from(n))]
            );
        }
    }
    #[test]
    fn rejects_foreign_tenant_status_timestamp_key() {
        for n in 0..4 {
            let mut k = key(1);
            let mut r = balance_row(1);
            match n {
                0 => k.tenant = TenantId(0),
                1 => k.row = RowId(2),
                2 => r.failed_at = 0,
                _ => r.status = Status::Queued,
            };
            assert!(decode(&[(k, r)]).is_err());
        }
    }
    #[test]
    fn rejects_duplicate_unsorted_scan() {
        for ids in [[1, 1], [60, 1]] {
            assert!(decode(&ids.map(|k| (key(k), balance_row(1)))).is_err());
        }
    }
    #[test]
    fn checker_limit_is_inconclusive() {
        let h = giggles_history::generate(42, 3).unwrap();
        assert_eq!(
            giggles_history::check(
                &h,
                giggles_history::Limits {
                    max_nodes: 0,
                    ..Default::default()
                }
            )
            .unwrap()
            .verdict,
            Verdict::Inconclusive
        );
    }
    #[test]
    fn rejects_balance_outside_profile() {
        for n in [u64::from(u32::MAX) + 1, u64::MAX] {
            let mut r = balance_row(0);
            r.attempt_count = n;
            assert!(decode(&[(key(1), r)]).is_err());
        }
    }
    #[test]
    fn versions_are_separate_from_balance_projection() {
        for version in [0, 1, u64::MAX] {
            let mut r = balance_row(7);
            r.version = version;
            assert_eq!(decode(&[(key(60), r)]).unwrap(), vec![(60, 7)]);
        }
    }
    #[test]
    fn all_small_balances_roundtrip() {
        for n in 0..4096 {
            assert_eq!(
                decode(&[(key(1), balance_row(n)), (key(60), balance_row(4096 - n))]).unwrap(),
                vec![(1, i64::from(n)), (60, i64::from(4096 - n))]
            );
        }
    }
    #[test]
    fn negative_controls_reject() {
        use giggles_history::{Attempt, Op, Outcome};
        let mut h = History {
            version: 1,
            horizon: 5,
            initial: BTreeMap::from([(1, 100), (60, 100)]),
            invariants: Default::default(),
            attempts: vec![
                Attempt {
                    id: "t".into(),
                    logical_id: "t".into(),
                    invoke: 0,
                    complete: Some(1),
                    outcome: Outcome::Committed,
                    durable: None,
                    decision_at: None,
                    ops: vec![Op::Transfer {
                        from: 1,
                        to: 60,
                        amount: 7,
                    }],
                    observations: vec![],
                    receipt: None,
                },
                Attempt {
                    id: "r".into(),
                    logical_id: "r".into(),
                    invoke: 2,
                    complete: Some(3),
                    outcome: Outcome::Committed,
                    durable: None,
                    decision_at: None,
                    ops: vec![Op::Scan { start: 0, end: 100 }],
                    observations: vec![Observation::Scan {
                        rows: vec![(1, 93), (60, 107)],
                    }],
                    receipt: None,
                },
            ],
        };
        let original = h.clone();
        assert_eq!(
            giggles_history::check(&h, Default::default())
                .unwrap()
                .verdict,
            Verdict::Pass
        );
        for (name, c) in negative_controls(&h).unwrap() {
            assert_eq!(
                giggles_history::check(&c, Default::default())
                    .unwrap()
                    .verdict,
                Verdict::Fail,
                "{name}"
            );
        }
        assert_eq!(h, original);
        h.attempts.clear();
        assert!(negative_controls(&h).is_err());
    }
    #[test]
    fn incomplete_controls_or_changed_sources_cannot_pass() {
        let good = BTreeMap::from([
            ("stale_read".into(), Verdict::Fail),
            ("partial_write".into(), Verdict::Fail),
            ("double_effect".into(), Verdict::Fail),
        ]);
        let checks = BTreeMap::from([("sources".into(), true)]);
        assert_eq!(
            combined_verdict(Verdict::Pass, &good, &checks),
            Verdict::Pass
        );
        assert_eq!(
            combined_verdict(Verdict::Pass, &BTreeMap::new(), &checks),
            Verdict::Inconclusive
        );
        assert_eq!(
            combined_verdict(
                Verdict::Pass,
                &good,
                &BTreeMap::from([("sources".into(), false)])
            ),
            Verdict::Inconclusive
        );
        for v in [Verdict::Fail, Verdict::Inconclusive] {
            assert_eq!(combined_verdict(v, &good, &checks), v);
        }
        for v in [Verdict::Pass, Verdict::Inconclusive] {
            let mut c = good.clone();
            c.insert("stale_read".into(), v);
            assert_eq!(
                combined_verdict(Verdict::Pass, &c, &checks),
                if v == Verdict::Pass {
                    Verdict::Fail
                } else {
                    Verdict::Inconclusive
                }
            );
        }
    }
}
