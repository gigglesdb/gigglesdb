//! Scoped milestone-1 catalog detection. Relation OIDs deliberately bind identity:
//! drop/recreate must block; storage rewrites (relfilenode) do not affect this hash.
use super::{DdlError, PollPoint, validate_fingerprint};
use crate::pg::chunk::QualifiedName;
use serde::{Deserialize, Serialize};
use tokio_postgres::GenericClient;

/// The stored migration definition is the sole authority for fingerprint scope.
/// Exclusions are advisory; only explicitly selected tables enter the scope.
pub fn scoped_schema_tables(definition: &crate::MigrationDefinition) -> Vec<QualifiedName> {
    definition
        .selected_objects
        .iter()
        .filter(|object| object.kind == crate::definition::ObjectKind::Table)
        .map(|object| (object.schema.clone(), object.name.clone()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|(schema, name)| QualifiedName { schema, name })
        .collect()
}

pub const SCOPED_FINGERPRINT_SQL: &str = include_str!("operational.sql");
pub const HAZARD_SQL: &str = r#"
WITH scope AS (
 SELECT c.oid, n.nspname, c.relname FROM unnest($1::text[], $2::text[]) s(schema_name, table_name)
 JOIN pg_namespace n ON n.nspname=s.schema_name JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=s.table_name
)
SELECT 'invalid-or-unready-index'::text AS kind, format('%I.%I',s.nspname,s.relname) AS relation, i.indexrelid::regclass::text AS object
 FROM scope s JOIN pg_index i ON i.indrelid=s.oid WHERE NOT i.indisvalid OR NOT i.indisready
UNION ALL SELECT 'index-build-in-progress',format('%I.%I',s.nspname,s.relname),p.command
 FROM scope s JOIN pg_stat_progress_create_index p ON p.relid=s.oid OR p.relid=0 OR p.relid IS NULL WHERE p.datid=(SELECT oid FROM pg_database WHERE datname=current_database()) OR p.datid IS NULL
UNION ALL SELECT 'partition-detach-pending',format('%I.%I',s.nspname,s.relname),h.inhrelid::regclass::text
 FROM scope s JOIN pg_inherits h ON h.inhrelid=s.oid OR h.inhparent=s.oid WHERE h.inhdetachpending
ORDER BY 1,2,3
"#;

pub const SCHEMA_REMEDIATION: &str = "Keep capture buffering; stop apply and readiness. Quiesce schema changes and restart from a fresh consistent snapshot; a fingerprint cannot identify the first affected commit.";
pub const BASELINE_STREAM: &str = "scoped-schema-baseline-v1";
pub const BLOCKER_STREAM: &str = "scoped-schema-blocker-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hazard {
    pub kind: String,
    pub relation: String,
    pub object: String,
}
/// Pure classification; unknown hazard names remain blockers.
pub fn classify_hazards(rows: Vec<Hazard>) -> Vec<Hazard> {
    let mut rows = rows;
    rows.sort_by(|a, b| (&a.kind, &a.relation, &a.object).cmp(&(&b.kind, &b.relation, &b.object)));
    rows.dedup();
    rows
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaObservation {
    pub fingerprint: String,
    pub hazards: Vec<Hazard>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaBlocker {
    pub reason: String,
    pub observed: SchemaObservation,
    pub point: PollPoint,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalSchema {
    pub baseline: Option<String>,
    pub blocker: Option<SchemaBlocker>,
}
impl OperationalSchema {
    pub fn is_empty(&self) -> bool {
        self.baseline.is_none() && self.blocker.is_none()
    }
    pub fn require_clear(&self) -> Result<(), DdlError> {
        if let Some(b) = &self.blocker {
            return Err(DdlError(format!(
                "scoped-schema-blocked: {} at {:?}; observed={:?}. {}",
                b.reason, b.point, b.observed, SCHEMA_REMEDIATION
            )));
        }
        Ok(())
    }
    pub fn poll(&mut self, point: PollPoint, observed: SchemaObservation) -> Result<(), DdlError> {
        self.require_clear()?;
        let reason = if !observed.hazards.is_empty() {
            Some("split-transaction DDL hazard")
        } else if validate_fingerprint(&observed.fingerprint).is_err() {
            Some("invalid catalog fingerprint")
        } else if self.baseline.as_ref() != Some(&observed.fingerprint) {
            Some("scoped catalog drift or missing baseline")
        } else {
            None
        };
        if let Some(reason) = reason {
            self.blocker = Some(SchemaBlocker {
                reason: reason.into(),
                observed,
                point,
            });
        }
        self.require_clear()
    }
}
/// One catalog statement supplies the hash; hazards are sampled separately on
/// both sides of slot creation and every poll. Query errors fail closed.
pub async fn observe(
    client: &impl GenericClient,
    tables: &[QualifiedName],
) -> Result<SchemaObservation, tokio_postgres::Error> {
    let schemas: Vec<_> = tables.iter().map(|t| t.schema.clone()).collect();
    let names: Vec<_> = tables.iter().map(|t| t.name.clone()).collect();
    let fingerprint = client
        .query_one(SCOPED_FINGERPRINT_SQL, &[&schemas, &names])
        .await?
        .get(0);
    let rows = client.query(HAZARD_SQL, &[&schemas, &names]).await?;
    Ok(SchemaObservation {
        fingerprint,
        hazards: classify_hazards(
            rows.into_iter()
                .map(|r| Hazard {
                    kind: r.get(0),
                    relation: r.get(1),
                    object: r.get(2),
                })
                .collect(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_hazards_are_deterministic_and_fail_closed() {
        let h = Hazard {
            kind: "partition-detach-pending".into(),
            relation: "public.t".into(),
            object: "public.child".into(),
        };
        assert_eq!(classify_hazards(vec![h.clone(), h.clone()]), vec![h]);
        assert!(classify_hazards(vec![]).is_empty());
        for kind in [
            "invalid-or-unready-index",
            "index-build-in-progress",
            "partition-detach-pending",
            "unknown",
        ] {
            let mut state = OperationalSchema {
                baseline: Some("a".repeat(32)),
                blocker: None,
            };
            assert!(
                state
                    .poll(
                        PollPoint::Watermark(1),
                        SchemaObservation {
                            fingerprint: "a".repeat(32),
                            hazards: vec![Hazard {
                                kind: kind.into(),
                                relation: "t".into(),
                                object: "i".into()
                            }]
                        }
                    )
                    .is_err()
            );
        }
    }
    #[test]
    fn blocker_survives_restart_and_return_to_baseline() {
        let mut state = OperationalSchema {
            baseline: Some("a".repeat(32)),
            blocker: None,
        };
        assert!(
            state
                .poll(
                    PollPoint::FinalMarker(2),
                    SchemaObservation {
                        fingerprint: "b".repeat(32),
                        hazards: vec![]
                    }
                )
                .is_err()
        );
        let mut restarted: OperationalSchema =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        assert!(
            restarted
                .poll(
                    PollPoint::Watermark(3),
                    SchemaObservation {
                        fingerprint: "a".repeat(32),
                        hazards: vec![]
                    }
                )
                .is_err()
        );
        assert_eq!(state, restarted);
    }
}
