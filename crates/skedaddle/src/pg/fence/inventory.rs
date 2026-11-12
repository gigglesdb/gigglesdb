//! Read-only, conservative source writer inventory. Function bodies, dynamic SQL
//! and arbitrary external producers cannot be proven safe by catalog inspection.
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use super::Rows;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    OutOfScopeWriter,
    Superuser,
    TableOwner,
    BypassRls,
    SecurityDefiner,
    UnscannedSecurityDefiner,
    UnscannedTrigger,
    ActiveWriterSession,
    PreparedWriter,
    Subscription,
    UnscannedInboundWriter,
}

impl Kind {
    fn parse(value: &str) -> Result<Self, String> {
        serde_json::from_value(serde_json::Value::String(value.into()))
            .map_err(|_| format!("unknown fence inventory kind: {value}"))
    }
    fn name(self) -> String {
        serde_json::to_value(self)
            .expect("unit inventory enum")
            .as_str()
            .expect("inventory enum name")
            .into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Risk {
    pub id: String,
    pub kind: Kind,
    pub principal: String,
    pub object: String,
    pub evidence: String,
}

/// Names, rather than transient OIDs/PIDs or evidence text, define acknowledgments.
/// Hex encoding prevents delimiter collisions, including quoted Unicode names.
pub fn from_rows(rows: Rows) -> Result<Vec<Risk>, String> {
    let mut risks = std::collections::BTreeMap::new();
    for row in rows {
        if row.len() != 4 || row[1].is_empty() || row[2].is_empty() {
            return Err("incomplete fence inventory row".into());
        }
        let kind = Kind::parse(&row[0])?;
        let hex = |s: &str| {
            s.as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        let id = format!("fence-v1:{}:{}:{}", kind.name(), hex(&row[1]), hex(&row[2]));
        risks.insert(
            id.clone(),
            Risk {
                id,
                kind,
                principal: row[1].clone(),
                object: row[2].clone(),
                evidence: row[3].clone(),
            },
        );
    }
    Ok(risks.into_values().collect())
}

pub fn missing<'a>(risks: &'a [Risk], acknowledged: &[String]) -> Vec<&'a Risk> {
    let exact: BTreeSet<_> = acknowledged.iter().map(String::as_str).collect();
    risks
        .iter()
        .filter(|risk| !exact.contains(risk.id.as_str()))
        .collect()
}

/// The single statement uses one source snapshot, walks every membership path
/// (including NOINHERIT/SET ROLE), and includes column grants. Only the actual
/// session/current migration identities are exempt from direct principal risks.
pub async fn collect(
    client: &Client,
    tables: &[String],
    applications: &[String],
) -> Result<Vec<Risk>, String> {
    if tables.is_empty() {
        return Err("fence inventory requires selected tables".into());
    }
    let rows = client
        .query(include_str!("inventory.sql"), &[&tables, &applications])
        .await
        .map_err(|error| format!("source fence inventory failed: {error}"))?;
    let mut found = BTreeSet::new();
    let mut risks = vec![];
    for row in rows {
        let values: Vec<String> = (0..4).map(|i| row.get(i)).collect();
        if values[0] == "scope" {
            found.insert(values[2].clone());
        } else {
            risks.push(values);
        }
    }
    if found.len() != tables.len() {
        return Err("fence inventory selected table missing or duplicated".into());
    }
    from_rows(risks)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_exact_ids_preserve_names_and_unknown_coverage() {
        let rows = vec![
            vec![
                "out-of-scope-writer",
                "indirect:writer",
                "s.t",
                "UPDATE column via NOINHERIT group",
            ],
            vec![
                "table-owner",
                "app",
                "s.t",
                "unrevokable owner inside scope",
            ],
            vec![
                "active-writer-session",
                "indirect:writer",
                "s.t",
                "session observed",
            ],
            vec![
                "unscanned-security-definer",
                "admin",
                "s.func()",
                "dynamic SQL not scanned",
            ],
            vec![
                "unscanned-inbound-writer",
                "unknown",
                "s.t",
                "external writers unknown",
            ],
        ]
        .into_iter()
        .map(|r| r.into_iter().map(str::to_owned).collect())
        .collect();
        let risks = from_rows(rows).unwrap();
        assert_eq!(risks.len(), 5);
        let mut changed = risks
            .iter()
            .map(|r| {
                vec![
                    r.kind.name(),
                    r.principal.clone(),
                    r.object.clone(),
                    "different evidence".into(),
                ]
            })
            .collect::<Rows>();
        changed.reverse();
        assert_eq!(
            risks.iter().map(|r| &r.id).collect::<Vec<_>>(),
            from_rows(changed)
                .unwrap()
                .iter()
                .map(|r| &r.id)
                .collect::<Vec<_>>()
        );
        let all = risks.iter().map(|r| r.id.clone()).collect::<Vec<_>>();
        assert!(missing(&risks, &all).is_empty());
        assert_eq!(missing(&risks, &["indirect:writer".into()]).len(), 5);
        assert_eq!(missing(&risks, &[format!("{} ", all[0])]).len(), 5);
        assert!(
            from_rows(vec![vec![
                "unknown-future-kind".into(),
                "x".into(),
                "y".into(),
                "".into()
            ]])
            .is_err()
        );
        let collisions = from_rows(
            vec![
                vec!["out-of-scope-writer", "a:b", "é", ""],
                vec!["out-of-scope-writer", "a", "b:é", ""],
            ]
            .into_iter()
            .map(|r| r.into_iter().map(str::to_owned).collect())
            .collect(),
        )
        .unwrap();
        assert_eq!(collisions.len(), 2);
        assert_ne!(collisions[0].id, collisions[1].id);
    }
}
