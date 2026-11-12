use super::*;
use crate::capability::{CapabilityItem, Criticality, Evidence, ItemStatus};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockerKind {
    InvalidIndex,
    NotValidConstraint,
    ExternalSideEffect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocker {
    pub kind: BlockerKind,
    pub schema: String,
    pub table: String,
    pub name: String,
    pub acknowledged: bool,
}

impl Blocker {
    /// Stable, unambiguous key for a specific ENABLE ALWAYS trigger.
    pub fn acknowledgement_key(&self) -> String {
        format!(
            "{}.{}.{}",
            ident(&self.schema),
            ident(&self.table),
            ident(&self.name)
        )
    }

    pub fn blocks(&self) -> bool {
        self.kind != BlockerKind::ExternalSideEffect || !self.acknowledged
    }

    pub fn capability_item(&self) -> CapabilityItem {
        let kind = match self.kind {
            BlockerKind::InvalidIndex => "invalid-index",
            BlockerKind::NotValidConstraint => "not-valid-constraint",
            BlockerKind::ExternalSideEffect => "external-side-effect",
        };
        CapabilityItem {
            id: format!("destination.{kind}:{}", self.acknowledgement_key()), object: None,
            status: if self.blocks() { ItemStatus::RequiresExternalChange } else { ItemStatus::SupportedWithTransformation },
            sampled: false, criticality: Criticality::Critical,
            remediation: match self.kind {
                BlockerKind::InvalidIndex => "Rebuild or remove the invalid destination index before activation.",
                BlockerKind::NotValidConstraint => "Validate the destination constraint before activation.",
                BlockerKind::ExternalSideEffect => "Review and explicitly acknowledge this ENABLE ALWAYS trigger's external side effects.",
            }.into(),
            evidence: vec![Evidence { source: "destination-pg-catalog".into(), description: format!("{kind} {}; acknowledged={}", self.acknowledgement_key(), self.acknowledged) }],
        }
    }
}

/// Complete inspection of the selected relations, including internal triggers.
/// Invalid indexes/constraints cannot be bypassed by trigger acknowledgements.
pub fn inspect_validity(
    connection: &mut impl Connection,
    tables: &[Table],
    acknowledged_triggers: &BTreeSet<String>,
    timeout: Duration,
) -> Result<Vec<Blocker>, Error> {
    if tables.is_empty() {
        return Err(Error::Invalid("validity scope is empty".into()));
    }
    let selected = selected(tables);
    let sql = format!(
        "SELECT 'index',n.nspname,c.relname,i.relname FROM pg_index x JOIN pg_class c ON c.oid=x.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_class i ON i.oid=x.indexrelid WHERE x.indrelid IN ({selected}) AND NOT x.indisvalid \
         UNION ALL SELECT 'constraint',n.nspname,c.relname,x.conname FROM pg_constraint x JOIN pg_class c ON c.oid=x.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE x.conrelid IN ({selected}) AND NOT x.convalidated \
         UNION ALL SELECT 'trigger',n.nspname,c.relname,x.tgname FROM pg_trigger x JOIN pg_class c ON c.oid=x.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE x.tgrelid IN ({selected}) AND x.tgenabled='A' ORDER BY 1,2,3,4"
    );
    query(connection, &sql, timeout)?
        .iter()
        .map(|row| {
            let mut item = Blocker {
                kind: match cell(row, 0)? {
                    "index" => BlockerKind::InvalidIndex,
                    "constraint" => BlockerKind::NotValidConstraint,
                    "trigger" => BlockerKind::ExternalSideEffect,
                    _ => return Err(Error::Invalid("unknown blocker kind".into())),
                },
                schema: cell(row, 1)?.into(),
                table: cell(row, 2)?.into(),
                name: cell(row, 3)?.into(),
                acknowledged: false,
            };
            item.acknowledged = item.kind == BlockerKind::ExternalSideEffect
                && acknowledged_triggers.contains(&item.acknowledgement_key());
            Ok(item)
        })
        .collect()
}
