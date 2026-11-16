//! Qualified PG reverse capture for MySQL: retain PG values and type identities.
//! Catalog qualification must establish nullability/generated expressions first;
//! pgoutput exposes only names, type OIDs, typmods and replica-identity flags.
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    mapping::{
        Classification, Mapping, MappingError, PgType, Value,
        mysql::{self, MysqlPgColumn},
    },
    pg::{
        pgoutput::{Relation, RelationColumn},
        source::{self, ColumnMapping, TypeResolver},
    },
};

/// Keys are quoted PG event identities, exactly as used by MysqlApplier's
/// descriptor aliases. Values are the saved M7 plans, not plans inferred from PG.
pub struct Resolver {
    tables: BTreeMap<String, BTreeMap<String, MysqlPgColumn>>,
}
impl Resolver {
    pub fn new(tables: BTreeMap<String, Vec<MysqlPgColumn>>) -> source::Result<Self> {
        let mut saved = BTreeMap::new();
        for (alias, plans) in tables {
            let mut columns = BTreeMap::new();
            for plan in plans {
                if columns.insert(plan.source.name.clone(), plan).is_some() {
                    return Err(source::Error::RecoveryRequired(
                        "duplicate MySQL reverse column",
                    ));
                }
            }
            if columns.is_empty() {
                return Err(source::Error::RecoveryRequired("empty MySQL reverse plan"));
            }
            saved.insert(alias, columns);
        }
        Ok(Self { tables: saved })
    }
    fn table(&self, relation: &Relation<'_>) -> source::Result<&BTreeMap<String, MysqlPgColumn>> {
        let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        self.tables
            .get(&format!(
                "{}.{}",
                quote(relation.namespace),
                quote(relation.name)
            ))
            .ok_or(source::Error::RecoveryRequired(
                "unknown MySQL reverse relation alias",
            ))
    }
}
impl TypeResolver for Resolver {
    fn included_columns(
        &mut self,
        relation: &Relation<'_>,
    ) -> source::Result<Option<BTreeSet<String>>> {
        let columns = self.table(relation)?;
        // Generated MySQL columns are recomputed at apply, and may be absent
        // from pgoutput. All writable saved columns must remain present.
        Ok(Some(
            columns
                .values()
                .filter(|p| !p.generated)
                .map(|p| p.source.name.clone())
                .collect(),
        ))
    }
    fn resolve(
        &mut self,
        relation: &Relation<'_>,
        column: &RelationColumn<'_>,
    ) -> source::Result<ColumnMapping> {
        let plan =
            self.table(relation)?
                .get(column.name)
                .ok_or(source::Error::RecoveryRequired(
                    "unknown MySQL reverse column",
                ))?;
        if oid(&plan.pg_type) != Some(column.type_oid) || plan.type_modifier != column.type_modifier
        {
            return Err(source::Error::RecoveryRequired(
                "MySQL reverse PG type/typmod differs from saved plan",
            ));
        }
        Ok(ColumnMapping {
            pg_type: plan.pg_type.clone(),
            mapping: Box::new(InverseDomain(plan.clone())),
        })
    }
}
fn oid(ty: &PgType) -> Option<u32> {
    Some(match ty {
        PgType::Int2 => 21,
        PgType::Int4 => 23,
        PgType::Int8 => 20,
        PgType::Numeric => 1700,
        PgType::Float4 => 700,
        PgType::Float8 => 701,
        PgType::Text { .. } => 25,
        PgType::Varchar { .. } => 1043,
        PgType::Char { .. } => 1042,
        PgType::Bytea => 17,
        PgType::Date => 1082,
        PgType::Timestamp => 1114,
        PgType::Timestamptz => 1184,
        PgType::Interval => 1186,
        PgType::Jsonb => 3802,
        _ => return None,
    })
}
struct InverseDomain(MysqlPgColumn);
impl Mapping for InverseDomain {
    fn forward(&self, value: &Value) -> Result<Value, MappingError> {
        self.domain_check(value)?;
        Ok(value.clone())
    }
    fn inverse(&self, value: &Value) -> Result<Value, MappingError> {
        self.forward(value)
    }
    fn domain_check(&self, value: &Value) -> Result<(), MappingError> {
        mysql::inverse(&self.0, value).map(|_| ())
    }
    fn is_reversible(&self) -> bool {
        true
    }
    fn classification(&self) -> Classification {
        self.0.classification
    }
}

#[cfg(test)]
#[path = "reverse_resolver_tests.rs"]
mod tests;
