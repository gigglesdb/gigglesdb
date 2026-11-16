use super::{Blocker, Result, TableCatalog, blocked};
use crate::pg::{
    apply::{self, ApplyError},
    chunk::{self, Direction},
};
use crate::{
    coordinator::{Materialized, chunk::ChunkPlan},
    event::{AssembledTxn, Position, Row},
    mapping::{Date, PgType, Timestamp, Value},
};
use std::cmp::Ordering;

/// Done applies only to transactions strictly after the saved high watermark.
/// Pre-H replay remains InWindow, even when the durable journal already has Done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeStatus {
    Done,
    InWindow,
    NotYet,
}
impl RangeStatus {
    pub fn apply_state(self) -> apply::RangeState {
        match self {
            Self::Done => apply::RangeState::Completed,
            Self::InWindow | Self::NotYet => apply::RangeState::Copying,
        }
    }
}
#[derive(Clone, Debug)]
pub struct RangeClassifier {
    catalog: TableCatalog,
    plan: ChunkPlan,
    order_domain: String,
}
impl RangeClassifier {
    pub(super) fn new(
        catalog: &TableCatalog,
        plan: &ChunkPlan,
        order_domain: &str,
    ) -> Result<Self> {
        if order_domain.is_empty()
            || plan.table != catalog.key
            || chunk::plan_chunks(&catalog.key, plan.chunk_size, &plan.samples)
                .ok()
                .as_ref()
                != Some(&plan.ranges)
        {
            return Err(blocked(
                &catalog.selection.name,
                "chunk plan",
                "catalog/plan mismatch or absent position domain",
                "Load the saved plan for this qualified table and its PostgreSQL lineage order domain.",
            ));
        }
        for column in &catalog.index_columns {
            if column.key.collation.is_some()
                || !matches!(
                    column.key.pg_type,
                    PgType::Bool
                        | PgType::Int2
                        | PgType::Int4
                        | PgType::Int8
                        | PgType::Bytea
                        | PgType::Uuid
                        | PgType::Date
                        | PgType::Time
                        | PgType::Timestamp
                        | PgType::Timestamptz
                )
            {
                return Err(Blocker { column: column.key.name.clone(), sql_type: column.key.sql_type.sql().unwrap_or_default(), reason: "PostgreSQL order is not qualified for this type/collation; canonical bytes are not sort keys".into(), next_step: "Qualify server-side range classification with the exact collation/opfamily before enabling this key." });
            }
        }
        let classifier = Self {
            catalog: catalog.clone(),
            plan: plan.clone(),
            order_domain: order_domain.into(),
        };
        for sample in &plan.samples {
            classifier.compare_keys(sample, sample).map_err(|_| {
                blocked(
                    &catalog.selection.name,
                    "chunk sample",
                    "invalid typed boundary",
                    "Reload qualified non-null source samples.",
                )
            })?;
        }
        for pair in plan.samples.windows(2) {
            if classifier.compare_keys(&pair[0], &pair[1]).ok() != Some(Ordering::Less) {
                return Err(blocked(
                    &catalog.selection.name,
                    "chunk samples",
                    "samples do not follow strict PostgreSQL index order",
                    "Use source-side ORDER BY sampling; never sort canonical bytes.",
                ));
            }
        }
        Ok(classifier)
    }
    pub fn compare_keys(
        &self,
        a: &[Value],
        b: &[Value],
    ) -> std::result::Result<Ordering, ApplyError> {
        if a.len() != self.catalog.key.columns.len() || b.len() != a.len() {
            return Err(ApplyError::Invalid("catalog key arity"));
        }
        for ((a, b), column) in a.iter().zip(b).zip(&self.catalog.key.columns) {
            typed(&column.pg_type, a, a)?;
            typed(&column.pg_type, b, b)?;
        }
        for ((a, b), column) in a.iter().zip(b).zip(&self.catalog.key.columns) {
            let cmp = typed(&column.pg_type, a, b)?;
            let cmp = if column.direction == Direction::Desc {
                cmp.reverse()
            } else {
                cmp
            };
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
        }
        Ok(Ordering::Equal)
    }
    pub fn classify(
        &self,
        row: &Row,
        position: &Position,
        state: &Materialized,
    ) -> std::result::Result<RangeStatus, ApplyError> {
        if position.order_domain() != self.order_domain {
            return Err(ApplyError::Invalid("catalog position domain mismatch"));
        }
        let lsn = u64::from_be_bytes(
            position
                .bytes()
                .try_into()
                .map_err(|_| ApplyError::Invalid("PostgreSQL LSN width"))?,
        );
        let mut key = Vec::new();
        for k in &self.catalog.key.columns {
            let c = self
                .catalog
                .columns
                .iter()
                .find(|c| c.name == k.name)
                .expect("qualified key");
            let v = row
                .get(&k.name)
                .ok_or(ApplyError::Invalid("catalog key column absent"))?;
            if v.type_id != c.source_type_id {
                return Err(ApplyError::Invalid("catalog key type identity"));
            }
            let v = Value::from_canonical_bytes(
                v.bytes
                    .as_deref()
                    .ok_or(ApplyError::Invalid("NULL catalog key"))?,
            )?;
            typed(&c.pg_type, &v, &v)?;
            key.push(v);
        }
        let reference = state
            .chunk_plans
            .get(&self.catalog.key.table)
            .ok_or(ApplyError::Invalid("catalog table is not durably planned"))?;
        if reference.plan != self.plan.ranges[0].id.plan
            || reference.chunks != self.plan.ranges.len()
        {
            return Err(ApplyError::Invalid("catalog durable plan mismatch"));
        }
        let mut found = None;
        for range in &self.plan.ranges {
            let lower = range
                .lo
                .as_ref()
                .map(|b| self.compare_keys(&key, b))
                .transpose()?
                .is_none_or(|o| o != Ordering::Less);
            let upper = range
                .hi
                .as_ref()
                .map(|b| self.compare_keys(&key, b))
                .transpose()?
                .is_none_or(|o| o == Ordering::Less);
            if lower && upper {
                if found.is_some() {
                    return Err(ApplyError::Invalid("overlapping catalog ranges"));
                }
                found = Some(range);
            }
        }
        let range = found.ok_or(ApplyError::Invalid("catalog key outside saved ranges"))?;
        let id = serde_json::to_string(&range.id)
            .map_err(|_| ApplyError::Invalid("chunk identity encoding"))?;
        let Some(attempt) = state.chunk_attempts.get(&id) else {
            return Ok(RangeStatus::NotYet);
        };
        if &attempt.chunk != range {
            return Err(ApplyError::Invalid("catalog attempt bounds mismatch"));
        }
        Ok(
            if attempt
                .done
                .as_ref()
                .map(|done| {
                    if done.high.order_domain() != self.order_domain {
                        return Err(ApplyError::Invalid("catalog Done position domain mismatch"));
                    }
                    let high = u64::from_be_bytes(
                        done.high
                            .bytes()
                            .try_into()
                            .map_err(|_| ApplyError::Invalid("PostgreSQL LSN width"))?,
                    );
                    Ok(lsn > high)
                })
                .transpose()?
                .unwrap_or(false)
            {
                RangeStatus::Done
            } else {
                RangeStatus::InWindow
            },
        )
    }
    /// Compose with PgApplier.plan by supplying its transaction/mseq and a
    /// PlanContext. Every old/new key lookup uses this transaction's position.
    pub fn plan_transaction(
        &self,
        transaction: &AssembledTxn,
        context: &apply::PlanContext,
        state: &Materialized,
    ) -> std::result::Result<apply::TransactionPlan, ApplyError> {
        apply::plan_transaction(
            transaction,
            std::slice::from_ref(&self.catalog.destination),
            context,
            |_, row| {
                self.classify(row, &transaction.commit_position, state)
                    .map(RangeStatus::apply_state)
            },
        )
    }
}
fn typed(ty: &PgType, a: &Value, b: &Value) -> std::result::Result<Ordering, ApplyError> {
    Ok(match (ty, a, b) {
        (PgType::Bool, Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (PgType::Int2, Value::Int2(a), Value::Int2(b)) => a.cmp(b),
        (PgType::Int4, Value::Int4(a), Value::Int4(b)) => a.cmp(b),
        (PgType::Int8, Value::Int8(a), Value::Int8(b)) => a.cmp(b),
        (PgType::Uuid, Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
        (PgType::Bytea, Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
        (PgType::Time, Value::Time(a), Value::Time(b)) => a.cmp(b),
        (PgType::Date, Value::Date(a), Value::Date(b)) => date(*a).cmp(&date(*b)),
        (PgType::Timestamp, Value::Timestamp(a), Value::Timestamp(b))
        | (PgType::Timestamptz, Value::Timestamptz(a), Value::Timestamptz(b)) => {
            timestamp(*a).cmp(&timestamp(*b))
        }
        _ => return Err(ApplyError::Invalid("unqualified catalog ordering value")),
    })
}
fn date(value: Date) -> (u8, i32) {
    match value {
        Date::NegativeInfinity => (0, 0),
        Date::Finite(v) => (1, v),
        Date::PositiveInfinity => (2, 0),
    }
}
fn timestamp(value: Timestamp) -> (u8, i64) {
    match value {
        Timestamp::NegativeInfinity => (0, 0),
        Timestamp::Finite(v) => (1, v),
        Timestamp::PositiveInfinity => (2, 0),
    }
}
