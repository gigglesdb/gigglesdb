//! MySQL owns range membership; mapped keys are used only as cache identities.
use super::{
    chunk::{MysqlChunkError, MysqlChunkQuery},
    chunk_key::MysqlTableKey,
    client::MysqlConfig,
    position::MysqlOrder,
};
use crate::{
    coordinator::{
        Materialized,
        chunk::{ChunkPlan, SourceTableKey},
    },
    engine::{PgPlanFuture, PgPlannerError, PgTransactionPlanner},
    event::{AssembledTxn, EventKind, Position, Row},
    mapping::Value,
    pg::{
        apply::{self, ApplyError},
        catalog::RangeStatus,
        chunk::{ChunkRange, Key, KeyIdentity},
    },
};
use mysql_async::{Conn, Params, prelude::Queryable};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

type Result<T> = std::result::Result<T, PgPlannerError>;
fn invalid(reason: &'static str) -> PgPlannerError {
    ApplyError::Invalid(reason).into()
}
fn chunk_error(error: MysqlChunkError) -> PgPlannerError {
    match error {
        MysqlChunkError::Driver(_) | MysqlChunkError::Mysql(_) => PgPlannerError::Infrastructure,
        MysqlChunkError::Mapping(error) => ApplyError::Mapping(error).into(),
        _ => invalid("invalid MySQL classifier key/query"),
    }
}

#[derive(Clone, Debug)]
pub struct MysqlPlannerTable {
    key: MysqlTableKey,
    plan: ChunkPlan,
    destination: apply::Table,
}
impl MysqlPlannerTable {
    pub fn new(plan: ChunkPlan, destination: apply::Table) -> Result<Self> {
        let SourceTableKey::Mysql(key) = &plan.table else {
            return Err(invalid("MySQL classifier requires a MySQL plan"));
        };
        if ChunkPlan::new(key.clone(), plan.chunk_size, plan.samples.clone())
            .ok()
            .as_ref()
            != Some(&plan)
            || key.event_table != destination.event_table
            || key.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
                != destination.key.iter().collect::<Vec<_>>()
            || key.columns.iter().any(|c| {
                !destination
                    .columns
                    .iter()
                    .any(|d| d.name == c.name && d.type_id == c.type_id)
            })
        {
            return Err(invalid("MySQL classifier destination/plan mismatch"));
        }
        // Exercise inverse bindings and all predicates without issuing a query.
        if let Some(sample) = plan.samples.first() {
            key.membership_query(&plan.ranges, std::slice::from_ref(sample))
                .map_err(chunk_error)?;
        }
        Ok(Self {
            key: key.clone(),
            plan,
            destination,
        })
    }
    fn key(&self, row: &Row) -> Result<(Vec<u8>, Key)> {
        let (identity, _) = self
            .key
            .key(row)
            .map_err(|_| invalid("invalid MySQL classifier key identity"))?;
        let values = self
            .key
            .columns
            .iter()
            .map(|c| {
                Value::from_canonical_bytes(row[&c.name].bytes.as_deref().expect("validated key"))
                    .map_err(ApplyError::Mapping)
                    .map_err(Into::into)
            })
            .collect::<Result<Key>>()?;
        Ok((identity, values))
    }
    fn validate(
        &self,
        position: &Position,
        state: &Materialized,
        order: &MysqlOrder,
    ) -> Result<()> {
        position
            .compare(position, order)
            .map_err(ApplyError::Position)?;
        let reference = state
            .chunk_plans
            .get(&self.key.event_table)
            .ok_or_else(|| invalid("MySQL table is not durably planned"))?;
        if reference.plan != self.plan.ranges[0].id.plan
            || reference.chunks != self.plan.ranges.len()
        {
            return Err(invalid("MySQL durable plan mismatch"));
        }
        Ok(())
    }
    fn status(
        &self,
        range: &ChunkRange,
        position: &Position,
        state: &Materialized,
        order: &MysqlOrder,
    ) -> Result<RangeStatus> {
        let id = serde_json::to_string(&range.id)
            .map_err(|_| invalid("MySQL chunk identity encoding"))?;
        let Some(attempt) = state.chunk_attempts.get(&id) else {
            return Ok(RangeStatus::NotYet);
        };
        if &attempt.chunk != range {
            return Err(invalid("MySQL attempt bounds mismatch"));
        }
        Ok(match &attempt.done {
            Some(done)
                if position
                    .compare(&done.high, order)
                    .map_err(ApplyError::Position)?
                    == Ordering::Greater =>
            {
                RangeStatus::Done
            }
            _ => RangeStatus::InWindow,
        })
    }
}

/// Owns a private source connection. Dropping a plan future poisons this instance;
/// reconstruct planner/applier from durable state before another preparation.
pub struct MysqlPlanner {
    source: Conn,
    tables: Vec<MysqlPlannerTable>,
    order: Arc<MysqlOrder>,
    context: apply::PlanContext,
    max_keys_per_query: usize,
    preparation_started: bool,
}
impl MysqlPlanner {
    pub async fn connect(
        config: &MysqlConfig,
        tables: Vec<MysqlPlannerTable>,
        order: MysqlOrder,
        stream: String,
        origin: String,
        max_keys_per_query: usize,
    ) -> Result<Self> {
        if tables.is_empty() || max_keys_per_query == 0 || stream.is_empty() || origin.is_empty() {
            return Err(invalid("empty MySQL planner/table/batch/stream/origin"));
        }
        let mut names = BTreeSet::new();
        for table in &tables {
            if !names.insert(&table.key.event_table) {
                return Err(invalid("duplicate MySQL planner table"));
            }
            let width = table.key.columns.len();
            if max_keys_per_query
                .checked_mul(width + 1)
                .and_then(|n| n.checked_add(2 * width * table.plan.samples.len()))
                .is_none_or(|n| n > 65535)
            {
                return Err(invalid("MySQL classifier parameter limit"));
            }
        }
        let order = Arc::new(order);
        let context = apply::PlanContext {
            positions: apply::SourcePositions::new(order.clone(), apply::OriginPolicy::Opaque),
            stream,
            origin,
            mseq: 0,
            synthetic_commit_time: None,
        };
        let mut source = config
            .connect()
            .await
            .map_err(|_| PgPlannerError::Infrastructure)?
            .conn;
        for sql in [
            "SET autocommit=1",
            "SET SESSION time_zone='+00:00'",
            "SET SESSION TRANSACTION READ ONLY",
        ] {
            source
                .query_drop(sql)
                .await
                .map_err(|_| PgPlannerError::Infrastructure)?;
        }
        Ok(Self {
            source,
            tables,
            order,
            context,
            max_keys_per_query,
            preparation_started: false,
        })
    }
    /// At most one membership query per table per bounded key batch, across all saved ranges.
    pub async fn classify_keys(
        &mut self,
        name: &str,
        rows: &[Row],
        position: &Position,
        state: &Materialized,
    ) -> Result<Vec<RangeStatus>> {
        if self.preparation_started {
            return Err(invalid("cancelled MySQL planner requires reconstruction"));
        }
        self.classify(name, rows, position, state).await
    }
    async fn classify(
        &mut self,
        name: &str,
        rows: &[Row],
        position: &Position,
        state: &Materialized,
    ) -> Result<Vec<RangeStatus>> {
        let table = self
            .tables
            .iter()
            .find(|t| t.key.event_table == name)
            .ok_or_else(|| invalid("unknown MySQL classifier table"))?;
        table.validate(position, state, &self.order)?;
        let keys = rows
            .iter()
            .map(|r| table.key(r).map(|(_, k)| k))
            .collect::<Result<Vec<_>>>()?;
        let mut statuses = Vec::with_capacity(keys.len());
        for batch in keys.chunks(self.max_keys_per_query) {
            let MysqlChunkQuery { sql, parameters } = table
                .key
                .membership_query(&table.plan.ranges, batch)
                .map_err(chunk_error)?;
            let results: Vec<mysql_async::Row> = self
                .source
                .exec(sql, Params::Positional(parameters))
                .await
                .map_err(|_| PgPlannerError::Infrastructure)?;
            let ranges = decode_membership(results, batch.len(), table.plan.ranges.len())?;
            for range in ranges {
                statuses.push(table.status(
                    &table.plan.ranges[range],
                    position,
                    state,
                    &self.order,
                )?);
            }
        }
        Ok(statuses)
    }
    async fn prepare(
        &mut self,
        transaction: &AssembledTxn,
        mseq: u64,
        state: &Materialized,
    ) -> Result<apply::TransactionPlan> {
        let tables = self
            .tables
            .iter()
            .map(|t| t.destination.clone())
            .collect::<Vec<_>>();
        let mut context = self.context.clone();
        context.mseq = mseq;
        let mut requested: BTreeMap<String, Vec<Row>> = BTreeMap::new();
        // Validate the entire transaction before source I/O. ChunkRows bypass lookup.
        apply::plan_transaction(transaction, &tables, &context, |table, row| {
            requested
                .entry(table.event_table.clone())
                .or_default()
                .push(row.clone());
            Ok(apply::RangeState::Copying)
        })?;
        for event in &transaction.events {
            if event.kind == EventKind::Update {
                let mut new = event.key.clone();
                if let Some(after) = &event.after {
                    new.extend(after.clone());
                }
                requested
                    .entry(
                        event
                            .table
                            .clone()
                            .ok_or_else(|| invalid("update without table"))?,
                    )
                    .or_default()
                    .push(new);
            }
        }
        let mut classified = BTreeMap::new();
        for (name, rows) in requested {
            let table = self
                .tables
                .iter()
                .find(|t| t.key.event_table == name)
                .expect("validated table");
            let mut distinct = BTreeMap::new();
            for row in rows {
                distinct.insert(table.key(&row)?.0, row);
            }
            let (identities, rows): (Vec<_>, Vec<_>) = distinct.into_iter().unzip();
            let statuses = self
                .classify(&name, &rows, &transaction.commit_position, state)
                .await?;
            for (identity, status) in identities.into_iter().zip(statuses) {
                classified.insert((name.clone(), identity), status);
            }
        }
        apply::plan_transaction(transaction, &tables, &context, |table, row| {
            let mysql = self
                .tables
                .iter()
                .find(|t| t.key.event_table == table.event_table)
                .expect("validated table");
            let identity = mysql
                .key
                .key(row)
                .map_err(|_| ApplyError::Invalid("invalid prepared MySQL key"))?
                .0;
            classified
                .get(&(table.event_table.clone(), identity))
                .copied()
                .map(RangeStatus::apply_state)
                .ok_or(ApplyError::Invalid("missing prepared MySQL classification"))
        })
        .map_err(Into::into)
    }
    pub async fn disconnect(self) -> Result<()> {
        self.source
            .disconnect()
            .await
            .map_err(|_| PgPlannerError::Infrastructure)
    }
}
impl PgTransactionPlanner for MysqlPlanner {
    fn plan<'a>(
        &'a mut self,
        transaction: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> PgPlanFuture<'a> {
        if self.preparation_started {
            return Box::pin(std::future::ready(Err(invalid(
                "cancelled MySQL planner requires reconstruction",
            ))));
        }
        self.preparation_started = true;
        Box::pin(async move {
            let result = self.prepare(transaction, mseq, state).await;
            self.preparation_started = false;
            result
        })
    }
}
fn decode_membership(
    rows: Vec<mysql_async::Row>,
    keys: usize,
    ranges: usize,
) -> Result<Vec<usize>> {
    let mut found = vec![None; keys];
    for row in rows {
        if row.len() != ranges + 1 {
            return Err(invalid("MySQL membership width"));
        }
        let ordinal = row
            .get_opt::<u64, _>(0)
            .and_then(std::result::Result::ok)
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n < keys)
            .ok_or_else(|| invalid("MySQL membership ordinal"))?;
        if found[ordinal].is_some() {
            return Err(invalid("duplicate MySQL membership ordinal"));
        }
        let mut matched = None;
        for i in 0..ranges {
            match row
                .get_opt::<u8, _>(i + 1)
                .and_then(std::result::Result::ok)
            {
                Some(0) => {}
                Some(1) if matched.is_none() => matched = Some(i),
                Some(1) => return Err(invalid("overlapping MySQL ranges")),
                _ => return Err(invalid("invalid MySQL membership boolean")),
            }
        }
        found[ordinal] = Some(matched.ok_or_else(|| invalid("MySQL key outside saved ranges"))?);
    }
    found
        .into_iter()
        .map(|v| v.ok_or_else(|| invalid("missing MySQL membership key")))
        .collect()
}

#[cfg(test)]
mod tests;
