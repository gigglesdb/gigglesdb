//! Source PostgreSQL owns ordering; canonical values are only lookup identities.
use super::{RangeStatus, TableCatalog};
use crate::{
    coordinator::{Materialized, chunk::ChunkPlan},
    engine::{PgPlanFuture, PgPlannerError, PgTransactionPlanner},
    event::{AssembledTxn, EventKind, Position, Row},
    mapping::{PgType, Value},
    pg::{
        apply::{self, ApplyError},
        chunk::{self, ChunkQuery, ChunkRange, Key, QualifiedName},
    },
};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::Client;

type Result<T> = std::result::Result<T, PgPlannerError>;
fn invalid(reason: &'static str) -> PgPlannerError {
    ApplyError::Invalid(reason).into()
}
fn chunk_error(error: chunk::ChunkError) -> PgPlannerError {
    match error {
        chunk::ChunkError::Postgres(_) => PgPlannerError::Infrastructure,
        _ => invalid("invalid server classifier key/query"),
    }
}

/// Saved qualification inputs for one table. No server order is inferred here.
#[derive(Clone, Debug)]
pub struct ServerTable {
    catalog: TableCatalog,
    plan: ChunkPlan,
    order_domain: String,
}
impl ServerTable {
    pub fn new(catalog: &TableCatalog, plan: &ChunkPlan, order_domain: &str) -> Result<Self> {
        if order_domain.is_empty()
            || plan.table != catalog.key
            || chunk::plan_chunks(&catalog.key, plan.chunk_size, &plan.samples)
                .ok()
                .as_ref()
                != Some(&plan.ranges)
        {
            return Err(invalid("server classifier catalog/plan/domain mismatch"));
        }
        for column in &catalog.key.columns {
            match &column.pg_type {
                PgType::Array(_) => return Err(invalid("array ordering is not qualified")),
                PgType::Json => return Err(invalid("json has no default btree ordering")),
                PgType::Jsonb => return Err(invalid("jsonb ordering is not qualified")),
                PgType::Float4 | PgType::Float8 => {
                    return Err(invalid("float NaN payload text capture is not qualified"));
                }
                PgType::Text { .. } | PgType::Varchar { .. } | PgType::Char { .. }
                    if column.collation.is_none() =>
                {
                    return Err(invalid("collatable key lacks exact source collation"));
                }
                PgType::Enum { .. } | PgType::Domain { .. } | PgType::Other { .. } => {
                    return Err(invalid(
                        "key type lacks built-in server ordering qualification",
                    ));
                }
                _ => {}
            }
        }
        for sample in &plan.samples {
            catalog.key.validate_key(sample).map_err(chunk_error)?;
        }
        Ok(Self {
            catalog: catalog.clone(),
            plan: plan.clone(),
            order_domain: order_domain.into(),
        })
    }
    fn key(&self, row: &Row) -> Result<Key> {
        let mut key = Vec::new();
        for k in &self.catalog.key.columns {
            let c = self
                .catalog
                .columns
                .iter()
                .find(|c| c.name == k.name)
                .ok_or_else(|| invalid("unqualified server classifier column"))?;
            let value = row
                .get(&k.name)
                .ok_or_else(|| invalid("catalog key column absent"))?;
            if value.type_id != c.source_type_id {
                return Err(invalid("catalog key type identity"));
            }
            key.push(
                Value::from_canonical_bytes(
                    value
                        .bytes
                        .as_deref()
                        .ok_or_else(|| invalid("NULL catalog key"))?,
                )
                .map_err(|_| invalid("invalid canonical catalog key"))?,
            );
        }
        self.catalog.key.validate_key(&key).map_err(chunk_error)?;
        Ok(key)
    }
    fn lsn(&self, position: &Position, state: &Materialized) -> Result<u64> {
        if position.order_domain() != self.order_domain {
            return Err(invalid("catalog position domain mismatch"));
        }
        let reference = state
            .chunk_plans
            .get(&self.catalog.key.table)
            .ok_or_else(|| invalid("catalog table is not durably planned"))?;
        if reference.plan != self.plan.ranges[0].id.plan
            || reference.chunks != self.plan.ranges.len()
        {
            return Err(invalid("catalog durable plan mismatch"));
        }
        Ok(u64::from_be_bytes(
            position
                .bytes()
                .try_into()
                .map_err(|_| invalid("PostgreSQL LSN width"))?,
        ))
    }
    fn status(&self, range: &ChunkRange, lsn: u64, state: &Materialized) -> Result<RangeStatus> {
        let id =
            serde_json::to_string(&range.id).map_err(|_| invalid("chunk identity encoding"))?;
        let Some(attempt) = state.chunk_attempts.get(&id) else {
            return Ok(RangeStatus::NotYet);
        };
        if &attempt.chunk != range {
            return Err(invalid("catalog attempt bounds mismatch"));
        }
        Ok(
            if attempt
                .done
                .as_ref()
                .is_some_and(|done| lsn > done.high_lsn)
            {
                RangeStatus::Done
            } else {
                RangeStatus::InWindow
            },
        )
    }
}

/// The exact server metadata retained for review. A NULL version is accepted
/// only for pg_catalog C/POSIX libc byte ordering, which has no locale version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollationEvidence {
    pub provider: String,
    pub deterministic: bool,
    pub encoding: i32,
    pub locale: Option<String>,
    pub lc_collate: Option<String>,
    pub lc_ctype: Option<String>,
    pub rules: Option<String>,
    pub recorded_version: Option<String>,
    pub actual_version: Option<String>,
    pub fixed_byte_order: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollationQualification {
    pub name: QualifiedName,
    pub source: CollationEvidence,
    pub destination: CollationEvidence,
}
async fn collation(client: &mut Client, name: &QualifiedName) -> Result<CollationEvidence> {
    let tx = client.build_transaction().read_only(true).start().await?;
    let rows = tx.query("SELECT c.collprovider::text,c.collisdeterministic,c.collencoding, COALESCE(to_jsonb(c)->>'colllocale',to_jsonb(c)->>'colliculocale'),to_jsonb(c)->>'collcollate',to_jsonb(c)->>'collctype',to_jsonb(c)->>'collicurules',c.collversion,pg_catalog.pg_collation_actual_version(c.oid) FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE n.nspname=$1 AND c.collname=$2 AND c.collencoding IN (-1,pg_catalog.pg_char_to_encoding(pg_catalog.current_setting('server_encoding')))", &[&name.schema,&name.name]).await?;
    if rows.len() != 1 {
        return Err(invalid("missing/ambiguous exact collation evidence"));
    }
    let r = &rows[0];
    let provider: String = r.get(0);
    let lc_collate: Option<String> = r.get(4);
    let lc_ctype: Option<String> = r.get(5);
    let fixed_byte_order = name.schema == "pg_catalog"
        && matches!(name.name.as_str(), "C" | "POSIX")
        && provider == "c"
        && matches!(lc_collate.as_deref(), Some("C" | "POSIX"))
        && matches!(lc_ctype.as_deref(), Some("C" | "POSIX"));
    let evidence = CollationEvidence {
        provider,
        deterministic: r.get(1),
        encoding: r.get(2),
        locale: r.get(3),
        lc_collate,
        lc_ctype,
        rules: r.get(6),
        recorded_version: r.get(7),
        actual_version: r.get(8),
        fixed_byte_order,
    };
    qualify_version(&evidence)?;
    tx.commit().await?;
    Ok(evidence)
}
fn qualify_version(e: &CollationEvidence) -> Result<()> {
    if e.fixed_byte_order && e.recorded_version.is_none() && e.actual_version.is_none() {
        return Ok(());
    }
    if e.recorded_version.as_ref().is_none_or(|v| v.is_empty())
        || e.actual_version.as_ref().is_none_or(|v| v.is_empty())
        || e.recorded_version != e.actual_version
    {
        return Err(invalid("unknown/stale collation version"));
    }
    Ok(())
}

/// Owns a dedicated source connection. Its driver is the caller's ordinary
/// connection driver; no classification task or I/O is detached. Dropping plan
/// preparation requires reconstructing this planner AND PgApplier from durable
/// state. Every invocation's results are local and discarded on cancellation.
pub struct ServerPlanner {
    source: Client,
    tables: Vec<ServerTable>,
    context: apply::PlanContext,
    max_keys_per_query: usize,
    collations: Vec<CollationQualification>,
    preparation_started: bool,
}
impl ServerPlanner {
    pub async fn new(
        source: Client,
        destination: &mut Client,
        tables: Vec<ServerTable>,
        context: apply::PlanContext,
        max_keys_per_query: usize,
    ) -> Result<Self> {
        if tables.is_empty() || max_keys_per_query == 0 {
            return Err(invalid("empty server planner/batch limit"));
        }
        let mut source = source;
        setup_source(&mut source).await?;
        verify_utf8(destination).await?;
        let mut identities = BTreeSet::new();
        let mut names = BTreeMap::new();
        for table in &tables {
            if !identities.insert(table.catalog.key.table.clone()) {
                return Err(invalid("duplicate server planner table"));
            }
            let width = table.catalog.key.columns.len();
            if max_keys_per_query
                .checked_mul(width + 1)
                .and_then(|n| n.checked_add(2 * width))
                .is_none_or(|n| n > 65535)
            {
                return Err(invalid("server classifier parameter limit"));
            }
            for c in &table.catalog.key.columns {
                if let Some(name) = &c.collation {
                    names.insert((name.schema.clone(), name.name.clone()), name.clone());
                }
            }
        }
        let mut planner = Self {
            source,
            tables,
            context,
            max_keys_per_query,
            collations: vec![],
            preparation_started: false,
        };
        for name in names.into_values() {
            let source = collation(&mut planner.source, &name).await?;
            let destination = collation(destination, &name).await?;
            if source != destination {
                return Err(invalid(
                    "source/destination collation identity/version mismatch",
                ));
            }
            planner.collations.push(CollationQualification {
                name,
                source,
                destination,
            });
        }
        // A strict upper chunk bound tests a < b using the very same predicate
        // builder as copy reads, including mixed directions and collations.
        for table in &planner.tables {
            for sample in &table.plan.samples {
                let mut range = table.plan.ranges[0].clone();
                range.lo = Some(sample.clone());
                range.hi = None;
                let query =
                    membership_query(&table.catalog.key, &range, std::slice::from_ref(sample))?;
                if query
                    .execute(&mut planner.source)
                    .await
                    .map_err(chunk_error)?
                    .len()
                    != 1
                {
                    return Err(invalid("invalid server sample self-order"));
                }
            }
            for pair in table.plan.samples.windows(2) {
                let mut range = table.plan.ranges[0].clone();
                range.lo = None;
                range.hi = Some(pair[1].clone());
                let query = membership_query(&table.catalog.key, &range, &pair[..1])?;
                if query
                    .execute(&mut planner.source)
                    .await
                    .map_err(chunk_error)?
                    .len()
                    != 1
                {
                    return Err(invalid(
                        "samples do not follow strict PostgreSQL index order",
                    ));
                }
            }
        }
        Ok(planner)
    }
    pub fn collation_evidence(&self) -> &[CollationQualification] {
        &self.collations
    }
    /// Batch classification is also exposed for qualification against real
    /// ORDER BY/chunk reads. Each query has at most the configured key count.
    pub async fn classify_keys(
        &mut self,
        table: &str,
        rows: &[Row],
        position: &Position,
        state: &Materialized,
    ) -> Result<Vec<RangeStatus>> {
        let table = self
            .tables
            .iter()
            .find(|t| t.catalog.key.table == table)
            .ok_or_else(|| invalid("unknown server classifier table"))?;
        let lsn = table.lsn(position, state)?;
        let keys = rows
            .iter()
            .map(|r| table.key(r))
            .collect::<Result<Vec<_>>>()?;
        let mut statuses = Vec::with_capacity(keys.len());
        for keys in keys.chunks(self.max_keys_per_query) {
            let mut found = vec![None; keys.len()];
            for range in &table.plan.ranges {
                let query = membership_query(&table.catalog.key, range, keys)?;
                for row in query.execute(&mut self.source).await.map_err(chunk_error)? {
                    let ordinal: i64 = row.try_get(0)?;
                    let ordinal = usize::try_from(ordinal)
                        .ok()
                        .filter(|i| *i < found.len())
                        .ok_or_else(|| invalid("server classifier ordinal"))?;
                    if found[ordinal].is_some() {
                        return Err(invalid("overlapping catalog ranges"));
                    }
                    found[ordinal] = Some(table.status(range, lsn, state)?);
                }
            }
            statuses.extend(
                found
                    .into_iter()
                    .map(|s| s.ok_or_else(|| invalid("catalog key outside saved ranges")))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(statuses)
    }
    async fn prepare(
        &mut self,
        transaction: &AssembledTxn,
        mseq: u64,
        state: &Materialized,
    ) -> Result<apply::TransactionPlan> {
        let tables: Vec<_> = self
            .tables
            .iter()
            .map(|t| t.catalog.destination.clone())
            .collect();
        let mut context = self.context.clone();
        context.mseq = mseq;
        // Validate the entire event envelope before any source query; collect
        // the exact old keys requested by the existing apply planner.
        let mut requested: BTreeMap<String, Vec<Row>> = BTreeMap::new();
        apply::plan_transaction(transaction, &tables, &context, |table, row| {
            requested
                .entry(table.event_table.clone())
                .or_default()
                .push(row.clone());
            Ok(apply::RangeState::Copying)
        })?;
        for event in &transaction.events {
            if event.kind == EventKind::Update {
                let name = event
                    .table
                    .as_ref()
                    .ok_or_else(|| invalid("row event without table"))?;
                let mut new = event.key.clone();
                if let Some(after) = &event.after {
                    new.extend(after.clone());
                }
                requested.entry(name.clone()).or_default().push(new);
            }
        }
        let mut classified = BTreeMap::new();
        for (name, rows) in requested {
            let statuses = self
                .classify_keys(&name, &rows, &transaction.commit_position, state)
                .await?;
            let table = self
                .tables
                .iter()
                .find(|t| t.catalog.key.table == name)
                .expect("validated table");
            for (row, status) in rows.iter().zip(statuses) {
                classified.insert((name.clone(), key_identity(&table.key(row)?)?), status);
            }
        }
        apply::plan_transaction(transaction, &tables, &context, |table, row| {
            let server_table = self
                .tables
                .iter()
                .find(|t| t.catalog.key.table == table.event_table)
                .ok_or(ApplyError::Invalid("unknown server classifier table"))?;
            let key = server_table
                .key(row)
                .and_then(|k| key_identity(&k))
                .map_err(|e| match e {
                    PgPlannerError::Apply(e) => e,
                    PgPlannerError::Infrastructure => {
                        ApplyError::Invalid("unexpected classifier I/O")
                    }
                })?;
            classified
                .get(&(table.event_table.clone(), key))
                .copied()
                .map(RangeStatus::apply_state)
                .ok_or(ApplyError::Invalid("missing prepared range classification"))
        })
        .map_err(Into::into)
    }
}
impl PgTransactionPlanner for ServerPlanner {
    fn plan<'a>(
        &'a mut self,
        transaction: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> PgPlanFuture<'a> {
        if self.preparation_started {
            return Box::pin(std::future::ready(Err(invalid(
                "cancelled server planner requires reconstruction",
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
fn key_identity(key: &Key) -> Result<Vec<u8>> {
    serde_json::to_vec(key).map_err(|_| invalid("catalog key identity encoding"))
}
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
/// Rebind only the exact generated FROM prefix. Everything after WHERE is the
/// original chunk builder output, including ORDER BY. The only adaptation is
/// explicit ROW constructors around uniform-bound operands; this does not rebuild
/// their comparisons. Mixed directions retain its lexicographic prefix expansion.
fn membership_query(
    table: &chunk::TableKey,
    range: &ChunkRange,
    keys: &[Key],
) -> Result<ChunkQuery> {
    if keys.is_empty() {
        return Err(invalid("empty server classifier query"));
    }
    let mut query = table.read_query(range).map_err(chunk_error)?;
    let prefix = format!(
        "SELECT * FROM {} WHERE ",
        table.relation.sql().map_err(chunk_error)?
    );
    let suffix = query
        .sql
        .strip_prefix(&prefix)
        .ok_or_else(|| invalid("chunk query prefix changed"))?;
    let suffix = if table
        .columns
        .iter()
        .all(|c| c.direction == table.columns[0].direction)
    {
        explicit_rows(
            suffix,
            2 * (usize::from(range.lo.is_some()) + usize::from(range.hi.is_some())),
        )?
    } else {
        suffix.to_string()
    };
    let mut ordinal = "__skedaddle_catalog_ordinal".to_string();
    while table.columns.iter().any(|c| c.name == ordinal) {
        ordinal.push('_');
    }
    let mut values = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        table.validate_key(key).map_err(chunk_error)?;
        query.parameters.push(i.to_string());
        let mut cells = vec![format!("${}::text::bigint", query.parameters.len())];
        for (value, column) in key.iter().zip(&table.columns) {
            let text = value
                .format_pg_text(&column.pg_type)
                .map_err(|_| invalid("catalog key binding"))?
                .ok_or_else(|| invalid("NULL catalog key"))?;
            query
                .parameters
                .push(String::from_utf8(text).map_err(|_| invalid("non-UTF8 catalog key"))?);
            cells.push(format!(
                "${}::text::{}",
                query.parameters.len(),
                column.sql_type.sql().map_err(chunk_error)?
            ));
        }
        values.push(format!("({})", cells.join(", ")));
    }
    if query.parameters.len() > 65535 {
        return Err(invalid("server classifier parameter limit"));
    }
    let mut columns = vec![quote(&ordinal)];
    columns.extend(table.columns.iter().map(|c| quote(&c.name)));
    query.sql = format!(
        "SELECT {} FROM (VALUES {}) AS \"__catalog_keys\" ({}) WHERE {suffix}",
        quote(&ordinal),
        values.join(", "),
        columns.join(", ")
    );
    Ok(query)
}

/// Insert ROW only at the generated uniform comparison's operand depth. SQL
/// identifiers may contain arbitrary parentheses and escaped quotes, so never
/// inspect their contents as grammar. Require exactly two operands per bound.
fn explicit_rows(sql: &str, expected: usize) -> Result<String> {
    let mut chars = sql.char_indices().peekable();
    let mut quoted = false;
    let mut depth = 0usize;
    let mut insertions = Vec::new();
    while let Some((i, c)) = chars.next() {
        if c == '"' {
            if quoted && chars.peek().is_some_and(|(_, next)| *next == '"') {
                chars.next();
            } else {
                quoted = !quoted;
            }
        } else if !quoted {
            match c {
                '(' => {
                    if depth == 1 {
                        insertions.push(i);
                    }
                    depth += 1;
                }
                ')' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| invalid("malformed generated chunk parentheses"))?;
                }
                _ => {}
            }
        }
    }
    if quoted || depth != 0 || insertions.len() != expected {
        return Err(invalid("uniform chunk predicate grammar changed"));
    }
    let mut result = String::with_capacity(sql.len() + 3 * insertions.len());
    let mut previous = 0;
    for i in insertions {
        result.push_str(&sql[previous..i]);
        result.push_str("ROW");
        previous = i;
    }
    result.push_str(&sql[previous..]);
    Ok(result)
}
async fn verify_utf8(client: &mut Client) -> Result<()> {
    let tx = client.build_transaction().read_only(true).start().await?;
    let row = tx.query_one("SELECT pg_catalog.current_setting('server_encoding'),pg_catalog.current_setting('client_encoding')", &[]).await?;
    if row.get::<_, String>(0) != "UTF8" || row.get::<_, String>(1) != "UTF8" {
        return Err(invalid("server classifier requires UTF8 encodings"));
    }
    tx.commit().await?;
    Ok(())
}
async fn setup_source(client: &mut Client) -> Result<()> {
    client
        .batch_execute(crate::pg::source::REQUIRED_SETTINGS_SQL)
        .await?;
    client
        .batch_execute("SET default_transaction_read_only = on")
        .await?;
    verify_utf8(client).await?;
    let tx = client.build_transaction().read_only(true).start().await?;
    let row = tx.query_one("SELECT pg_catalog.current_setting('DateStyle'),pg_catalog.current_setting('IntervalStyle'),pg_catalog.current_setting('TimeZone'),pg_catalog.current_setting('extra_float_digits'),pg_catalog.current_setting('standard_conforming_strings'),pg_catalog.current_setting('transaction_read_only')", &[]).await?;
    for (i, expected) in ["ISO, MDY", "postgres", "UTC", "3", "on", "on"]
        .iter()
        .enumerate()
    {
        if row.get::<_, String>(i) != *expected {
            return Err(invalid("source classifier session settings mismatch"));
        }
    }
    tx.commit().await?;
    Ok(())
}
#[cfg(test)]
mod tests;
