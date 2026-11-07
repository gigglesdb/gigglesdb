//! Pure unique-target planning against one authoritative staged snapshot.
use super::*;
use crate::relational::{
    Relational,
    secondary_index::{self, Descriptor},
    secondary_unique_lookup::Limits,
};

/// Preserve SQL failures until the frontend chooses its error representation.
#[derive(Debug)]
pub(super) enum PlanError {
    Engine(Error),
    Sql(SqlError),
}
impl From<Error> for PlanError {
    fn from(error: Error) -> Self {
        Self::Engine(error)
    }
}
impl PlanError {
    /// Exactly the previous native representation.
    #[cfg_attr(not(test), allow(dead_code, reason = "native planner compatibility"))]
    pub(super) fn into_operations(self) -> Error {
        match self {
            Self::Engine(error) => error,
            Self::Sql(error) => core(CoreError::Sql(error)),
        }
    }
}

/// Arbiter used to resolve an incoming conflict.
pub(super) enum Arbiter<'a> {
    /// Exact named unique descriptor (existing behavior).
    Unique(&'a Descriptor),
    /// The table's primary key.
    PrimaryKey,
    /// PK plus every unique descriptor; valid only for DO NOTHING.
    AnyUnique,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Semantics {
    Native,
    Sql,
}

struct ResolvedArbiter<'a> {
    columns: Vec<usize>,
    descriptor: Option<&'a Descriptor>,
}
impl ResolvedArbiter<'_> {
    fn tuple(
        &self,
        value: impl Fn(usize) -> Value,
    ) -> std::result::Result<Option<Vec<u8>>, PlanError> {
        let values = self
            .columns
            .iter()
            .map(|index| value(*index))
            .collect::<Vec<_>>();
        if values.iter().any(|value| matches!(value, Value::Null)) {
            Ok(None)
        } else {
            crate::relational::encode_key(&values)
                .map(Some)
                .map_err(sql)
        }
    }
}

/// One physical change; skips and no-op updates never appear here.
pub(super) struct PlannedChange<'s> {
    pub(super) table: u64,
    input_index: usize,
    pub(super) old: Option<(&'s [u8], &'s SqlRow)>,
    pub(super) new: (Vec<u8>, SqlRow),
}
pub(super) struct UniquePlan<'s> {
    pub(super) changes: Vec<PlannedChange<'s>>,
    pub(super) next_version: u64,
    pub(super) outcome: Outcome,
}
impl UniquePlan<'_> {
    /// Borrowed view for BatchApply::apply.
    pub(super) fn batch(&self) -> Vec<crate::relational::BatchRowChange<'_>> {
        self.changes
            .iter()
            .map(|change| crate::relational::BatchRowChange {
                table: change.table,
                old: change.old,
                new: Some((&change.new.0, &change.new.1)),
            })
            .collect()
    }
}
fn tick(cancel: &mut dyn FnMut() -> bool) -> std::result::Result<(), PlanError> {
    if cancel() {
        Err(PlanError::Sql(SqlError::Aborted))
    } else {
        Ok(())
    }
}
fn sql(error: SqlError) -> PlanError {
    PlanError::Sql(error)
}
fn legacy(column: &ColumnDef) -> bool {
    matches!(
        column.data_type,
        DataType::Boolean
            | DataType::SmallInt
            | DataType::Integer
            | DataType::BigInt
            | DataType::Text
            | DataType::Bytes
    )
}
struct Resolved<'s> {
    key: Vec<u8>,
    rowid: Option<(usize, Value)>,
    winner: Option<(&'s [u8], &'s SqlRow)>,
    arbiters: Vec<Option<Vec<u8>>>,
}
enum Postimage<'a> {
    Insert,
    Skip,
    Update {
        values: Vec<(usize, Cow<'a, Value>)>,
        changed: bool,
    },
}
impl Postimage<'_> {
    fn changed(&self) -> bool {
        matches!(self, Self::Insert | Self::Update { changed: true, .. })
    }
}
fn value<'a>(
    post: &'a Postimage<'_>,
    resolved: &'a Resolved<'_>,
    incoming: &'a [Value],
    index: usize,
) -> &'a Value {
    match post {
        Postimage::Insert => resolved
            .rowid
            .as_ref()
            .filter(|(column, _)| *column == index)
            .map_or(&incoming[index], |(_, value)| value),
        Postimage::Update { values, .. } => {
            values.iter().find(|(column, _)| *column == index).map_or(
                &resolved.winner.expect("update winner").1.values[index],
                |(_, value)| value.as_ref(),
            )
        }
        Postimage::Skip => unreachable!("skip has no postimage"),
    }
}

#[expect(clippy::too_many_arguments, reason = "frozen private planner contract")]
pub(super) fn plan_unique_conflict<'s>(
    src: &'s Relational,
    table: &TableDef,
    epoch: u64,
    arbiter: Arbiter<'_>,
    semantics: Semantics,
    incoming: &[Vec<Value>],
    action: &BoundConflict,
    returning: Option<&[BoundProjection]>,
    mut retained: Option<&mut usize>,
    cancel: &mut dyn FnMut() -> bool,
) -> std::result::Result<UniquePlan<'s>, PlanError> {
    // Raw incoming admission precedes comparisons, key copies and callbacks.
    if incoming.len() > crate::relational::MAX_ROWS {
        return Err(Error::Limit.into());
    }
    let hidden_rowid = table.hidden_rowid();
    let mut remaining_input = MAX_INSERT_BYTES;
    for row in incoming {
        if row.len() != table.columns.len() {
            return Err(Error::Type.into());
        }
        let mut remaining_row = crate::relational::MAX_ROW_BYTES;
        for (index, (value, column)) in row.iter().zip(&table.columns).enumerate() {
            if hidden_rowid == Some(index) {
                charge_value(&mut remaining_row, &Value::Int(0))?;
                charge_value(&mut remaining_input, &Value::Int(0))?;
                continue;
            }
            check(value, column)?;
            charge_value(&mut remaining_row, value)?;
            charge_value(&mut remaining_input, value)?;
        }
    }
    tick(cancel)?;
    for _ in incoming {
        tick(cancel)?;
    }
    if src.epoch != epoch
        || src
            .tables
            .values()
            .find(|candidate| candidate.id == table.id)
            != Some(table)
    {
        return Err(sql(SqlError::StaleSchema));
    }
    if semantics == Semantics::Native && !matches!(arbiter, Arbiter::Unique(_))
        || matches!(arbiter, Arbiter::AnyUnique) && matches!(action, BoundConflict::Update { .. })
    {
        return Err(Error::Unsupported.into());
    }
    if let Arbiter::Unique(target) = &arbiter
        && (!target.unique || target.table != table.id || target.columns.is_empty())
    {
        return Err(Error::Unsupported.into());
    }
    if hidden_rowid.is_some() && matches!(arbiter, Arbiter::PrimaryKey) {
        debug_assert!(false, "heap primary key cannot be a SQL arbiter");
        return Err(Error::Unsupported.into());
    }
    let sidecar = src.secondary_state_engine(&mut *cancel).map_err(sql)?;
    let mut targets = Vec::new();
    let include_pk = hidden_rowid.is_none() && !matches!(arbiter, Arbiter::Unique(_));
    match arbiter {
        Arbiter::Unique(target) => {
            let descriptor = sidecar
                .descriptors()
                .iter()
                .find(|descriptor| descriptor.id == target.id)
                .ok_or(Error::Unsupported)?;
            if descriptor.table != target.table
                || descriptor.columns != target.columns
                || descriptor.key_encoding != target.key_encoding
                || descriptor.unique != target.unique
            {
                return Err(Error::Unsupported.into());
            }
            targets.push(target);
        }
        Arbiter::PrimaryKey => {}
        Arbiter::AnyUnique => {
            targets.extend(
                sidecar
                    .descriptors()
                    .iter()
                    .filter(|descriptor| descriptor.table == table.id && descriptor.unique),
            );
            targets.sort_by_key(|descriptor| descriptor.id);
        }
    }
    let mut arbiters = Vec::new();
    if include_pk {
        arbiters.push(ResolvedArbiter {
            columns: table.primary_key.clone(),
            descriptor: None,
        });
    }
    for target in targets {
        let mut columns = Vec::new();
        for id in &target.columns {
            let index = table
                .columns
                .iter()
                .position(|column| column.id == *id)
                .ok_or(Error::Unsupported)?;
            if !legacy(&table.columns[index]) {
                return Err(Error::Unsupported.into());
            }
            columns.push(index);
        }
        arbiters.push(ResolvedArbiter {
            columns,
            descriptor: Some(target),
        });
    }
    // The unique lookup bridge requires legacy PK kinds; PK map lookup does not.
    // Sidecar validation and the publisher retain their own key restrictions.
    if !include_pk {
        for index in &table.primary_key {
            if !table.columns.get(*index).is_some_and(legacy) {
                return Err(Error::Unsupported.into());
            }
        }
    }
    tick(cancel)?;
    let limits = Limits {
        bytes: 64 * 1024 * 1024,
        work: secondary_index::MAX_WORK * 3,
        candidates: secondary_index::MAX_CANDIDATES,
    };
    let base = if include_pk {
        Some(
            src.secondary_access_engine(table, epoch, &mut *cancel)
                .map_err(sql)?
                .1,
        )
    } else {
        None
    };
    let mut resolved = Vec::new();
    for row in incoming {
        tick(cancel)?;
        let mut winner = None;
        let mut tuples = Vec::new();
        for arbiter in &arbiters {
            let tuple = arbiter.tuple(|index| row[index].clone())?;
            if winner.is_none()
                && let Some(tuple) = &tuple
            {
                winner = if let Some(target) = arbiter.descriptor {
                    let values = arbiter
                        .columns
                        .iter()
                        .map(|index| row[*index].clone())
                        .collect::<Vec<_>>();
                    src.secondary_unique_lookup_engine(
                        table,
                        epoch,
                        target,
                        &values,
                        &limits,
                        &mut *cancel,
                    )
                    .map_err(sql)?
                } else {
                    base.expect("PK base map")
                        .get_key_value(tuple)
                        .map(|(key, row)| (key.as_slice(), row))
                };
            }
            tuples.push(tuple);
        }
        let key = if hidden_rowid.is_some() {
            Vec::new()
        } else {
            crate::relational::encode_key(
                &table
                    .primary_key
                    .iter()
                    .map(|index| row[*index].clone())
                    .collect::<Vec<_>>(),
            )
            .map_err(sql)?
        };
        resolved.push(Resolved {
            key,
            rowid: None,
            winner,
            arbiters: tuples,
        });
    }
    // Every input resolves against the same source before duplicate refusal.
    tick(cancel)?;
    let mut native_arbiters = BTreeSet::new();
    let mut winners = BTreeSet::new();
    let mut inserts = BTreeSet::new();
    for resolved in &resolved {
        tick(cancel)?;
        if semantics == Semantics::Sql {
            continue;
        }
        if let Some((pk, _)) = resolved.winner
            && !winners.insert(pk)
        {
            return Err(Error::DuplicateTarget.into());
        }
        if let Some(arbiter) = &resolved.arbiters[0]
            && !native_arbiters.insert(arbiter.as_slice())
        {
            return Err(Error::DuplicateTarget.into());
        }
        if hidden_rowid.is_none()
            && resolved.winner.is_none()
            && !inserts.insert(resolved.key.as_slice())
        {
            return Err(Error::DuplicateTarget.into());
        }
    }
    tick(cancel)?;
    let source_rows = src
        .secondary_access_engine(table, epoch, &mut *cancel)
        .map_err(sql)?
        .1
        .len();
    if source_rows.checked_add(inserts.len()).ok_or(Error::Limit)? > crate::relational::MAX_ROWS {
        return Err(Error::Limit.into());
    }
    tick(cancel)?;
    let mut guards = vec![true; incoming.len()];
    if let BoundConflict::Update { guard, .. } = action {
        for (index, (proposal, resolved)) in incoming.iter().zip(&resolved).enumerate() {
            tick(cancel)?;
            if let Some((_, old)) = resolved.winner
                && *guard.eval_conflict(&|index| &old.values[index], &|index| &proposal[index])?
                    != Value::Bool(true)
            {
                if semantics == Semantics::Native {
                    return Err(Error::ConditionFailed.into());
                }
                guards[index] = false;
            }
        }
    }
    tick(cancel)?;
    let mut postimages = Vec::new();
    let mut remaining_mutation = MAX_MUTATION_BYTES;
    let mut claimed_winners = BTreeSet::new();
    let mut proposed = vec![BTreeSet::new(); arbiters.len()];
    let mut sql_inserts = 0usize;
    for (input_index, (proposal, resolved)) in incoming.iter().zip(&resolved).enumerate() {
        tick(cancel)?;
        let mut skip = false;
        if semantics == Semantics::Sql {
            let proposed_conflict = resolved
                .arbiters
                .iter()
                .zip(&proposed)
                .any(|(tuple, prior)| tuple.as_ref().is_some_and(|tuple| prior.contains(tuple)));
            match action {
                BoundConflict::Nothing => skip = resolved.winner.is_some() || proposed_conflict,
                BoundConflict::Update { .. } => {
                    if proposed_conflict
                        || resolved
                            .winner
                            .is_some_and(|(key, _)| !claimed_winners.insert(key))
                    {
                        return Err(Error::DuplicateTarget.into());
                    }
                    skip = !guards[input_index];
                }
            }
        }
        let mut post = match (resolved.winner, action) {
            _ if skip => Postimage::Skip,
            (None, _) => Postimage::Insert,
            (Some(_), BoundConflict::Nothing) => Postimage::Skip,
            (Some((_, old)), BoundConflict::Update { values, .. }) => {
                let mut evaluated = Vec::new();
                for (index, expression) in values {
                    if table.primary_key.contains(index) {
                        return Err(Error::Unsupported.into());
                    }
                    evaluated.push((
                        *index,
                        expression.eval_conflict(&|index| &old.values[index], &|index| {
                            &proposal[index]
                        })?,
                    ));
                }
                Postimage::Update {
                    values: evaluated,
                    changed: false,
                }
            }
        };
        if !matches!(post, Postimage::Skip) {
            let mut remaining_row = crate::relational::MAX_ROW_BYTES;
            for (index, column) in table.columns.iter().enumerate() {
                if hidden_rowid == Some(index) && matches!(post, Postimage::Insert) {
                    charge_value(&mut remaining_row, &Value::Int(0))?;
                    charge_value(&mut remaining_mutation, &Value::Int(0))?;
                    continue;
                }
                let final_value = value(&post, resolved, proposal, index);
                check(final_value, column)?;
                charge_value(&mut remaining_row, final_value)?;
                charge_value(&mut remaining_mutation, final_value)?;
            }
            // Equality is admitted only after the entire final row.
            if let Postimage::Update { values, changed } = &mut post {
                let old = resolved.winner.expect("update winner").1;
                *changed = semantics == Semantics::Sql
                    || values
                        .iter()
                        .any(|(index, value)| value.as_ref() != &old.values[*index]);
            }
        }
        if semantics == Semantics::Sql && !matches!(post, Postimage::Skip) {
            for (arbiter, prior) in arbiters.iter().zip(&mut proposed) {
                tick(cancel)?;
                if let Some(tuple) =
                    arbiter.tuple(|index| value(&post, resolved, proposal, index).clone())?
                {
                    prior.insert(tuple);
                }
            }
            if matches!(post, Postimage::Insert) {
                sql_inserts += 1;
            }
        }
        postimages.push(post);
    }
    tick(cancel)?;
    if source_rows.checked_add(sql_inserts).ok_or(Error::Limit)? > crate::relational::MAX_ROWS {
        return Err(Error::Limit.into());
    }
    let mut next_version = src.next_version_engine();
    let mut versions = vec![0; incoming.len()];
    if let Some(column) = hidden_rowid {
        // Existing winners retain key-order version allocation. Fresh insert rowids
        // exceed every existing key, so allocate those next in input order.
        let mut updates = (0..incoming.len())
            .filter(|index| matches!(postimages[*index], Postimage::Update { changed: true, .. }))
            .collect::<Vec<_>>();
        updates.sort_by_key(|index| resolved[*index].winner.expect("update winner").0);
        for index in updates {
            tick(cancel)?;
            versions[index] = next_version;
            next_version = next_version.checked_add(1).ok_or(Error::Overflow)?;
        }
        for (index, post) in postimages.iter().enumerate() {
            tick(cancel)?;
            if matches!(post, Postimage::Insert) {
                let rowid = i64::try_from(next_version).map_err(|_| sql(SqlError::Overflow))?;
                versions[index] = next_version;
                next_version = next_version.checked_add(1).ok_or(Error::Overflow)?;
                resolved[index].rowid = Some((column, Value::Int(rowid)));
                resolved[index].key =
                    crate::relational::encode_key(&[Value::Int(rowid)]).map_err(sql)?;
            }
        }
    }
    let mut order = (0..incoming.len()).collect::<Vec<_>>();
    let key = |index: usize| {
        resolved[index]
            .winner
            .map_or(resolved[index].key.as_slice(), |(key, _)| key)
    };
    order.sort_by(|left, right| key(*left).cmp(key(*right)));
    for index in &order {
        tick(cancel)?;
        if hidden_rowid.is_some() && postimages[*index].changed() {
            continue;
        }
        if postimages[*index].changed() {
            versions[*index] = next_version;
            next_version = next_version.checked_add(1).ok_or(Error::Overflow)?;
        } else if let Some((_, old)) = resolved[*index].winner {
            versions[*index] = old.version;
        }
    }
    let mut outcome = Outcome {
        matched: incoming.len(),
        affected: postimages
            .iter()
            .filter(|post| !matches!(post, Postimage::Skip))
            .count(),
        changed: postimages.iter().filter(|post| post.changed()).count(),
        conflict_results: Some(
            postimages
                .iter()
                .enumerate()
                .map(|(input_index, post)| ConflictTupleOutcome {
                    input_index,
                    status: if matches!(post, Postimage::Skip) {
                        ConflictStatus::Skipped
                    } else {
                        ConflictStatus::Success
                    },
                    changed: post.changed(),
                    returned_row: None,
                })
                .collect(),
        ),
        ..Outcome::default()
    };
    tick(cancel)?;
    if let Some(projection) = returning {
        outcome.columns = projection
            .iter()
            .map(|item| item.column.name.clone())
            .collect();
        let mut remaining = crate::relational::MAX_RESULT_BYTES;
        let mut projected = Vec::new();
        for index in &order {
            tick(cancel)?;
            if matches!(postimages[*index], Postimage::Skip) {
                continue;
            }
            let values = project_returning(
                projection,
                &|column| {
                    value(
                        &postimages[*index],
                        &resolved[*index],
                        &incoming[*index],
                        column,
                    )
                },
                &mut remaining,
            )?;
            outcome
                .conflict_results
                .as_mut()
                .expect("conflict outcomes")[*index]
                .returned_row = Some(projected.len());
            projected.push((versions[*index], values));
        }
        for (_, values) in &projected {
            for value in values {
                charge_retained(&mut retained, value)?;
            }
        }
        outcome.rows = projected
            .into_iter()
            .map(|(version, values)| SqlRow {
                version,
                values: values.into_iter().map(Cow::into_owned).collect(),
            })
            .collect();
    }
    tick(cancel)?;
    let mut changes = Vec::new();
    for index in order {
        tick(cancel)?;
        if !postimages[index].changed() {
            continue;
        }
        let values = (0..table.columns.len())
            .map(|column| {
                value(
                    &postimages[index],
                    &resolved[index],
                    &incoming[index],
                    column,
                )
                .clone()
            })
            .collect();
        changes.push(PlannedChange {
            input_index: index,
            table: table.id,
            old: resolved[index].winner,
            new: (
                key(index).to_vec(),
                SqlRow {
                    values,
                    version: versions[index],
                },
            ),
        });
    }
    tick(cancel)?;
    Ok(UniquePlan {
        changes,
        next_version,
        outcome,
    })
}

fn into_sql(error: PlanError) -> SqlError {
    match error {
        PlanError::Sql(error) => error,
        PlanError::Engine(error) => match error {
            Error::DuplicateTarget => SqlError::Duplicate,
            Error::Unsupported => SqlError::Unsupported,
            Error::Type => SqlError::Type,
            Error::DivisionByZero => SqlError::DivisionByZero,
            Error::Limit => SqlError::Limit,
            Error::Overflow => SqlError::Overflow,
            Error::ConditionFailed => {
                debug_assert!(false, "SQL planner returned native condition failure");
                SqlError::Type
            }
            Error::Core(_) => {
                debug_assert!(false, "SQL planner lost a structured SQL error");
                SqlError::Unsupported
            }
            _ => SqlError::Unsupported,
        },
    }
}

type SqlPostimageProjection<'a> =
    dyn FnMut(&Relational, &[&SqlRow]) -> std::result::Result<Vec<SqlRow>, SqlError> + 'a;

impl Relational {
    /// SQL-only binding and publication; native planner semantics stay unchanged.
    pub(crate) fn sql_conflict_engine(
        &mut self,
        table: &TableDef,
        target: (Option<&[String]>, Option<&str>),
        incoming: &[Vec<Value>],
        action: &ConflictAction,
        cancel: &mut dyn FnMut() -> bool,
    ) -> std::result::Result<crate::relational::SqlResult, SqlError> {
        self.sql_conflict_engine_projected(table, target, incoming, action, None, cancel)
    }

    pub(crate) fn sql_conflict_engine_projected(
        &mut self,
        table: &TableDef,
        target: (Option<&[String]>, Option<&str>),
        incoming: &[Vec<Value>],
        action: &ConflictAction,
        project: Option<&mut SqlPostimageProjection<'_>>,
        cancel: &mut dyn FnMut() -> bool,
    ) -> std::result::Result<crate::relational::SqlResult, SqlError> {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        Self::sql_admit_conflict_input(table, incoming, action)?;
        if cancel() {
            return Err(SqlError::Aborted);
        }
        // Immutable planner source. Its whole-state size was already bounded
        // by secondary_admit_mutation in the relational mutation entry.
        #[cfg(test)]
        Self::sql_insert_test_event(2);
        let src = self.clone();
        let descriptors = src.secondary_descriptors_engine();
        let arbiter = if let Some(columns) = target.0 {
            let columns = columns
                .iter()
                .map(|name| {
                    table
                        .columns
                        .iter()
                        .position(|column| &column.name == name)
                        .ok_or(SqlError::UnknownColumn)
                })
                .collect::<std::result::Result<BTreeSet<_>, _>>()?;
            let primary = table.primary_key.iter().copied().collect::<BTreeSet<_>>();
            if columns == primary {
                Arbiter::PrimaryKey
            } else {
                let ids = columns
                    .iter()
                    .map(|index| table.columns[*index].id)
                    .collect::<BTreeSet<_>>();
                let descriptor = descriptors
                    .iter()
                    .filter(|descriptor| {
                        descriptor.table == table.id
                            && descriptor.unique
                            && descriptor.columns.iter().copied().collect::<BTreeSet<_>>() == ids
                    })
                    .min_by_key(|descriptor| descriptor.id)
                    .ok_or(SqlError::Unsupported)?;
                Arbiter::Unique(descriptor)
            }
        } else if let Some(name) = target.1 {
            if let Some(descriptor) = descriptors.iter().find(|descriptor| {
                descriptor.table == table.id && descriptor.unique && descriptor.name == name
            }) {
                Arbiter::Unique(descriptor)
            } else if table.hidden_rowid().is_none() && name == format!("{}_pkey", table.name) {
                Arbiter::PrimaryKey
            } else {
                return Err(SqlError::Unsupported);
            }
        } else if matches!(action, ConflictAction::Nothing) {
            Arbiter::AnyUnique
        } else {
            return Err(SqlError::Syntax);
        };
        let bindings = Bindings::new();
        let mut binder = Binder {
            table: Some(table),
            allow_fields: false,
            allow_conflict_fields: true,
            locals: None,
            bindings: &bindings,
            used: BTreeSet::new(),
            remaining_value_bytes: MAX_BOUND_VALUE_BYTES,
        };
        let bound = (|| -> Result<BoundConflict> {
            Ok(match action {
                ConflictAction::Nothing => BoundConflict::Nothing,
                ConflictAction::Update { guard, values } => BoundConflict::Update {
                    guard: binder.optional(guard)?,
                    values: binder.expression_assignments(values)?,
                },
            })
        })()
        .map_err(|error| into_sql(error.into()))?;
        #[cfg(test)]
        Self::sql_insert_test_event(3);
        let plan = plan_unique_conflict(
            &src,
            table,
            self.epoch,
            arbiter,
            Semantics::Sql,
            incoming,
            &bound,
            None,
            None,
            cancel,
        )
        .map_err(into_sql)?;
        // SQL semantics mark every accepted postimage changed.
        if plan.changes.len() != plan.outcome.affected {
            return Err(SqlError::Unsupported);
        }
        // Borrow planner postimages in VALUES order without copying full rows.
        let returned = if let Some(project) = project {
            let mut changes = plan.changes.iter().collect::<Vec<_>>();
            changes.sort_by_key(|change| change.input_index);
            project(
                &src,
                &changes
                    .iter()
                    .map(|change| &change.new.1)
                    .collect::<Vec<_>>(),
            )?
        } else {
            vec![]
        };
        #[cfg(test)]
        Self::sql_insert_test_event(4);
        let mut result = self.secondary_apply_batch_engine(
            self.epoch,
            &plan.batch(),
            plan.next_version,
            cancel,
        )?;
        result.affected = plan.outcome.affected as u64;
        result.rows = returned;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "unique_conflict_tests.rs"]
mod tests;
