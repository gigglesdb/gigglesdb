//! Correlated equalities can reuse physical probes or a statement-local lookup
//! of snapshot rows. The ordinary WHERE expression still checks every candidate.
use super::*;
use std::{borrow::Cow, ops::Bound, rc::Rc};

#[derive(Ord, PartialOrd, Eq, PartialEq)]
enum Key {
    Integer(i64),
    Boolean(bool),
    Uuid(String),
}
impl Key {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int(n) => Some(Self::Integer(*n)),
            Value::Bool(b) => Some(Self::Boolean(*b)),
            Value::Text(s) => Some(Self::Uuid(s.clone())),
            _ => None,
        }
    }
}

type Row = (Vec<u8>, SqlRow);
type Groups = Rc<BTreeMap<Key, Vec<Row>>>;
#[derive(Default)]
pub(in crate::relational::query) struct Cache {
    groups: RefCell<BTreeMap<(u64, usize), Option<Groups>>>,
}

pub(super) struct Lookup<'a, 'e> {
    base: &'a dyn scan::RowSource,
    execution: &'a Execution<'e>,
    table: u64,
    column: usize,
    value: Value,
}

impl<'a, 'e> Lookup<'a, 'e> {
    pub(super) fn new(
        plan: &Plan,
        state: &Relational,
        base: &'a dyn scan::RowSource,
        execution: &'a Execution<'e>,
    ) -> Option<Self> {
        let Body::Select(select) = &plan.body else {
            return None;
        };
        let [source] = select.query.sources.as_slice() else {
            return None;
        };
        if select.input.is_some()
            || select.from.is_some()
            || !plan.ctes.is_empty()
            || select.query.derived().is_some()
            || source.name.starts_with('\0')
            || state.view_names.contains_key(&source.name)
        {
            return None;
        }
        let table = state.tables.get(&source.name)?;
        let (column, value) = equality(select.query.where_filter()?, source, table)?;
        Some(Self {
            base,
            execution,
            table: table.id,
            column,
            value,
        })
    }

    fn groups(&self, table: &TableDef) -> R<Option<Groups>> {
        let cache = &self.execution.correlation_rows.groups;
        let id = (self.table, self.column);
        if let Some(groups) = cache.borrow().get(&id) {
            return Ok(groups.clone());
        }
        let mut groups = BTreeMap::<_, Vec<_>>::new();
        let mut bytes = 0usize;
        let scope = RetentionScope::new(self.execution);
        let all = scan::KeyRange {
            lo: Bound::Unbounded,
            hi: Bound::Unbounded,
        };
        for row in self.base.scan(table, &all, scan::Direction::Forward)? {
            self.execution.comparison()?;
            let row = row?;
            let Some(key) = Key::from_value(&row.row.values[self.column]) else {
                continue;
            };
            let size = row.key.len()
                + std::mem::size_of::<Row>()
                + 64
                + row
                    .row
                    .values
                    .iter()
                    .map(|value| std::mem::size_of::<Value>() + value_bytes(value))
                    .sum::<usize>();
            if scope.borrow().charge(size).is_err() {
                cache.borrow_mut().insert(id, None);
                return Ok(None);
            }
            bytes += size;
            groups
                .entry(key)
                .or_default()
                .push((row.key.into_owned(), row.row.into_owned()));
        }
        drop(scope);
        self.execution.retention.borrow_mut().charge(bytes)?;
        let groups = Rc::new(groups);
        cache.borrow_mut().insert(id, Some(groups.clone()));
        Ok(Some(groups))
    }
}

fn equality(filter: &Filter, source: &Source, table: &TableDef) -> Option<(usize, Value)> {
    match filter {
        Filter::Logic(left, true, right) => {
            equality(left, source, table).or_else(|| equality(right, source, table))
        }
        Filter::Compare(left, op, right) if op == "=" => {
            let (column, literal) = match (left, right) {
                (Scalar::Column(column), literal) | (literal, Scalar::Column(column)) => {
                    (column, literal)
                }
                _ => return None,
            };
            let Scalar::Cast(literal, ty) = literal else {
                return None;
            };
            let Scalar::Literal(value) = &**literal else {
                return None;
            };
            // Equality for other types may depend on collation, typmods,
            // numeric scale, or the session. Leave those to the WHERE evaluator.
            if !matches!(
                ty,
                DataType::SmallInt
                    | DataType::Integer
                    | DataType::BigInt
                    | DataType::Boolean
                    | DataType::Uuid
            ) || column
                .qualifier
                .as_ref()
                .is_some_and(|q| q != &source.alias)
            {
                return None;
            }
            let i = col(table, &column.name).ok()?;
            let def = &table.columns[i];
            if &def.data_type != ty || *value == Value::Null {
                return None;
            }
            // Match the bound predicate's cast before comparing against stored values.
            // A speculative cast must leave any diagnostics to normal binding.
            let value = user_types::probe_preserving_error_message(|| {
                functions::cast_value(value.clone(), ty, &mut 0)
            })
            .ok()?;
            check(&value, def).ok()?;
            Some((i, value))
        }
        _ => None,
    }
}

impl scan::RowSource for Lookup<'_, '_> {
    fn streams_reductions(&self) -> bool {
        self.base.streams_reductions()
    }

    fn scan<'s>(
        &'s self,
        table: &TableDef,
        range: &scan::KeyRange,
        direction: scan::Direction,
    ) -> R<scan::SourceRows<'s>> {
        if table.id != self.table
            || direction != scan::Direction::Forward
            || *range != scan::KeyRange::from_bounds(bounds(table, &[], &[])?)
        {
            return self.base.scan(table, range, direction);
        }
        let Some(key) = Key::from_value(&self.value) else {
            return Ok(Box::new(std::iter::empty()));
        };
        if let Some(rows) =
            self.base
                .join_probe(table, &[(self.column, Some(self.value.clone()))], false)?
        {
            return Ok(rows);
        }
        let Some(groups) = self.groups(table)? else {
            return self.base.scan(table, range, direction);
        };
        let mut index = 0;
        Ok(Box::new(std::iter::from_fn(move || {
            let (key, row) = groups.get(&key)?.get(index)?;
            index += 1;
            Some(Ok(scan::SourceRow {
                key: Cow::Owned(key.clone()),
                row: Cow::Owned(row.clone()),
            }))
        })))
    }

    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<scan::SourceRow<'s>>> {
        self.base.get(table, key)
    }
}
