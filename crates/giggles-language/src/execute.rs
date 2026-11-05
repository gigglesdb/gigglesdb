use crate::*;
use giggles_core::{
    Admin, CoreError, SqlSession,
    relational::{ColumnDef, DataType, SqlError, TableDef},
};
use std::{cmp::Ordering, collections::BTreeSet};

/// Trusted embedded executor. Independent Admin holders retain unrestricted SQL authority.
#[derive(Clone)]
pub struct Engine {
    admin: Admin,
}
fn core(e: CoreError) -> Error {
    match e {
        CoreError::Sql(SqlError::Conflict) => Error::Conflict,
        _ => Error::Core(e.to_string()),
    }
}
impl Engine {
    pub fn new(admin: Admin) -> Self {
        Self { admin }
    }
    pub fn execute_text(&self, text: &str, bindings: &Bindings) -> Result<Outcome> {
        self.execute(&parse(text)?, bindings)
    }
    pub fn execute(&self, stmt: &Statement, bindings: &Bindings) -> Result<Outcome> {
        validate_depth(stmt)?;
        if serde_json::to_vec(stmt).map_err(|_| Error::Limit)?.len() > MAX_INPUT {
            return Err(Error::Limit);
        }
        if !parser::identifier(stmt.table()) {
            return Err(Error::Bind("identifier".into()));
        }
        let (epoch, tables) = self.admin.catalog().map_err(core)?;
        let table = tables
            .into_iter()
            .find(|t| t.name == stmt.table())
            .ok_or_else(|| Error::Bind("table".into()))?;
        let mut session = self.admin.sql_session();
        session.execute("begin", &[], None).map_err(core)?;
        let staged = (|| {
            // This prevents borrowing a primary-key definition from before BEGIN.
            if self.admin.catalog().map_err(core)?.0 != epoch {
                return Err(Error::Conflict);
            }
            let sql = format!("select * from {} limit {}", table.name, MAX_ROWS);
            let p = session.prepare(&sql).map_err(core)?;
            if session.describe(&p).map_err(core)?.columns != table.columns {
                return Err(Error::Conflict);
            }
            let mut binder = Binder {
                table: &table,
                bindings,
                used: BTreeSet::new(),
            };
            let operation = binder.bind(stmt)?;
            if binder.used != bindings.keys().cloned().collect() {
                return Err(Error::Bind("parameters".into()));
            }
            let rows = session.execute_prepared(&p, &[], None).map_err(core)?.rows;
            if rows.len() == MAX_ROWS {
                // MAX_ROWS is a core result-page bound, NOT a table capacity.
                // Compare the complete PK at the end of this page to the true
                // maximum in the same snapshot, including composite keys.
                let maximum = session
                    .scan(giggles_core::relational::Scan {
                        table: table.name.clone(),
                        projection: table
                            .primary_key
                            .iter()
                            .map(|i| table.columns[*i].name.clone())
                            .collect(),
                        prefix: vec![],
                        lower: None,
                        upper: None,
                        reverse: true,
                        limit: 1,
                    })
                    .map_err(core)?;
                let last = rows.last().ok_or(Error::Limit)?;
                let key: Vec<_> = table
                    .primary_key
                    .iter()
                    .map(|i| last.values[*i].clone())
                    .collect();
                if maximum.rows.first().is_none_or(|r| r.values != key) {
                    return Err(Error::Limit);
                }
                if matches!(operation, Operation::Insert(_)) {
                    return Err(Error::Limit);
                }
            }
            run(&mut session, &table, operation, rows)
        })();
        let outcome = match staged {
            Ok(v) => v,
            Err(e) => {
                session.rollback();
                return Err(e);
            }
        };
        match session.execute("commit", &[], None) {
            Ok(_) => Ok(outcome),
            Err(e) => {
                session.rollback();
                Err(match e {
                    CoreError::Storage(_) => Error::Unknown,
                    _ => core(e),
                })
            }
        }
    }
}
pub(crate) fn validate_depth(stmt: &Statement) -> Result<()> {
    let mut pending = vec![];
    match stmt {
        Statement::Read {
            filter: Some(e), ..
        } => pending.push((e, 0)),
        Statement::Update { filter, guard, .. } | Statement::Delete { filter, guard, .. } => {
            pending.push((filter, 0));
            if let Some(e) = guard {
                pending.push((e, 0))
            }
        }
        _ => (),
    }
    let mut nodes = 0;
    while let Some((e, d)) = pending.pop() {
        nodes += 1;
        if d > MAX_DEPTH || nodes > 4096 {
            return Err(Error::Limit);
        }
        match e {
            Expr::Compare(a, _, b) | Expr::And(a, b) | Expr::Or(a, b) => {
                pending.push((a, d + 1));
                pending.push((b, d + 1))
            }
            Expr::IsNull(e, _) | Expr::Not(e) => pending.push((e, d + 1)),
            _ => (),
        }
    }
    Ok(())
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ty {
    Bool,
    Int,
    Text,
    Bytes,
}
fn ty(v: &Value) -> Option<Ty> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(Ty::Bool),
        Value::Int(_) => Some(Ty::Int),
        Value::Text(_) => Some(Ty::Text),
        Value::Bytes(_) => Some(Ty::Bytes),
    }
}
fn colty(c: &ColumnDef) -> Ty {
    match c.data_type {
        DataType::Boolean => Ty::Bool,
        DataType::SmallInt | DataType::Integer | DataType::BigInt => Ty::Int,
        DataType::Text => Ty::Text,
        DataType::Bytes => Ty::Bytes,
    }
}
fn check(v: &Value, c: &ColumnDef) -> Result<()> {
    if *v == Value::Null {
        return if c.not_null { Err(Error::Type) } else { Ok(()) };
    }
    if ty(v) != Some(colty(c)) {
        return Err(Error::Type);
    }
    if let Value::Int(n) = v {
        match c.data_type {
            DataType::SmallInt if i16::try_from(*n).is_err() => return Err(Error::Type),
            DataType::Integer if i32::try_from(*n).is_err() => return Err(Error::Type),
            _ => (),
        }
    }
    Ok(())
}
#[derive(Clone)]
enum Bound {
    Value(Value),
    Field(usize),
    Compare(Box<Bound>, Compare, Box<Bound>),
    IsNull(Box<Bound>, bool),
    Not(Box<Bound>),
    And(Box<Bound>, Box<Bound>),
    Or(Box<Bound>, Box<Bound>),
}
struct Binder<'a> {
    table: &'a TableDef,
    bindings: &'a Bindings,
    used: BTreeSet<String>,
}
impl Binder<'_> {
    fn field(&self, s: &str) -> Result<usize> {
        if !parser::identifier(s) {
            return Err(Error::Bind("identifier".into()));
        }
        self.table
            .columns
            .iter()
            .position(|c| c.name == s)
            .ok_or_else(|| Error::Bind("column".into()))
    }
    fn value(&mut self, e: &ValueExpr) -> Result<Value> {
        match e {
            ValueExpr::Literal(v) => Ok(v.clone()),
            ValueExpr::Parameter(n) => {
                if n.len() > 63
                    || n.is_empty()
                    || !(n.as_bytes()[0].is_ascii_alphabetic() || n.starts_with('_'))
                    || !n
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
                {
                    return Err(Error::Bind("parameter name".into()));
                }
                self.used.insert(n.clone());
                self.bindings
                    .get(n)
                    .cloned()
                    .ok_or_else(|| Error::Bind("parameter missing".into()))
            }
        }
    }
    fn count(&mut self, e: &ValueExpr) -> Result<usize> {
        match self.value(e)? {
            Value::Int(n) if (0..=MAX_ROWS as i64).contains(&n) => Ok(n as usize),
            _ => Err(Error::Type),
        }
    }
    fn expr(&mut self, e: &Expr, d: usize) -> Result<(Bound, Option<Ty>)> {
        if d > MAX_DEPTH {
            return Err(Error::Limit);
        }
        let boolean = |t: Option<Ty>| {
            if t.is_none() || t == Some(Ty::Bool) {
                Ok(())
            } else {
                Err(Error::Type)
            }
        };
        Ok(match e {
            Expr::Value(v) => {
                let v = self.value(v)?;
                let t = ty(&v);
                (Bound::Value(v), t)
            }
            Expr::Field(n) => {
                let i = self.field(n)?;
                (Bound::Field(i), Some(colty(&self.table.columns[i])))
            }
            Expr::Compare(a, op, b) => {
                let (a, ta) = self.expr(a, d + 1)?;
                let (b, tb) = self.expr(b, d + 1)?;
                if ta.is_some() && tb.is_some() && ta != tb {
                    return Err(Error::Type);
                }
                (
                    Bound::Compare(Box::new(a), *op, Box::new(b)),
                    Some(Ty::Bool),
                )
            }
            Expr::IsNull(e, neg) => {
                let (e, _) = self.expr(e, d + 1)?;
                (Bound::IsNull(Box::new(e), *neg), Some(Ty::Bool))
            }
            Expr::Not(e) => {
                let (e, t) = self.expr(e, d + 1)?;
                boolean(t)?;
                (Bound::Not(Box::new(e)), Some(Ty::Bool))
            }
            Expr::And(a, b) | Expr::Or(a, b) => {
                let (a, ta) = self.expr(a, d + 1)?;
                let (b, tb) = self.expr(b, d + 1)?;
                boolean(ta)?;
                boolean(tb)?;
                (
                    if matches!(e, Expr::And(..)) {
                        Bound::And(Box::new(a), Box::new(b))
                    } else {
                        Bound::Or(Box::new(a), Box::new(b))
                    },
                    Some(Ty::Bool),
                )
            }
        })
    }
    fn predicate(&mut self, e: &Expr) -> Result<Bound> {
        let (e, t) = self.expr(e, 0)?;
        if t.is_some() && t != Some(Ty::Bool) {
            return Err(Error::Type);
        }
        Ok(e)
    }
    fn optional(&mut self, e: &Option<Expr>) -> Result<Bound> {
        match e {
            Some(e) => self.predicate(e),
            None => Ok(Bound::Value(Value::Bool(true))),
        }
    }
    fn order(&self, order: &[Sort]) -> Result<Vec<(usize, bool)>> {
        let mut seen = BTreeSet::new();
        let mut out = vec![];
        for o in order {
            let i = self.field(&o.field)?;
            if !seen.insert(i) {
                return Err(Error::Bind("duplicate order".into()));
            }
            out.push((i, o.descending))
        }
        for i in &self.table.primary_key {
            if seen.insert(*i) {
                out.push((*i, false))
            }
        }
        Ok(out)
    }
    fn assignments(
        &mut self,
        v: &[(String, ValueExpr)],
        insert: bool,
    ) -> Result<Vec<(usize, Value)>> {
        if v.is_empty() || v.len() > giggles_core::relational::MAX_COLUMNS {
            return Err(Error::Bind("assignments".into()));
        }
        let mut seen = BTreeSet::new();
        let mut out = vec![];
        for (n, e) in v {
            let i = self.field(n)?;
            if !seen.insert(i) {
                return Err(Error::Bind("duplicate assignment".into()));
            }
            if !insert && self.table.primary_key.contains(&i) {
                return Err(Error::Unsupported);
            }
            let v = self.value(e)?;
            check(&v, &self.table.columns[i])?;
            out.push((i, v))
        }
        Ok(out)
    }
    fn bind(&mut self, s: &Statement) -> Result<Operation> {
        Ok(match s {
            Statement::Read {
                filter,
                order,
                limit,
                projection,
                ..
            } => {
                let filter = self.optional(filter)?;
                let order = self.order(order)?;
                let limit = match limit {
                    Some(v) => self.count(v)?,
                    None => 100,
                };
                let projection = if projection.is_empty() {
                    (0..self.table.columns.len()).collect()
                } else {
                    let mut seen = BTreeSet::new();
                    let mut out = vec![];
                    for n in projection {
                        let i = self.field(n)?;
                        if !seen.insert(i) {
                            return Err(Error::Bind("duplicate projection".into()));
                        }
                        out.push(i)
                    }
                    out
                };
                Operation::Read {
                    filter,
                    order,
                    limit,
                    projection,
                }
            }
            Statement::Insert { values, .. } => {
                let a = self.assignments(values, true)?;
                let mut values = vec![Value::Null; self.table.columns.len()];
                for (i, v) in a {
                    values[i] = v
                }
                for (v, c) in values.iter().zip(&self.table.columns) {
                    check(v, c)?
                }
                Operation::Insert(values)
            }
            Statement::Update {
                filter,
                guard,
                values,
                intent,
                ..
            } => {
                if guard.is_some() && !matches!(intent, Intent::One) {
                    return Err(Error::Unsupported);
                }
                let filter = self.predicate(filter)?;
                let guard = self.optional(guard)?;
                let sets = self.assignments(values, false)?;
                let selection = match intent {
                    Intent::One => Selection::One,
                    Intent::Many { cap } => Selection::Many(self.count(cap)?),
                    Intent::Claim { order } => {
                        if order.is_empty() {
                            return Err(Error::Bind("claim order".into()));
                        }
                        Selection::Claim(self.order(order)?)
                    }
                };
                Operation::Mutate {
                    filter,
                    guard,
                    sets: Some(sets),
                    selection,
                }
            }
            Statement::Delete {
                filter, guard, cap, ..
            } => {
                if cap.is_some() && guard.is_some() {
                    return Err(Error::Unsupported);
                }
                Operation::Mutate {
                    filter: self.predicate(filter)?,
                    guard: self.optional(guard)?,
                    sets: None,
                    selection: match cap {
                        Some(v) => Selection::Many(self.count(v)?),
                        None => Selection::One,
                    },
                }
            }
        })
    }
}
enum Selection {
    One,
    Many(usize),
    Claim(Vec<(usize, bool)>),
}
enum Operation {
    Read {
        filter: Bound,
        order: Vec<(usize, bool)>,
        limit: usize,
        projection: Vec<usize>,
    },
    Insert(Vec<Value>),
    Mutate {
        filter: Bound,
        guard: Bound,
        sets: Option<Vec<(usize, Value)>>,
        selection: Selection,
    },
}
fn compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => unreachable!("bound comparable types"),
    }
}
fn truth(v: Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(b),
        Value::Null => None,
        _ => unreachable!("bound boolean"),
    }
}
fn boolval(v: Option<bool>) -> Value {
    v.map_or(Value::Null, Value::Bool)
}
impl Bound {
    fn eval(&self, row: &[Value]) -> Value {
        match self {
            Self::Value(v) => v.clone(),
            Self::Field(i) => row[*i].clone(),
            Self::Compare(a, op, b) => {
                let a = a.eval(row);
                let b = b.eval(row);
                if a == Value::Null || b == Value::Null {
                    return Value::Null;
                }
                let c = compare(&a, &b);
                Value::Bool(match op {
                    Compare::Eq => c == Ordering::Equal,
                    Compare::Ne => c != Ordering::Equal,
                    Compare::Lt => c == Ordering::Less,
                    Compare::Le => c != Ordering::Greater,
                    Compare::Gt => c == Ordering::Greater,
                    Compare::Ge => c != Ordering::Less,
                })
            }
            Self::IsNull(e, neg) => Value::Bool((e.eval(row) == Value::Null) != *neg),
            Self::Not(e) => boolval(truth(e.eval(row)).map(|b| !b)),
            Self::And(a, b) => {
                let a = truth(a.eval(row));
                let b = truth(b.eval(row));
                boolval(if a == Some(false) || b == Some(false) {
                    Some(false)
                } else if a == Some(true) && b == Some(true) {
                    Some(true)
                } else {
                    None
                })
            }
            Self::Or(a, b) => {
                let a = truth(a.eval(row));
                let b = truth(b.eval(row));
                boolval(if a == Some(true) || b == Some(true) {
                    Some(true)
                } else if a == Some(false) && b == Some(false) {
                    Some(false)
                } else {
                    None
                })
            }
        }
    }
    fn accepts(&self, row: &SqlRow) -> bool {
        self.eval(&row.values) == Value::Bool(true)
    }
}
fn sort(rows: &mut [SqlRow], order: &[(usize, bool)]) {
    rows.sort_by(|a, b| {
        for (i, desc) in order {
            let c = compare(&a.values[*i], &b.values[*i]);
            let c = if *desc { c.reverse() } else { c };
            if c != Ordering::Equal {
                return c;
            }
        }
        Ordering::Equal
    })
}
fn key_predicate(t: &TableDef, start: usize) -> String {
    t.primary_key
        .iter()
        .enumerate()
        .map(|(n, i)| format!("{} = ${}", t.columns[*i].name, n + start))
        .collect::<Vec<_>>()
        .join(" and ")
}
fn run(s: &mut SqlSession, t: &TableDef, op: Operation, rows: Vec<SqlRow>) -> Result<Outcome> {
    let mut out = Outcome::default();
    match op {
        Operation::Read {
            filter,
            order,
            limit,
            projection,
        } => {
            let mut rows: Vec<_> = rows.into_iter().filter(|r| filter.accepts(r)).collect();
            sort(&mut rows, &order);
            out.matched = rows.len();
            out.complete = rows.len() <= limit;
            out.columns = projection
                .iter()
                .map(|i| t.columns[*i].name.clone())
                .collect();
            out.rows = rows
                .into_iter()
                .take(limit)
                .map(|r| SqlRow {
                    version: r.version,
                    values: projection.iter().map(|i| r.values[*i].clone()).collect(),
                })
                .collect()
        }
        Operation::Insert(values) => {
            let names = t
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(", ");
            let params = (1..=values.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(", ");
            s.execute(
                &format!("insert into {} ({names}) values ({params})", t.name),
                &values,
                None,
            )
            .map_err(core)?;
            out.matched = 1;
            out.affected = 1;
            out.changed = 1;
        }
        Operation::Mutate {
            filter,
            guard,
            sets,
            selection,
        } => {
            let mut matches: Vec<_> = rows.into_iter().filter(|r| filter.accepts(r)).collect();
            match &selection {
                Selection::One => {
                    if matches.is_empty() {
                        return Err(Error::NotFound);
                    }
                    if matches.len() != 1 {
                        return Err(Error::AmbiguousMatch);
                    }
                }
                Selection::Many(cap) => {
                    if matches.len() > *cap {
                        return Err(Error::TooManyMatches);
                    }
                }
                Selection::Claim(order) => {
                    sort(&mut matches, order);
                    matches.truncate(1)
                }
            }
            out.matched = matches.len();
            if matches.iter().any(|r| !guard.accepts(r)) {
                return Err(Error::ConditionFailed);
            }
            let mut work = vec![];
            let mut claimed_key = None;
            if matches!(selection, Selection::Claim(_)) {
                out.columns = t.columns.iter().map(|c| c.name.clone()).collect();
                claimed_key = matches.first().map(|r| {
                    t.primary_key
                        .iter()
                        .map(|i| r.values[*i].clone())
                        .collect::<Vec<_>>()
                });
            }
            for row in matches {
                let mut post = row.values.clone();
                if let Some(sets) = &sets {
                    for (i, v) in sets {
                        post[*i] = v.clone()
                    }
                }
                if matches!(selection, Selection::Claim(_))
                    && filter.eval(&post) == Value::Bool(true)
                {
                    return Err(Error::ClaimNotConsumed);
                }
                out.affected += 1;
                if sets.is_some() && post == row.values {
                    continue;
                }
                out.changed += 1;
                let mut params = vec![];
                let sql = if let Some(sets) = &sets {
                    let setsql = sets
                        .iter()
                        .enumerate()
                        .map(|(n, (i, v))| {
                            params.push(v.clone());
                            format!("{} = ${}", t.columns[*i].name, n + 1)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "update {} set {} where {}",
                        t.name,
                        setsql,
                        key_predicate(t, params.len() + 1)
                    )
                } else {
                    format!("delete from {} where {}", t.name, key_predicate(t, 1))
                };
                params.extend(t.primary_key.iter().map(|i| row.values[*i].clone()));
                let p = s.prepare(&sql).map_err(core)?;
                s.describe(&p).map_err(core)?;
                work.push((p, params, row.version));
            }
            for (p, params, version) in work {
                let result = s
                    .execute_prepared(&p, &params, Some(version))
                    .map_err(core)?;
                if result.affected != 1 {
                    return Err(Error::Conflict);
                }
            }
            if let Some(key) = claimed_key {
                let result = s
                    .execute(
                        &format!("select * from {} where {}", t.name, key_predicate(t, 1)),
                        &key,
                        None,
                    )
                    .map_err(core)?;
                if result.rows.len() != 1 {
                    return Err(Error::Conflict);
                }
                out.rows = result.rows;
            }
        }
    }
    Ok(out)
}
