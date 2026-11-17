//! Bounded SQL multi-row INSERT and ON CONFLICT frontend.
use super::*;
use crate::operations::{self, ConflictAction};

#[derive(Debug, Clone)]
pub(super) struct Insert {
    pub table: String,
    alias: Option<String>,
    copy_input: Option<Vec<DataType>>,
    normalized: Option<Vec<Vec<Value>>>,
    source_types: Option<Vec<Option<DataType>>>,
    columns: Vec<String>,
    rows: Vec<Vec<Expr>>,
    casts: BTreeMap<(usize, usize), query::conflict::Expression>,
    defaults: Vec<Vec<bool>>,
    default_values: bool,
    overriding: Option<bool>,
    conflict: Option<Conflict>,
    pub(super) returning: Option<returning::Returning>,
    pub(super) source: Option<Box<query::Query>>,
}
/// Borrowed syntax needed by static lock classification, without expression internals.
pub(super) struct LockConflict<'a> {
    pub(super) columns: Option<&'a [String]>,
    pub(super) constraint: Option<&'a str>,
    pub(super) sets: Vec<&'a str>,
}
#[derive(Debug, Clone)]
struct Conflict {
    columns: Option<Vec<String>>,
    constraint: Option<String>,
    update: Option<Update>,
}
#[derive(Debug, Clone)]
struct Update {
    sets: Vec<(String, Operand)>,
    guard: Option<Condition>,
}
#[derive(Debug, Clone)]
enum Operand {
    Default,
    Scalar(query::conflict::Expression),
}
type Condition = query::conflict::Expression;

#[cfg(test)]
use operations::ConflictSource;
#[cfg(test)]
#[allow(non_snake_case)]
impl Operand {
    fn Value(value: Expr) -> Self {
        Self::Scalar(Condition::admission_value(value))
    }
    fn Field(source: ConflictSource, name: String) -> Self {
        Self::Scalar(Condition::admission_field(source, name))
    }
}
#[cfg(test)]
#[allow(non_snake_case)]
impl Condition {
    fn Compare(a: Operand, op: operations::Compare, b: Operand) -> Self {
        match (a, b) {
            (Operand::Scalar(a), Operand::Scalar(b)) => Self::admission_compare(a, op, b),
            _ => unreachable!("admission comparisons use scalar operands"),
        }
    }
}

/// Claim SELECT sources, multiple VALUES tuples or a top-level ON CONFLICT clause.
pub(super) fn claims(tokens: &[Token]) -> bool {
    if tokens.iter().any(
        |token| matches!(token, Token::Word(word) if word == "default" || word == "overriding"),
    ) {
        return true;
    }
    let mut depth = 0usize;
    let mut values_at = None;
    let mut has_values_clause = false;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::Word(word) if index >= 3 && word == "select" && !has_values_clause => {
                return true;
            }
            Token::Word(word) if depth == 0 && word == "on" => {
                if matches!(tokens.get(index + 1), Some(Token::Word(word)) if word == "conflict") {
                    return true;
                }
            }
            // INSERT INTO target occupies the first three tokens. The target
            // itself may legally be named values.
            Token::Word(word) if depth == 0 && index >= 3 && word == "values" => {
                values_at.get_or_insert(index);
                has_values_clause |= tokens.get(index - 1) != Some(&Token::Symbol('.'));
            }
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth = depth.saturating_sub(1),
            Token::Symbol(';') if depth == 0 => break,
            _ => {}
        }
    }
    let Some(mut at) = values_at.map(|index| index + 1) else {
        return false;
    };
    let mut tuples = 0usize;
    while tokens.get(at) == Some(&Token::Symbol('(')) {
        tuples += 1;
        if tuples >= 2 {
            return true;
        }
        depth = 1;
        at += 1;
        while depth != 0 {
            match tokens.get(at) {
                Some(Token::Symbol('(')) => depth += 1,
                Some(Token::Symbol(')')) => depth -= 1,
                Some(_) => {}
                None => return false,
            }
            at += 1;
        }
        if tokens.get(at) != Some(&Token::Symbol(',')) {
            break;
        }
        at += 1;
    }
    false
}

fn names(p: &mut Parser) -> R<Vec<String>> {
    p.punct('(')?;
    let mut names = vec![p.name()?];
    while p.sym(',') {
        names.push(p.name()?);
    }
    p.punct(')')?;
    Ok(names)
}
fn value(p: &mut Parser) -> R<Expr> {
    if matches!(
        p.tokens.get(p.at),
        None | Some(Token::Symbol(')' | ',' | ';'))
    ) {
        return Err(SqlError::Syntax);
    }
    p.expr()
}
fn unsupported_or_syntax(p: &Parser) -> SqlError {
    if matches!(p.tokens.get(p.at), None | Some(Token::Symbol(';' | ')'))) {
        SqlError::Syntax
    } else {
        SqlError::Unsupported
    }
}
pub(super) fn parse(p: &mut Parser) -> R<Insert> {
    p.need("insert")?;
    p.need("into")?;
    let table = p.table_name()?;
    let alias = if p.word("as") { Some(p.name()?) } else { None };
    let columns = if p.tokens.get(p.at) == Some(&Token::Symbol('('))
        && !matches!(p.tokens.get(p.at + 1), Some(Token::Word(word)) if word == "select")
        && p.tokens.get(p.at + 1) != Some(&Token::Symbol('('))
    {
        names(p)?
    } else {
        vec![]
    };
    let overriding = if p.word("overriding") {
        let system = if p.word("system") {
            true
        } else {
            p.need("user")?;
            false
        };
        p.need("value")?;
        Some(system)
    } else {
        None
    };
    let default_values = p.word("default");
    let mut rows = Vec::new();
    let mut casts = BTreeMap::new();
    let mut defaults = Vec::new();
    let source = if !default_values
        && (matches!(p.tokens.get(p.at), Some(Token::Word(word)) if word == "select")
            || p.tokens.get(p.at) == Some(&Token::Symbol('(')))
    {
        Some(Box::new(parse_source(p)?))
    } else {
        if !p.word("values") {
            return Err(unsupported_or_syntax(p));
        }
        None
    };
    if default_values {
        rows.push(vec![]);
        defaults.push(vec![]);
    } else if source.is_none() {
        loop {
            p.punct('(')?;
            let mut row = Vec::new();
            let mut row_defaults = Vec::new();
            loop {
                let default = p.word("default");
                row_defaults.push(default);
                row.push(if default {
                    Expr::Literal(Value::Null)
                } else {
                    let start = p.at;
                    let parameters = p.params.clone();
                    let plain = value(p);
                    if plain.is_err() || !matches!(p.tokens.get(p.at), Some(Token::Symbol(',' | ')'))) {
                        p.at = start;
                        p.params = parameters;
                        casts.insert(
                            (rows.len(), row.len()),
                            query::conflict::Expression::input(p)?,
                        );
                        Expr::Literal(Value::Null)
                    } else {
                        plain?
                    }
                });
                if !p.sym(',') {
                    break;
                }
            }
            if !p.sym(')') {
                return Err(unsupported_or_syntax(p));
            }
            rows.push(row);
            defaults.push(row_defaults);
            if rows.len() > MAX_INSERT_TOKENS {
                return Err(SqlError::Limit);
            }
            if !p.sym(',') {
                break;
            }
        }
    }
    let conflict = if p.word("on") {
        p.need("conflict")?;
        let mut columns = None;
        let mut constraint = None;
        if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
            columns = Some(names(p)?);
        } else if p.word("on") {
            p.need("constraint")?;
            constraint = Some(p.name()?);
        }
        p.need("do")?;
        let update = if p.word("nothing") {
            None
        } else {
            p.need("update")?;
            if columns.is_none() && constraint.is_none() {
                return Err(SqlError::Syntax);
            }
            p.need("set")?;
            let mut sets = Vec::new();
            loop {
                let tuple = p.tokens.get(p.at) == Some(&Token::Symbol('('));
                let targets = if tuple { names(p)? } else { vec![p.name()?] };
                if p.tokens.get(p.at) != Some(&Token::Op("=".into())) {
                    return Err(SqlError::Syntax);
                }
                p.at += 1;
                if tuple {
                    p.punct('(')?;
                }
                for (slot, column) in targets.iter().enumerate() {
                    if slot > 0 {
                        p.punct(',')?;
                    }
                    sets.push((column.clone(), operand(p)?));
                }
                if tuple {
                    p.punct(')')?;
                }
                if !p.sym(',') {
                    break;
                }
            }
            let guard = if p.word("where") {
                Some(query::conflict::Expression::condition(p)?)
            } else {
                None
            };
            Some(Update { sets, guard })
        };
        Some(Conflict {
            columns,
            constraint,
            update,
        })
    } else {
        None
    };
    let returning = returning::Returning::parse(p, &table)?;
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(unsupported_or_syntax(p));
    }
    Ok(Insert {
        copy_input: None,
        normalized: None,
        source_types: None,
        table,
        alias,
        columns,
        rows,
        casts,
        defaults,
        default_values,
        overriding,
        conflict,
        returning,
        source,
    })
}
fn parse_source(p: &mut Parser) -> R<query::Query> {
    let start = p.at;
    let mut end = start;
    let mut depth = 0usize;
    while let Some(token) = p.tokens.get(end) {
        match token {
            Token::Word(word) if depth == 0 && word == "returning" => break,
            Token::Word(word)
                if depth == 0
                    && word == "on"
                    && matches!(p.tokens.get(end + 1), Some(Token::Word(word)) if word == "conflict")
                    && (p.tokens.get(end + 2) == Some(&Token::Symbol('('))
                        || matches!(p.tokens.get(end + 2), Some(Token::Word(word)) if word == "do")
                        || matches!((p.tokens.get(end + 2), p.tokens.get(end + 3)),
                            (Some(Token::Word(on)), Some(Token::Word(constraint))) if on == "on" && constraint == "constraint")) =>
            {
                break;
            }
            Token::Symbol(';') if depth == 0 => break,
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth = depth.checked_sub(1).ok_or(SqlError::Syntax)?,
            _ => {}
        }
        end += 1;
    }
    if depth != 0 {
        return Err(SqlError::Syntax);
    }
    let mut tokens = p.tokens[start..end].to_vec();
    while tokens.first() == Some(&Token::Symbol('(')) {
        let mut nesting = 0usize;
        let wraps = tokens.iter().enumerate().all(|(index, token)| {
            match token {
                Token::Symbol('(') => nesting += 1,
                Token::Symbol(')') => nesting = nesting.saturating_sub(1),
                _ => {}
            }
            nesting != 0 || index + 1 == tokens.len()
        });
        if !wraps || tokens.last() != Some(&Token::Symbol(')')) {
            break;
        }
        tokens = tokens[1..tokens.len() - 1].to_vec();
    }
    let mut parser = Parser {
        tokens,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let source = query::Query::parse_insert_source(&mut parser)?;
    p.params.extend(parser.params);
    p.relations.extend(parser.relations);
    p.at = end;
    Ok(source)
}
fn operand(p: &mut Parser) -> R<Operand> {
    if p.word("default") {
        return Ok(Operand::Default);
    }
    query::conflict::Expression::parse(p).map(Operand::Scalar)
}

fn indexes(insert: &Insert, table: &TableDef) -> R<Vec<usize>> {
    let indexes = if insert.default_values {
        if !insert.columns.is_empty() {
            return Err(SqlError::Syntax);
        }
        vec![]
    } else if insert.source.is_some() || insert.rows.is_empty() {
        insert_indexes(table, &insert.columns)?
    } else {
        insert_value_indexes(table, &insert.columns, insert.rows.first().map_or(0, Vec::len))?
    };
    if insert.source.is_none() && insert.rows.iter().any(|row| row.len() != indexes.len()) {
        return Err(SqlError::Syntax);
    }
    Ok(indexes)
}
fn source_indexes(insert: &Insert, state: &Relational, table: &TableDef) -> R<Vec<usize>> {
    let mut indexes = indexes(insert, table)?;
    if let Some(source) = &insert.source {
        let width = source.insert_outputs(state)?.len();
        if width > indexes.len() || !insert.columns.is_empty() && width < indexes.len() {
            return Err(SqlError::Syntax);
        }
        indexes.truncate(width);
    }
    Ok(indexes)
}
impl Insert {
    /// Normalize once: probing must reuse defaults/identities consumed by execution.
    pub(super) fn witness_rows(
        &mut self, state: &Relational, params: &[Value], declared: &[Option<DataType>],
        clock: Option<&clock::ExecutionClock>,
    ) -> R<Option<Vec<Vec<Value>>>> {
        let table = state.tables.get(&self.table).ok_or(SqlError::UnknownTable)?;
        if self.rows.len() > 200 || self.source.is_some() || !self.can_witness(state) {
            return Ok(None);
        }
        let indexes = indexes(self, table)?;
        admit_statement(self, params)?;
        self.check_explicit(state, table, &indexes)?;
        let rows = self.rows.iter().zip(&self.defaults).enumerate().map(|(i, (row, defaults))| {
            normalize_row(state, table, self, &indexes, row, i, defaults, params,
                declared, clock, self.source_types.as_deref())
        }).collect::<R<Vec<_>>>()?;
        self.normalized = Some(rows.clone());
        Ok(Some(rows))
    }

    pub(super) fn can_witness(&self, state: &Relational) -> bool {
        state.tables.contains_key(&self.table) && self.conflict.is_some()
    }
    fn check_explicit(&self, state: &Relational, table: &TableDef, indexes: &[usize]) -> R<()> {
        if let Some(update) = self.conflict.as_ref().and_then(|c| c.update.as_ref()) {
            for (name, operand) in &update.sets {
                if !matches!(operand, Operand::Default) {
                    defaults::check_explicit(state, &table.columns[col(table, name)?])?;
                }
            }
        }
        for row_defaults in &self.defaults {
            for (index, default) in indexes.iter().zip(row_defaults) {
                if !default && self.overriding.is_none() {
                    defaults::check_explicit(state, &table.columns[*index])?;
                }
            }
        }
        Ok(())
    }
    pub(super) fn stable_referenced_keys(&self, state: &Relational, table: &TableDef) -> bool {
        self.conflict.as_ref().and_then(|c| c.update.as_ref()).is_none_or(|u| {
            u.sets.iter().all(|(name, _)| table.columns.iter().all(|c| {
                name != &c.name || !state.ext.constraints.values().flatten().any(|constraint| {
                    matches!(&constraint.kind, ConstraintKind::ForeignKey { ref_table, ref_columns, .. }
                        if *ref_table == table.id && ref_columns.contains(&c.id))
                })
            }))
        })
    }
    pub(super) fn lock_conflict(&self) -> Option<LockConflict<'_>> {
        let c = self.conflict.as_ref()?;
        Some(LockConflict {
            columns: c.columns.as_deref(),
            constraint: c.constraint.as_deref(),
            sets: c.update.as_ref().map(|u| u.sets.iter().map(|(name, _)| name.as_str()).collect())
                .unwrap_or_default(),
        })
    }
    /// Static VALUES admission plus the executor's exact assignment/coercion.
    /// None cells are omitted fresh PK defaults; no default is evaluated here.
    pub(super) fn lock_values(&self, state: &Relational, target: &[u64],
        params: &[Value], declared: &[Option<DataType>],
        clock: Option<&clock::ExecutionClock>,
    ) -> Option<Vec<Vec<Option<Value>>>> {
        let table = state.tables.get(&self.table)?;
        let indexes = indexes(self, table).ok()?;
        if self.overriding.is_some() { return None; }
        let needed = table.primary_key.iter().copied().chain(table.columns.iter()
            .enumerate().filter(|(_, c)| target.contains(&c.id)).map(|(i, _)| i))
            .collect::<BTreeSet<_>>();
        self.rows.iter().enumerate().map(|(row_index, row)| {
            let mut values = vec![None; table.columns.len()];
            for index in &needed {
                let column = &table.columns[*index];
                if let Some(slot) = indexes.iter().position(|i| i == index) {
                    if *self.defaults.get(row_index)?.get(slot)? { return None; }
                    if self.casts.get(&(row_index, slot))
                        .is_some_and(|cast| !cast.lock_input(&column.data_type)) { return None; }
                    let value = input_value(state, table, self, row_index, slot, *index,
                        row.get(slot)?, params, declared, clock).ok()?;
                    values[*index] = Some(coerce_to_column(state, column, value).ok()?);
                } else if target.contains(&column.id) {
                    if defaults::default_sql(state, column).is_some() { return None; }
                    values[*index] = Some(Value::Null);
                }
            }
            Some(values)
        }).collect()
    }
    pub(super) fn lock_supplies(&self, name: &str) -> bool {
        !self.default_values && self.columns.is_empty()
            || self.columns.iter().any(|column| column == name)
    }
    /// Probe classification only; the conflict planner still validates the target.
    pub(super) fn conflict_arbiter_columns(&self, state: &Relational,
        descriptors: &[secondary_index::Descriptor]) -> Option<BTreeSet<u64>> {
        let conflict = self.conflict.as_ref()?;
        conflict.update.as_ref()?;
        let table = state.tables.get(&self.table)?;
        if let Some(columns) = &conflict.columns {
            return columns.iter().map(|name| col(table, name).ok()
                .map(|index| table.columns[index].id)).collect();
        }
        let name = conflict.constraint.as_deref()?;
        if let Some(descriptor) = descriptors.iter().find(|d| d.table == table.id
            && d.unique && split_key(&d.name).1 == name) {
            return Some(descriptor.columns.iter().copied().collect());
        }
        (state.primary_key_constraint_name(table).as_deref() == Some(name))
            .then(|| table.primary_key.iter().map(|index| table.columns[*index].id).collect())
    }
    pub(super) fn updates_conflicts(&self) -> bool {
        self.conflict.as_ref().is_some_and(|c| c.update.is_some())
    }
    pub(super) fn append_only(&self) -> bool {
        self.conflict.is_none()
    }
    pub(super) fn needs_pages(&self) -> bool {
        self.rows.len() > 200 || self.source.is_some()
    }
    pub(super) fn batch(&self, start: usize, end: usize) -> Self {
        Self {
            rows: self.rows[start..end].to_vec(),
            defaults: self.defaults[start..end].to_vec(),
            casts: self
                .casts
                .range((start, 0)..(end, 0))
                .map(|((row, col), expr)| ((*row - start, *col), expr.clone()))
                .collect(),
            table: self.table.clone(),
            alias: self.alias.clone(),
            copy_input: self.copy_input.clone(),
            normalized: None,
            columns: self.columns.clone(),
            source_types: self.source_types.clone(),
            default_values: self.default_values,
            overriding: self.overriding,
            conflict: self.conflict.clone(),
            returning: self.returning.clone(),
            source: None,
        }
    }
    pub(super) fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub(super) fn source_page(
        &self,
        state: &Relational,
        params: &[Value],
        declared: &[Option<DataType>],
        clock: Option<&clock::ExecutionClock>,
    ) -> R<Self> {
        let materialized = materialize_source(
            self,
            state,
            params,
            declared,
            clock,
            &mut || crate::cancellation::check().is_err(),
            None,
            false,
        )?;
        let mut insert = materialized.insert;
        insert.source_types = Some(materialized.types);
        Ok(insert)
    }
    /// V2 non-pageable or self-reading source: run it once, before any page is
    /// inserted, into a spill-backed store. Returns a literal-row template.
    pub(super) fn source_store(
        &self,
        state: &Relational,
        params: &[Value],
        declared: &[Option<DataType>],
        clock: Option<&clock::ExecutionClock>,
        source: &dyn scan::RowSource,
    ) -> R<(Self, SqlResult, Option<spill::Store<SqlRow>>)> {
        let materialized = materialize_source(self, state, params, declared, clock,
            &mut || crate::cancellation::check().is_err(), Some(source), true)?;
        let mut insert = materialized.insert;
        insert.source_types = Some(materialized.types);
        let (result, store) = materialized.stored.unwrap_or_default();
        Ok((insert, result, store))
    }
    /// One page of stored source rows on the `source_store` template.
    pub(super) fn literal_page(&self, rows: Vec<SqlRow>) -> Self {
        let mut page = self.batch(0, 0);
        page.source_types.clone_from(&self.source_types);
        page.defaults = vec![vec![false; self.columns.len()]; rows.len()];
        page.rows = rows.into_iter()
            .map(|row| row.values.into_iter().map(Expr::Literal).collect()).collect();
        page
    }
    pub(super) fn sequence_dependencies(&self) -> Vec<Option<String>> {
        let mut dependencies = self
            .source
            .as_ref()
            .map_or_else(Vec::new, |source| source.sequence_dependencies());
        for expression in self.casts.values() {
            dependencies.extend(expression.sequence_dependencies());
        }
        if let Some(update) = self
            .conflict
            .as_ref()
            .and_then(|conflict| conflict.update.as_ref())
        {
            for (_, operand) in &update.sets {
                if let Operand::Scalar(expression) = operand {
                    dependencies.extend(expression.sequence_dependencies());
                }
            }
            if let Some(guard) = &update.guard {
                dependencies.extend(guard.sequence_dependencies());
            }
        }
        dependencies
    }
    pub(super) fn from_query(table: String, columns: Vec<String>, source: query::Query) -> Self {
        Self { table, columns, source: Some(Box::new(source)), alias: None,
            copy_input: None, normalized: None, source_types: None, rows: vec![],
            casts: BTreeMap::new(), defaults: vec![], default_values: false,
            overriding: None, conflict: None, returning: None }
    }
    pub(super) fn copy(
        table: String,
        columns: Vec<String>,
        types: Vec<DataType>,
        rows: Vec<Vec<Value>>,
    ) -> Self {
        let defaults = rows.iter().map(|row| vec![false; row.len()]).collect();
        Self {
            table,
            alias: None,
            columns,
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(Expr::Literal).collect())
                .collect(),
            defaults,
            casts: BTreeMap::new(),
            default_values: false,
            overriding: Some(true),
            conflict: None,
            returning: None,
            copy_input: Some(types),
            normalized: None,
            source_types: None,
            source: None,
        }
    }
    pub(super) fn replace_copy_rows(
        &mut self,
        rows: Vec<Vec<Value>>,
        clock: Option<&clock::ExecutionClock>,
    ) -> R<()> {
        let types = self.copy_input.as_ref().ok_or(SqlError::Unsupported)?;
        let rows = rows
            .into_iter()
            .map(|row| {
                if row.len() > types.len() {
                    return Err(SqlError::BadCopyFileFormat);
                }
                let missing = row.len() < types.len();
                let row = row.into_iter()
                    .zip(types)
                    .map(|(value, ty)| match value {
                        Value::Text(text) => sql_copy::copy_text_field(text, ty, clock),
                        other => Ok(other),
                    })
                    .collect::<R<Vec<_>>>()?;
                if missing {
                    return Err(SqlError::BadCopyFileFormat);
                }
                Ok(row)
            })
            .collect::<R<Vec<_>>>()?;
        *self = Self::copy(
            self.table.clone(),
            self.columns.clone(),
            types.clone(),
            rows,
        );
        Ok(())
    }

    pub(super) fn implicit_sequences(&self, state: &Relational) -> R<Vec<Option<String>>> {
        let table = state
            .tables
            .get(&self.table)
            .ok_or(SqlError::UnknownTable)?;
        let indexes = source_indexes(self, state, table)?;
        let mut dependencies = vec![];
        let source_defaults = self.source.as_ref().map(|_| vec![false; indexes.len()]);
        for row_defaults in self.defaults.iter().chain(source_defaults.iter()) {
            for (index, column) in table.columns.iter().enumerate() {
                if column.name == HIDDEN_ROWID {
                    continue;
                }
                let supplied = indexes.iter().position(|i| *i == index);
                let generates = supplied.is_none_or(|slot| row_defaults[slot])
                    || self.overriding == Some(false) && state.column_identity(column.id).is_some();
                if generates && let Some(sql) = defaults::default_sql(state, column) {
                    dependencies.extend(defaults::dependencies(&sql)?.into_iter().map(Some));
                }
            }
        }
        if let Some(update) = self
            .conflict
            .as_ref()
            .and_then(|conflict| conflict.update.as_ref())
        {
            for (name, value) in &update.sets {
                if matches!(value, Operand::Default) {
                    let column = &table.columns[col(table, name)?];
                    if let Some(sql) = defaults::default_sql(state, column) {
                        dependencies.extend(defaults::dependencies(&sql)?.into_iter().map(Some));
                    }
                }
            }
        }
        Ok(dependencies)
    }
    pub(super) fn has_duplicate_assignments(&self) -> bool {
        self.conflict
            .as_ref()
            .and_then(|conflict| conflict.update.as_ref())
            .is_some_and(|update| assign::duplicate_names(update.sets.iter().map(|(name, _)| name)))
    }
    pub(super) fn describe(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<Description> {
        let table = state
            .tables
            .get(&self.table)
            .ok_or(SqlError::UnknownTable)?;
        let indexes = source_indexes(self, state, table)?;
        let mut parameters = vec![None; count];
        if declared.len() > count {
            return Err(SqlError::Parameters);
        }
        parameters[..declared.len()].clone_from_slice(declared);
        for (row_index, row) in self.rows.iter().enumerate() {
            for (slot, (expression, index)) in row.iter().zip(&indexes).enumerate() {
                if let Some(expression) = self.casts.get(&(row_index, slot)) {
                    expression.infer(
                        table,
                        None,
                        &table.columns[*index].data_type,
                        &mut parameters,
                    )?;
                    continue;
                }
                infer_mutation_parameter(
                    expression,
                    &table.columns[*index].data_type,
                    &mut parameters,
                    declared,
                    true,
                )?;
            }
        }
        let mut duplicate = false;
        if let Some(Conflict {
            update: Some(update),
            ..
        }) = &self.conflict
        {
            duplicate = assign::duplicate_names(update.sets.iter().map(|(name, _)| name));
            for (name, operand) in &update.sets {
                let index = col(table, name)?;
                if let Operand::Scalar(expression) = operand {
                    defaults::check_explicit(state, &table.columns[index])?;
                    expression.infer(
                        table,
                        self.alias.as_deref(),
                        &table.columns[index].data_type,
                        &mut parameters,
                    )?;
                }
            }
            if let Some(guard) = &update.guard {
                guard.infer(
                    table,
                    self.alias.as_deref(),
                    &DataType::Boolean,
                    &mut parameters,
                )?;
            }
        }
        if let Some(source) = &self.source {
            let targets = indexes
                .iter()
                .map(|index| table.columns[*index].data_type.clone())
                .collect::<Vec<_>>();
            source.bind_insert_source(state, &targets, &mut parameters, declared)?;
            if self.overriding.is_none() {
                for index in &indexes {
                    defaults::check_explicit(state, &table.columns[*index])?;
                }
            }
        }
        if let Some(returning) = &self.returning {
            let (columns, parameters, typmods) = returning.describe(state, parameters)?;
            if duplicate {
                return Err(SqlError::Syntax);
            }
            return Ok(Description {
                type_oids: vec![],
                typmods,
                columns,
                parameters,
            });
        }
        if duplicate {
            return Err(SqlError::Syntax);
        }
        Ok(Description {
            type_oids: vec![],
            parameters: parameters
                .into_iter()
                .map(|ty| ty.ok_or(SqlError::Parameters))
                .collect::<R<Vec<_>>>()?,
            typmods: vec![],
            columns: vec![],
        })
    }
}
struct SourceRows {
    insert: Insert,
    types: Vec<Option<DataType>>,
    stored: Option<(SqlResult, Option<spill::Store<SqlRow>>)>,
}
#[expect(clippy::too_many_arguments, reason = "statement inputs plus an optional source and capture mode")]
fn materialize_source(
    insert: &Insert,
    state: &Relational,
    params: &[Value],
    declared: &[Option<DataType>],
    clock: Option<&clock::ExecutionClock>,
    cancel: &mut dyn FnMut() -> bool,
    injected_source: Option<&dyn scan::RowSource>,
    capture: bool,
) -> R<SourceRows> {
    let source = insert.source.as_ref().ok_or(SqlError::Unsupported)?;
    let table = state
        .tables
        .get(&insert.table)
        .ok_or(SqlError::UnknownTable)?;
    let indexes = source_indexes(insert, state, table)?;
    let targets = indexes
        .iter()
        .map(|index| table.columns[*index].data_type.clone())
        .collect::<Vec<_>>();
    let mut parameters = insert
        .describe(state, params.len(), declared)?
        .parameters
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    let bound = source
        .bind_insert_source(state, &targets, &mut parameters, declared)?
        .with_clock(clock);
    let columns = bound.description(state).columns;
    let unknown = source.insert_outputs(state)?;
    // Input conversion is a binding obligation even when WHERE produces no rows.
    for (value, target) in unknown.iter().zip(&targets) {
        if let Some(value) = value {
            query::assignment::literal(value.clone(), target, clock)?;
        }
    }
    let legacy = scan::LegacyRows(state);
    let source = injected_source.unwrap_or(&legacy);
    let (result, stored) = if capture {
        let (result, store) = bound.execute_insert_source_captured(state, source, params, cancel, declared)?;
        (SqlResult::default(), Some((result, store)))
    } else {
        (bound.execute_insert_source(state, source, params, cancel, declared)?, None)
    };
    if result.rows.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    let mut materialized = insert.clone();
    materialized.source = None;
    materialized.columns = indexes
        .iter()
        .map(|index| table.columns[*index].name.clone())
        .collect();
    materialized.rows = result
        .rows
        .into_iter()
        .map(|row| row.values.into_iter().map(Expr::Literal).collect())
        .collect();
    materialized.defaults = vec![vec![false; indexes.len()]; materialized.rows.len()];
    let types = columns
        .into_iter()
        .zip(unknown)
        .map(|(column, unknown)| unknown.is_none().then_some(column.data_type))
        .collect();
    Ok(SourceRows {
        insert: materialized,
        types,
        stored,
    })
}
/// The executor and pre-lock probe share input assignment, without defaults.
#[allow(clippy::too_many_arguments)]
fn input_value(
    state: &Relational, table: &TableDef, insert: &Insert, row_index: usize,
    slot: usize, index: usize, expression: &Expr, params: &[Value],
    declared: &[Option<DataType>], clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    if let Some(expression) = insert.casts.get(&(row_index, slot)) {
        let parameters = insert.describe(state, params.len(), declared)?.parameters
            .into_iter().map(Some).collect::<Vec<_>>();
        let bound = expression.bind(table, None, &table.columns[index].data_type, &parameters, clock)?;
        bound.evaluate(&SqlRow { values: vec![], version: 0 }, &[], params, &mut 0)
    } else if matches!(expression, Expr::Param(n) if declared.get(n - 1).is_some_and(Option::is_some)) {
        assign::plain_value(state, expression, &table.columns[index], declared, params, clock)
    } else {
        resolved_value(expression, params, &table.columns[index].data_type, clock)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "shared INSERT row normalization inputs"
)]
fn normalize_row(
    state: &Relational,
    table: &TableDef,
    insert: &Insert,
    indexes: &[usize],
    values: &[Expr],
    row_index: usize,
    row_defaults: &[bool],
    params: &[Value],
    declared: &[Option<DataType>],
    clock: Option<&clock::ExecutionClock>,
    source_types: Option<&[Option<DataType>]>,
) -> R<Vec<Value>> {
    let hidden_rowid = table.hidden_rowid();
    let replaced = |index: usize, default: bool| {
        default
            || insert.overriding == Some(false)
                && state.column_identity(table.columns[index].id).is_some()
    };
    let supplied = indexes
        .iter()
        .zip(row_defaults)
        .filter_map(|(i, default)| (!replaced(*i, *default)).then_some(*i))
        .collect::<Vec<_>>();
    let mut row = defaults::row(state, table, &supplied, clock)?;
    for (slot, ((expression, index), default)) in
        values.iter().zip(indexes).zip(row_defaults).enumerate()
    {
        if !replaced(*index, *default) {
            row[*index] = if let Some(Some(source_type)) =
                source_types.and_then(|types| types.get(slot))
            {
                // Allocate defaults in input order before a row's assignment can fail.
                query::assignment::convert(
                    expression.value(params)?,
                    source_type,
                    &table.columns[*index].data_type,
                    clock,
                )?
            } else {
                input_value(state, table, insert, row_index, slot, *index,
                    expression, params, declared, clock)?
            };
        }
    }
    for (value, column) in row.iter_mut().zip(&table.columns) {
        *value = coerce_to_column(state, column, value.clone())?;
    }
    Relational::secondary_admit_values(row.iter())?;
    if let Some(hidden) = hidden_rowid {
        // The NULL placeholder has the same fixed byte budget as its future Int.
        // Skip only its check until a version supplies the hidden rowid.
        let mut size = 0usize;
        for (index, (value, column)) in row.iter().zip(&table.columns).enumerate() {
            if index != hidden {
                check(value, column)?;
            }
            size = size
                .checked_add(value_bytes(value))
                .ok_or(SqlError::Limit)?;
        }
        if size > MAX_ROW_BYTES {
            return Err(SqlError::Limit);
        }
    } else {
        row_check(table, &row)?;
    }
    if insert.copy_input.is_some() {
        state.check_insert_candidate(table, &row)?;
    }
    Ok(row)
}

pub(super) fn normalize_copy(
    state: &Relational,
    prepared: &Prepared,
    rows: Vec<Vec<Value>>,
    clock: Option<&clock::ExecutionClock>,
) -> R<(Prepared, Vec<Vec<Value>>)> {
    let mut prepared = prepared.copy_rows(rows, clock)?;
    if prepared.epoch != state.epoch {
        return Err(SqlError::StaleSchema);
    }
    let Statement::InsertMulti(insert) = &prepared.statement else {
        return Err(SqlError::Unsupported);
    };
    admit_statement(insert, &[])?;
    let table = state
        .tables
        .get(&insert.table)
        .ok_or(SqlError::UnknownTable)?;
    let indexes = indexes(insert, table)?;
    let mut output = Vec::with_capacity(insert.rows.len());
    for (row_index, (row, defaults)) in insert.rows.iter().zip(&insert.defaults).enumerate() {
        if crate::cancellation::check().is_err() {
            return Err(SqlError::Aborted);
        }
        let full = normalize_row(
            state,
            table,
            insert,
            &indexes,
            row,
            row_index,
            defaults,
            &[],
            &[],
            clock,
            None,
        )?;
        output.push(
            table
                .logical_offsets()
                .into_iter()
                .map(|i| full[i].clone())
                .collect(),
        );
    }
    let columns = table.logical_columns().map(|c| c.name.clone()).collect();
    let types = table
        .logical_columns()
        .map(|c| c.data_type.clone())
        .collect();
    prepared.statement = Statement::InsertMulti(Box::new(Insert::copy(
        table.name.clone(),
        columns,
        types,
        vec![],
    )));
    Ok((prepared, output))
}

fn resolved_value(
    expression: &Expr,
    params: &[Value],
    ty: &DataType,
    clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    let value = expression.value(params)?;
    if matches!(expression, Expr::Literal(_)) {
        query::assignment::literal(value, ty, clock)
    } else {
        Ok(value)
    }
}
enum BoundOperand {
    Default(Option<String>),
    Scalar(Box<query::conflict::BoundExpression>),
}
struct ConflictPlan<'a> {
    values: Vec<(usize, BoundOperand)>,
    guard: Option<query::conflict::BoundExpression>,
    params: &'a [Value],
    clock: Option<clock::ExecutionClock>,
}
impl<'a> ConflictPlan<'a> {
    fn new(
        update: &Update,
        alias: Option<&str>,
        table: &TableDef,
        state: &Relational,
        declared: &[Option<DataType>],
        params: &'a [Value],
        clock: Option<&clock::ExecutionClock>,
    ) -> R<Self> {
        let mut parameters = declared.to_vec();
        // Execution receives the fully inferred prepared parameter types.
        if parameters.len() != params.len() {
            return Err(SqlError::Parameters);
        }
        for (name, operand) in &update.sets {
            if let Operand::Scalar(expression) = operand {
                expression.infer(
                    table,
                    alias,
                    &table.columns[col(table, name)?].data_type,
                    &mut parameters,
                )?;
            }
        }
        if let Some(guard) = &update.guard {
            guard.infer(table, alias, &DataType::Boolean, &mut parameters)?;
        }
        let values = update
            .sets
            .iter()
            .map(|(name, operand)| {
                let index = col(table, name)?;
                let column = &table.columns[index];
                let value = match operand {
                    Operand::Default => BoundOperand::Default(defaults::default_sql(state, column)),
                    Operand::Scalar(expression) => BoundOperand::Scalar(Box::new(
                        expression.bind(table, alias, &column.data_type, &parameters, clock)?,
                    )),
                };
                Ok((index, value))
            })
            .collect::<R<Vec<_>>>()?;
        let guard = update
            .guard
            .as_ref()
            .map(|guard| guard.bind(table, alias, &DataType::Boolean, &parameters, clock))
            .transpose()?;
        Ok(Self {
            values,
            guard,
            params,
            clock: clock.cloned(),
        })
    }
}
impl operations::SqlConflictEvaluation for ConflictPlan<'_> {
    fn guard(&self, old: &SqlRow, incoming: &[Value]) -> R<bool> {
        self.guard.as_ref().map_or(Ok(true), |guard| {
            Ok(guard.evaluate(old, incoming, self.params, &mut 0)? == Value::Bool(true))
        })
    }
    fn values(&self, table: &TableDef, old: &SqlRow, incoming: &[Value]) -> R<Vec<(usize, Value)>> {
        let mut bytes = 0;
        let values = self
            .values
            .iter()
            .map(|(index, value)| {
                let value = match value {
                    BoundOperand::Default(Some(sql)) => defaults::evaluate_sql(
                        sql,
                        &table.columns[*index].data_type,
                        self.clock.as_ref(),
                    )?,
                    BoundOperand::Default(None) => Value::Null,
                    BoundOperand::Scalar(value) => {
                        value.evaluate(old, incoming, self.params, &mut bytes)?
                    }
                };
                check(&value, &table.columns[*index])?;
                Ok((*index, value))
            })
            .collect::<R<Vec<_>>>()?;
        Relational::secondary_admit_values(values.iter().map(|(_, value)| value))?;
        payload_bound(values.iter().map(|(_, value)| value))?;
        Ok(values)
    }
}

/// Allocation-free raw byte admission with secondary_admit_values work accounting.
/// Rows have separate byte budgets; the whole input shares byte/work budgets.
#[derive(Default)]
struct InputAdmission {
    total: usize,
    row_bytes: usize,
    work: usize,
}
impl InputAdmission {
    fn row(&mut self) {
        self.row_bytes = 0;
    }
    fn value(&mut self, value: &Value) -> R<()> {
        let raw = match value {
            Value::Text(value) => value.len(),
            Value::Bytes(value) => value.len(),
            Value::Numeric(value) => value.display_byte_bound(),
            Value::Interval(_) => 16,
            Value::TimeTz(_) => 12,
            _ => 9,
        };
        self.total = self.total.checked_add(raw).ok_or(SqlError::Limit)?;
        self.row_bytes = self.row_bytes.checked_add(raw).ok_or(SqlError::Limit)?;
        self.work = self.work.checked_add(raw).ok_or(SqlError::Limit)?;
        if matches!(value, Value::Numeric(_)) {
            let numeric_work = raw
                .checked_mul(raw)
                .and_then(|work| work.checked_mul(8))
                .ok_or(SqlError::Limit)?;
            self.work = self.work.checked_add(numeric_work).ok_or(SqlError::Limit)?;
        }
        if self.row_bytes > MAX_ROW_BYTES
            || self.total > operations::MAX_INSERT_BYTES
            || self.work > secondary_index::MAX_WORK
        {
            return Err(SqlError::Limit);
        }
        Ok(())
    }
}
fn borrowed_value<'a>(expression: &'a Expr, params: &'a [Value]) -> R<&'a Value> {
    match expression {
        Expr::Literal(value) => Ok(value),
        Expr::Param(number) => params.get(number - 1).ok_or(SqlError::Parameters),
    }
}
fn admit_operand(operand: &Operand, params: &[Value], admission: &mut InputAdmission) -> R<()> {
    if let Operand::Scalar(expression) = operand {
        expression.visit_values(params, &mut |value| admission.value(value))?;
    }
    Ok(())
}
fn admit_statement(insert: &Insert, params: &[Value]) -> R<()> {
    if insert.rows.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    if insert.columns.len() > MAX_COLUMNS {
        return Err(SqlError::Type);
    }
    let mut admission = InputAdmission::default();
    for (row_index, row) in insert.rows.iter().enumerate() {
        if row.len() > MAX_COLUMNS
            || !insert.columns.is_empty() && row.len() != insert.columns.len()
        {
            return Err(SqlError::Syntax);
        }
        admission.row();
        for (slot, expression) in row.iter().enumerate() {
            if let Some(expression) = insert.casts.get(&(row_index, slot)) {
                expression.visit_values(params, &mut |value| admission.value(value))?;
            } else {
                admission.value(borrowed_value(expression, params)?)?;
            }
        }
    }
    if let Some(Conflict {
        update: Some(update),
        ..
    }) = &insert.conflict
    {
        if update.sets.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        admission.row();
        for (_, operand) in &update.sets {
            admit_operand(operand, params, &mut admission)?;
        }
        if let Some(condition) = &update.guard {
            condition.visit_values(params, &mut |value| admission.value(value))?;
        }
    }
    Ok(())
}
fn admit_engine_expression(
    expression: &operations::Expr,
    admission: &mut InputAdmission,
    depth: usize,
) -> R<()> {
    if depth > operations::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    match expression {
        operations::Expr::Value(operations::ValueExpr::Literal(value)) => admission.value(value)?,
        operations::Expr::Value(operations::ValueExpr::Parameter(_)) => {
            return Err(SqlError::Parameters);
        }
        operations::Expr::Field(_) | operations::Expr::ConflictField { .. } => {}
        operations::Expr::Compare(left, _, right)
        | operations::Expr::And(left, right)
        | operations::Expr::Or(left, right) => {
            admit_engine_expression(left, admission, depth + 1)?;
            admit_engine_expression(right, admission, depth + 1)?;
        }
        operations::Expr::IsNull(inner, _) | operations::Expr::Not(inner) => {
            admit_engine_expression(inner, admission, depth + 1)?
        }
        _ => return Err(SqlError::Unsupported),
    }
    Ok(())
}
impl Relational {
    /// Re-admit the full owned proposals before the immutable planner snapshot.
    pub(crate) fn sql_admit_conflict_input(
        table: &TableDef,
        incoming: &[Vec<Value>],
        action: &ConflictAction,
    ) -> R<()> {
        if incoming.len() > MAX_ROWS {
            return Err(SqlError::Limit);
        }
        let mut admission = InputAdmission::default();
        let logical_width = table.logical_columns().count();
        for row in incoming {
            if logical_width > MAX_COLUMNS || row.len() != table.columns.len() {
                return Err(SqlError::Type);
            }
            admission.row();
            for value in row {
                admission.value(value)?;
            }
        }
        if let ConflictAction::Update { guard, values } = action {
            if values.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            admission.row();
            for (_, expression) in values {
                admit_engine_expression(expression, &mut admission, 0)?;
            }
            if let Some(expression) = guard {
                admit_engine_expression(expression, &mut admission, 0)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    static ADMISSION_EVENTS: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
}
#[cfg(test)]
impl Relational {
    pub(crate) fn sql_insert_test_reset() {
        ADMISSION_EVENTS.set([0; 5]);
    }
    /// Table clone, incoming materialization, state clone, planner, publisher.
    pub(crate) fn sql_insert_test_counts() -> [usize; 5] {
        ADMISSION_EVENTS.get()
    }
    pub(crate) fn sql_insert_test_event(event: usize) {
        ADMISSION_EVENTS.with(|events| {
            let mut counts = events.get();
            counts[event] += 1;
            events.set(counts);
        });
    }
}

#[cfg(test)]
pub(super) fn execute(
    state: &mut Relational,
    insert: &Insert,
    params: &[Value],
    expected: Option<u64>,
    returning: &Option<returning::Plan<'_>>,
    clock: Option<&clock::ExecutionClock>,
    cancel: &mut dyn FnMut() -> bool,
) -> R<SqlResult> {
    execute_declared(
        state,
        insert,
        params,
        expected,
        returning,
        clock,
        cancel,
        &[],
        &mut Vec::new(),
        None,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "statement inputs plus clock, cancel and client-declared parameter types"
)]
pub(super) fn execute_declared(
    state: &mut Relational,
    insert: &Insert,
    params: &[Value],
    expected: Option<u64>,
    returning: &Option<returning::Plan<'_>>,
    clock: Option<&clock::ExecutionClock>,
    cancel: &mut dyn FnMut() -> bool,
    declared: &[Option<DataType>],
    events: &mut Vec<constraints::Event>,
    injected_source: Option<&dyn scan::RowSource>,
) -> R<SqlResult> {
    if cancel() {
        return Err(SqlError::Aborted);
    }
    let materialized;
    let (insert, source_types) = if insert.source.is_some() {
        if expected.is_some() {
            return Err(SqlError::Unsupported);
        }
        materialized = materialize_source(
            insert,
            state,
            params,
            declared,
            clock,
            cancel,
            injected_source,
            false,
        )?;
        (&materialized.insert, Some(materialized.types.as_slice()))
    } else {
        (insert, insert.source_types.as_deref())
    };
    admit_statement(insert, params)?;
    if cancel() {
        return Err(SqlError::Aborted);
    }
    if expected.is_some() {
        return Err(SqlError::Unsupported);
    }
    let table = state
        .tables
        .get(&insert.table)
        .ok_or(SqlError::UnknownTable)?;
    let indexes = indexes(insert, table)?;
    #[cfg(test)]
    Relational::sql_insert_test_event(0);
    let table = table.clone();
    let hidden_rowid = table.hidden_rowid();
    #[cfg(test)]
    Relational::sql_insert_test_event(1);
    let mut incoming = Vec::with_capacity(insert.rows.len());
    let mut total = 0usize;
    insert.check_explicit(state, &table, &indexes)?;
    for (row_index, (values, row_defaults)) in insert.rows.iter().zip(&insert.defaults).enumerate()
    {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        let row = if let Some(rows) = &insert.normalized { rows[row_index].clone() } else { normalize_row(
            state,
            &table,
            insert,
            &indexes,
            values,
            row_index,
            row_defaults,
            params,
            declared,
            clock,
            source_types,
        )? };
        for value in &row {
            total = total
                .checked_add(value_bytes(value))
                .ok_or(SqlError::Limit)?;
        }
        if total > MAX_RESULT_BYTES {
            return Err(SqlError::Limit);
        }
        incoming.push(row);
    }
    if let Some(conflict) = &insert.conflict {
        let parameters = insert
            .describe(state, params.len(), declared)?
            .parameters
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let evaluation = conflict
            .update
            .as_ref()
            .map(|update| {
                ConflictPlan::new(
                    update,
                    insert.alias.as_deref(),
                    &table,
                    state,
                    &parameters,
                    params,
                    clock,
                )
            })
            .transpose()?;
        // The syntax-independent action carries only the assignment names. SQL
        // scalar evaluation is prebound separately, outside the wire types.
        let action = match &conflict.update {
            None => ConflictAction::Nothing,
            Some(update) => ConflictAction::Update {
                guard: None,
                values: update
                    .sets
                    .iter()
                    .map(|(name, _)| {
                        (
                            name.clone(),
                            operations::Expr::Value(operations::ValueExpr::Literal(Value::Null)),
                        )
                    })
                    .collect(),
            },
        };
        let mut result = if returning.is_some() {
            state.sql_conflict_engine_captured(
                &table,
                (conflict.columns.as_deref(), conflict.constraint.as_deref()),
                &incoming,
                &action,
                evaluation
                    .as_ref()
                    .map(|plan| plan as &dyn operations::SqlConflictEvaluation),
                Some(&mut |source, rows| returning::project(returning, source, params, rows)),
                cancel,
                events,
            )?
        } else {
            state.sql_conflict_engine_captured(
                &table,
                (conflict.columns.as_deref(), conflict.constraint.as_deref()),
                &incoming,
                &action,
                evaluation
                    .as_ref()
                    .map(|plan| plan as &dyn operations::SqlConflictEvaluation),
                None,
                cancel,
                events,
            )?
        };
        let rows = std::mem::take(&mut result.rows);
        returning::attach(returning, &mut result, rows);
        return Ok(result);
    }
    let mut next_version = state.next_version_engine();
    // Heap tables take their hidden row id from a version allocated in VALUES order.
    let mut proposed = if let Some(hidden) = hidden_rowid {
        incoming
            .into_iter()
            .enumerate()
            .map(|(input, mut values)| {
                let version = allocate(&mut next_version)?;
                values[hidden] =
                    Value::Int(i64::try_from(version).map_err(|_| SqlError::Overflow)?);
                Ok((pk(&table, &values)?, SqlRow { values, version }, input))
            })
            .collect::<R<Vec<_>>>()?
    } else {
        incoming
            .into_iter()
            .enumerate()
            .map(|(input, values)| Ok((pk(&table, &values)?, SqlRow { values, version: 0 }, input)))
            .collect::<R<Vec<_>>>()?
    };
    // Project RETURNING in VALUES order, after hidden row ids exist and before the key sort.
    let mut returned = if returning.is_some() {
        returning::project(
            returning,
            state,
            params,
            &proposed.iter().map(|(_, row, _)| row).collect::<Vec<_>>(),
        )?
    } else {
        vec![]
    };
    proposed.sort_by(|left, right| left.0.cmp(&right.0));
    if hidden_rowid.is_none() {
        for (_, row, _) in &mut proposed {
            row.version = allocate(&mut next_version)?;
        }
    }
    for (_, row, input) in &proposed {
        if let Some(projected) = returned.get_mut(*input) {
            projected.version = row.version;
        }
    }
    let changes = proposed
        .iter()
        .map(|(key, row, _)| BatchRowChange {
            table: table.id,
            old: None,
            new: Some((key, row)),
        })
        .collect::<Vec<_>>();
    #[cfg(test)]
    Relational::sql_insert_test_event(4);
    let mut result =
        state.secondary_apply_batch_engine(state.epoch, &changes, next_version, cancel)?;
    returning::attach(returning, &mut result, returned);
    Ok(result)
}

#[cfg(test)]
#[path = "sql_insert_tests.rs"]
mod sql_insert_tests;
