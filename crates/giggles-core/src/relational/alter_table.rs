//! Bounded ALTER actions, staged together before catalog publication.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct AlterTable {
    pub table: String,
    pub actions: Vec<Action>,
    pub resolution_error: Option<SqlError>,
}
#[derive(Debug, Clone)]
pub(super) enum Action {
    Add(ColumnDef, Option<i32>, Option<String>, Option<SqlError>),
    Drop(String, bool),
    RenameColumn(String, String),
    RenameTable(String),
    Default(String, Option<String>),
    NotNull(String, bool),
    Unique(Option<String>, Vec<String>),
    PrimaryKey(Option<String>, Vec<String>),
    Constraint(constraints::Definition),
    Validate(String),
    DropConstraint(String, bool),
}
impl AlterTable {
    pub fn sequence_dependencies(&self) -> Vec<Option<String>> {
        self.actions
            .iter()
            .filter_map(|action| match action {
                Action::Add(_, _, Some(sql), _) | Action::Default(_, Some(sql)) => Some(sql),
                _ => None,
            })
            .flat_map(|sql| defaults::dependencies(sql).unwrap_or_default())
            .map(Some)
            .collect()
    }
}
pub(super) fn parse(p: &mut Parser) -> R<AlterTable> {
    p.need("alter")?;
    p.need("table")?;
    p.word("only");
    // Keep relation policy out of parsing so syntax wins over lookup and authority.
    let first = raw_name(p)?;
    let (table, resolution_error) = if p.sym('.') {
        let table = raw_name(p)?;
        let error = if first != "public" {
            Some(SqlError::UnknownTable)
        } else {
            ident::ident(&table).err()
        };
        (table, error)
    } else {
        let error = ident::ident(&first).err();
        (first, error)
    };
    let mut actions = Vec::new();
    loop {
        if p.at == p.tokens.len() || matches!(p.tokens.get(p.at), Some(Token::Symbol(';'))) {
            return Err(SqlError::Syntax);
        }
        let action = if p.word("add") {
            let constraint = if p.word("constraint") {
                Some(raw_name(p)?)
            } else {
                None
            };
            if p.word("check") {
                Action::Constraint(constraints::parse_check(p, constraint)?)
            } else if p.word("foreign") {
                p.need("key")?;
                let columns = raw_names(p)?;
                p.need("references")?;
                Action::Constraint(constraints::parse_foreign(p, constraint, columns)?)
            } else if p.word("primary") {
                p.need("key")?;
                Action::PrimaryKey(constraint, raw_names(p)?)
            } else if p.word("unique") {
                Action::Unique(constraint, raw_names(p)?)
            } else {
                if constraint.is_some() {
                    if p.at == p.tokens.len()
                        || matches!(p.tokens.get(p.at), Some(Token::Symbol(';' | ',')))
                    {
                        return Err(SqlError::Syntax);
                    }
                    return Err(SqlError::Unsupported);
                }
                let explicit_column = p.word("column");
                if !explicit_column && (p.word("check") || p.word("primary") || p.word("foreign")) {
                    return Err(SqlError::Unsupported);
                }
                let name = raw_name(p)?;
                if !matches!(p.tokens.get(p.at), Some(Token::Word(_))) {
                    return Err(SqlError::Syntax);
                }
                let mut data_type = defaults::cast_type(p)?;
                let mut semantic_error = None;
                let mut typmod = if p.sym('(') {
                    let first = integer(p)?;
                    let value = if data_type == DataType::Numeric {
                        let scale = if p.sym(',') { integer(p)? } else { 0 };
                        match giggles_pg_types::typmod::NumericTypmod::new(first, scale) {
                            Ok(value) => value.to_typmod(),
                            Err(_) => {
                                semantic_error = Some(SqlError::InvalidParameterValue);
                                0
                            }
                        }
                    } else {
                        first
                    };
                    p.punct(')')?;
                    Some(if data_type == DataType::Interval {
                        (32767 << 16) | value
                    } else {
                        value
                    })
                } else {
                    None
                };
                if matches!(data_type, DataType::Time | DataType::Timestamp) {
                    let with = p.word("with");
                    if with || p.word("without") {
                        p.need("time")?;
                        p.need("zone")?;
                        if with {
                            data_type = if data_type == DataType::Time {
                                DataType::TimeTz
                            } else {
                                DataType::TimestampTz
                            };
                        }
                    }
                }
                if data_type == DataType::Interval {
                    let field = |p: &mut Parser| -> Option<i32> {
                        for (word, mask) in [
                            ("year", 4),
                            ("month", 2),
                            ("day", 8),
                            ("hour", 1024),
                            ("minute", 2048),
                            ("second", 4096),
                        ] {
                            if p.word(word) {
                                return Some(mask);
                            }
                        }
                        None
                    };
                    if let Some(first) = field(p) {
                        let mask = if p.word("to") {
                            let last = field(p).ok_or(SqlError::Unsupported)?;
                            match (first, last) {
                                (4, 2) => 6,
                                (8, 1024) => 1032,
                                (8, 2048) => 3080,
                                (8, 4096) => 7176,
                                (1024, 2048) => 3072,
                                (1024, 4096) => 7168,
                                (2048, 4096) => 6144,
                                _ => return Err(SqlError::InvalidParameterValue),
                            }
                        } else {
                            first
                        };
                        if typmod.is_some() {
                            return Err(SqlError::Unsupported);
                        }
                        let precision = if p.sym('(') {
                            let value = integer(p)?;
                            p.punct(')')?;
                            value
                        } else {
                            65535
                        };
                        let value = (mask << 16) | precision;
                        typmod = Some(value);
                    }
                }
                let mut not_null = false;
                let mut default = None;
                loop {
                    if p.word("not") {
                        p.need("null")?;
                        not_null = true;
                    } else if p.word("default") {
                        if default.is_some() {
                            return Err(SqlError::Syntax);
                        }
                        default = Some(defaults::parse_alter_untyped(p)?);
                    } else {
                        break;
                    }
                }
                Action::Add(
                    ColumnDef {
                        id: 0,
                        name,
                        data_type,
                        not_null,
                    },
                    typmod,
                    default,
                    semantic_error,
                )
            }
        } else if p.word("drop") {
            if p.word("constraint") {
                let optional = p.word("if");
                if optional {
                    p.need("exists")?;
                }
                Action::DropConstraint(raw_name(p)?, optional)
            } else {
                p.word("column");
                let optional = p.word("if");
                if optional {
                    p.need("exists")?;
                }
                Action::Drop(raw_name(p)?, optional)
            }
        } else if p.word("rename") {
            if p.word("to") {
                Action::RenameTable(raw_name(p)?)
            } else {
                p.word("column");
                let name = raw_name(p)?;
                p.need("to")?;
                Action::RenameColumn(name, raw_name(p)?)
            }
        } else if p.word("alter") {
            p.word("column");
            let name = raw_name(p)?;
            let set = if p.word("set") {
                true
            } else if p.word("drop") {
                false
            } else if p.word("type") {
                return Err(SqlError::Unsupported);
            } else {
                return Err(SqlError::Syntax);
            };
            if p.word("default") {
                // The type is resolved during execution; expression text retains its own casts.
                let sql = if set {
                    Some(defaults::parse_alter_untyped(p)?)
                } else {
                    None
                };
                Action::Default(name, sql)
            } else if p.word("not") {
                p.need("null")?;
                Action::NotNull(name, set)
            } else {
                return Err(SqlError::Syntax);
            }
        } else if p.word("validate") {
            p.need("constraint")?;
            Action::Validate(raw_name(p)?)
        } else if p.word("set") {
            return Err(SqlError::Unsupported);
        } else {
            return Err(SqlError::Syntax);
        };
        actions.push(action);
        if !p.sym(',') {
            break;
        }
    }
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(AlterTable {
        table,
        actions,
        resolution_error,
    })
}
pub(super) fn raw_names(p: &mut Parser) -> R<Vec<String>> {
    p.punct('(')?;
    let mut names = vec![raw_name(p)?];
    while p.sym(',') {
        names.push(raw_name(p)?);
    }
    p.punct(')')?;
    Ok(names)
}
pub(super) fn raw_name(p: &mut Parser) -> R<String> {
    match p.tokens.get(p.at) {
        Some(Token::Word(value) | Token::QuotedIdent(value)) => {
            let value = ident::truncate(value, 63).to_owned();
            p.at += 1;
            Ok(value)
        }
        _ => Err(SqlError::Syntax),
    }
}
fn integer(p: &mut Parser) -> R<i32> {
    match p.expr()? {
        Expr::Literal(Value::Int(n)) => n.try_into().map_err(|_| SqlError::InvalidParameterValue),
        _ => Err(SqlError::Syntax),
    }
}
fn position(table: &TableDef, name: &str) -> R<usize> {
    if name == HIDDEN_ROWID {
        return Err(SqlError::Reserved);
    }
    col(table, name)
}
pub(super) fn execute(
    state: &mut Relational,
    alter: &AlterTable,
    clock: Option<&clock::ExecutionClock>,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    if let Some(error) = &alter.resolution_error {
        return Err(error.clone());
    }
    let mut candidate = state.clone();
    let mut name = alter.table.clone();
    let epoch = state.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
    for action in &alter.actions {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        let mut table = candidate
            .tables
            .get(&name)
            .ok_or(SqlError::UnknownTable)?
            .clone();
        match action {
            Action::PrimaryKey(explicit, columns) => constraint_primary_key::add(
                &mut candidate,
                &table,
                explicit.as_deref(),
                columns,
                &mut *cancel,
            )?,
            Action::Constraint(definition) => {
                constraints::install(&mut candidate, &table, definition, &mut *cancel)?
            }
            Action::Validate(name) => {
                constraints::validate_named(&mut candidate, &table, name, &mut *cancel)?
            }
            Action::DropConstraint(name, optional) => {
                constraints::drop_named(&mut candidate, &table, name, *optional)?
            }
            Action::Add(column, typmod, default, semantic_error) => {
                if let Some(error) = semantic_error {
                    return Err(error.clone());
                }
                if let Some(typmod) = typmod {
                    validate_typmod(&column.data_type, *typmod)
                        .map_err(|_| SqlError::InvalidParameterValue)?;
                }
                ident::ident(&column.name)?;
                if table.columns.iter().any(|c| c.name == column.name) {
                    return Err(SqlError::DuplicateColumn);
                }
                if table.columns.len() >= MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                let mut column = column.clone();
                column.id = allocate(&mut candidate.next_id)?;
                // Keep the hidden heap key last, matching CREATE's logical-column convention.
                let at = table
                    .columns
                    .iter()
                    .position(|c| c.name == HIDDEN_ROWID)
                    .unwrap_or(table.columns.len());
                table.columns.insert(at, column.clone());
                for key in &mut table.primary_key {
                    if *key >= at {
                        *key += 1;
                    }
                }
                candidate.tables.insert(name.clone(), table.clone());
                if let Some(typmod) = typmod {
                    candidate.set_column_typmod(column.id, Some(*typmod))?;
                }
                defaults::install_alter(&mut candidate, &column, default.as_deref())?;
                let mut rows = candidate.rows.get(&table.id).ok_or(SqlError::Type)?.clone();
                for row in rows.values_mut() {
                    if cancel() {
                        return Err(SqlError::Aborted);
                    }
                    let value = defaults::evaluate(&candidate, &column, clock)?;
                    row.values.insert(at, value);
                    row_check(&table, &row.values)?;
                }
                candidate.rows.insert(table.id, rows);
            }
            Action::Drop(column, optional) => {
                let at = match position(&table, column) {
                    Ok(at) => at,
                    Err(SqlError::UnknownColumn) if *optional => continue,
                    Err(error) => return Err(error),
                };
                if table.logical_columns().count() <= 1 || table.primary_key.contains(&at) {
                    return Err(SqlError::Unsupported);
                }
                let id = table.columns[at].id;
                if candidate.ext.constraints.values().flatten().any(|constraint| matches!(&constraint.kind, ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } if columns.contains(&id) || (*ref_table == table.id && ref_columns.contains(&id)))) { return Err(SqlError::Unsupported); }
                let descriptors = candidate
                    .secondary_descriptors_engine()
                    .iter()
                    .filter(|d| !(d.table == table.id && d.columns.contains(&id)))
                    .cloned()
                    .collect::<Vec<_>>();
                table.columns.remove(at);
                for key in &mut table.primary_key {
                    if *key > at {
                        *key -= 1;
                    }
                }
                for row in candidate
                    .rows
                    .get_mut(&table.id)
                    .ok_or(SqlError::Type)?
                    .values_mut()
                {
                    row.values.remove(at);
                }
                candidate.tables.insert(name.clone(), table.clone());
                candidate.drop_column_ext(id)?;
                candidate.secondary =
                    candidate.secondary_build_complete_engine(&descriptors, &mut *cancel)?;
            }
            Action::RenameColumn(old, new) => {
                let at = position(&table, old)?;
                ident::ident(new)?;
                if table.columns.iter().any(|c| &c.name == new) {
                    return Err(SqlError::DuplicateColumn);
                }
                table.columns[at].name = new.clone();
                candidate.tables.insert(name.clone(), table.clone());
            }
            Action::RenameTable(new) => {
                ident::ident(new)?;
                if candidate.relation_name_taken(new) {
                    return Err(SqlError::DuplicateTable);
                }
                constraint_primary_key::retain_name_before_rename(&mut candidate, &table)?;
                candidate.tables.remove(&name);
                name = new.clone();
                table.name = name.clone();
                candidate.tables.insert(name.clone(), table.clone());
            }
            Action::Default(column, default) => {
                let at = position(&table, column)?;
                defaults::install_alter(&mut candidate, &table.columns[at], default.as_deref())?;
            }
            Action::NotNull(column, set) => {
                let at = position(&table, column)?;
                if !set && table.primary_key.contains(&at) {
                    return Err(SqlError::InvalidTableDefinition);
                }
                if *set
                    && candidate
                        .rows
                        .get(&table.id)
                        .ok_or(SqlError::Type)?
                        .values()
                        .any(|row| row.values[at] == Value::Null)
                {
                    return Err(SqlError::Null);
                }
                table.columns[at].not_null = *set;
                candidate.tables.insert(name.clone(), table.clone());
            }
            Action::Unique(explicit, columns) => {
                constraints::install_unique(&mut candidate, &table, explicit, columns, cancel)?;
            }
        }
    }
    let descriptors = candidate.secondary_descriptors_engine().to_vec();
    candidate.secondary = candidate.secondary_build_complete_engine(&descriptors, &mut *cancel)?;
    candidate.epoch = epoch;
    candidate.validate_ext()?;
    if cancel() {
        return Err(SqlError::Aborted);
    }
    *state = candidate;
    Ok(())
}

#[cfg(test)]
#[path = "alter_table_tests.rs"]
mod tests;
