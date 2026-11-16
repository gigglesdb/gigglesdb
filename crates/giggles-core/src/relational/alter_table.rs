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
    Drop(String, bool, bool),
    RenameColumn(String, String),
    RenameTable(String),
    Default(String, Option<String>),
    NotNull(String, bool),
    Unique(Option<String>, Vec<String>),
    PrimaryKey(Option<String>, Vec<String>),
    Constraint(constraints::Definition),
    Validate(String),
    DropConstraint(String, bool, bool),
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
        let error = ident::ident(&table).err();
        p.relations
            .push(schemas::Reference::read(table.clone(), Some(first.clone())));
        (format!("{first}\0{table}"), error)
    } else {
        let error = ident::ident(&first).err();
        p.relations
            .push(schemas::Reference::read(first.clone(), None));
        (first, error)
    };
    let mut actions = Vec::new();
    let mut unsupported_type = false;
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
                let ddl_type::Parsed {
                    data_type,
                    typmod,
                    semantic_error,
                } = ddl_type::parse(p, ddl_type::Site::Alter)?;
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
                let name = raw_name(p)?;
                let cascade = p.word("cascade");
                if !cascade { p.word("restrict"); }
                Action::DropConstraint(name, optional, cascade)
            } else {
                p.word("column");
                let optional = p.word("if");
                if optional {
                    p.need("exists")?;
                }
                let name = raw_name(p)?;
                let cascade = p.word("cascade");
                if !cascade { p.word("restrict"); }
                Action::Drop(name, optional, cascade)
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
            let set = p.word("set");
            let type_change = if set && p.word("data") {
                p.need("type")?;
                true
            } else {
                !set && p.word("type")
            };
            if type_change {
                ddl_type::parse(p, ddl_type::Site::Alter)?;
                unsupported_type = true;
                if p.sym(',') {
                    continue;
                }
                break;
            }
            let set = if set {
                true
            } else if p.word("drop") {
                false
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
    if unsupported_type {
        return Err(SqlError::Unsupported);
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
fn position(table: &TableDef, name: &str) -> R<usize> {
    if name == HIDDEN_ROWID {
        return Err(SqlError::Reserved);
    }
    col(table, name)
}
pub(super) fn execute(
    state: &mut Relational,
    alter: &AlterTable,
    path: &[String],
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
            Action::DropConstraint(name, optional, cascade) => {
                constraints::drop_named_cascade(&mut candidate, &table, name, *optional, *cascade)?
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
                if column.name == "ctid" {
                    return Err(SqlError::DuplicateColumn);
                }
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
                let logical = match column.data_type {
                    DataType::Uuid => Some(catalog_ext::LogicalKind::Uuid),
                    DataType::Jsonb => Some(catalog_ext::LogicalKind::Jsonb),
                    DataType::IntArray => Some(catalog_ext::LogicalKind::IntArray),
                    DataType::BigIntArray => Some(catalog_ext::LogicalKind::BigIntArray),
                    DataType::TextArray => Some(catalog_ext::LogicalKind::TextArray),
                    _ => None,
                };
                if let Some(kind) = logical {
                    candidate.ext.logical_kinds.insert(column.id, kind);
                    Relational::normalize_ext(&mut candidate.ext);
                }
                let kind = match column.data_type {
                    DataType::Varchar => Some(catalog_ext::TextKind::Varchar),
                    DataType::Bpchar => Some(catalog_ext::TextKind::Bpchar),

                    _ => None,
                };
                if let Some(kind) = kind {
                    candidate
                        .ext
                        .columns
                        .entry(column.id)
                        .or_default()
                        .text_kind = Some(kind);
                    // ext_version follows content (v3 iff any text_kind); validate_ext checks it.
                    Relational::normalize_ext(&mut candidate.ext);
                }
                if let Some(typmod) = typmod {
                    candidate.set_column_typmod(column.id, Some(*typmod))?;
                }
                defaults::install_alter(&mut candidate, &column, default.as_deref(), path)?;
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
            Action::Drop(column, optional, cascade) => {
                let at = match position(&table, column) {
                    Ok(at) => at,
                    Err(SqlError::UnknownColumn) if *optional => continue,
                    Err(error) => return Err(error),
                };
                views::drop_column_views(&mut candidate, table.columns[at].id, *cascade)?;
                if table.logical_columns().count() <= 1 || table.primary_key.contains(&at) {
                    return Err(SqlError::Unsupported);
                }
                let id = table.columns[at].id;
                if candidate.ext.constraints.values().flatten().any(|constraint| matches!(&constraint.kind, ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } if columns.contains(&id) || (*ref_table == table.id && ref_columns.contains(&id)))) { return Err(SqlError::Unsupported); }
                if let Some(cs) = candidate.ext.constraints.get_mut(&table.id) {
                    cs.retain(|c| !matches!(&c.kind, ConstraintKind::Unique { columns, .. } if columns.contains(&id)));
                }
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
                if candidate
                    .relation_name_taken(&relation_key(candidate.table_schema(table.id), new))
                {
                    return Err(SqlError::DuplicateTable);
                }
                constraint_primary_key::retain_name_before_rename(&mut candidate, &table)?;
                candidate.tables.remove(&name);
                name = relation_key(candidate.table_schema(table.id), new);
                table.name = new.clone();
                candidate.tables.insert(name.clone(), table.clone());
            }
            Action::Default(column, default) => {
                let at = position(&table, column)?;
                defaults::install_alter(
                    &mut candidate,
                    &table.columns[at],
                    default.as_deref(),
                    path,
                )?;
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
