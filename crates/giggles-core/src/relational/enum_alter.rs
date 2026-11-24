//! PostgreSQL enum label changes over the existing text-backed enum format.
use super::*;
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub(super) struct Alter {
    name: String,
    action: Action,
}

#[derive(Debug, Clone)]
enum Action {
    Add {
        label: String,
        optional: bool,
        neighbor: Option<(bool, String)>,
    },
    Rename {
        old: String,
        new: String,
    },
}

/// An explicit or implicit SQL block owns this state. Prepared expressions
/// carry the enum definition, so its schema and name also identify casts here.
#[derive(Clone, Default)]
pub(crate) struct EnumTransaction(Arc<BTreeMap<String, BTreeMap<String, EnumChanges>>>);

#[derive(Clone, Default)]
struct EnumChanges {
    created_at_top_level: bool,
    unsafe_labels: BTreeSet<String>,
}

thread_local! {
    static TRANSACTION: RefCell<Option<EnumTransaction>> = const { RefCell::new(None) };
}

thread_local! {
    static IN_SAVEPOINT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) struct EnumScope(Option<EnumTransaction>, bool);

impl EnumScope {
    pub(crate) fn enter(transaction: Option<&EnumTransaction>, in_savepoint: bool) -> Self {
        Self(TRANSACTION.replace(transaction.cloned()), IN_SAVEPOINT.replace(in_savepoint))
    }

    pub(crate) fn changes(&self) -> EnumTransaction {
        TRANSACTION.with_borrow(|state| state.clone().unwrap_or_default())
    }
}

impl Drop for EnumScope {
    fn drop(&mut self) {
        TRANSACTION.replace(self.0.take());
        IN_SAVEPOINT.set(self.1);
    }
}

fn change(ty: &TypeDef, update: impl FnOnce(&mut EnumChanges)) {
    TRANSACTION.with_borrow_mut(|transaction| {
        if let Some(transaction) = transaction {
            let changes = Arc::make_mut(&mut transaction.0);
            let schema = changes.entry(ty.schema().into()).or_default();
            update(schema.entry(ty.name().into()).or_default());
        }
    });
}

pub(super) fn created(ty: &TypeDef) {
    if matches!(ty, TypeDef::Enum { .. }) {
        change(ty, |changes| {
            *changes = EnumChanges {
                created_at_top_level: !IN_SAVEPOINT.get(),
                ..Default::default()
            }
        });
    }
}

fn check_use(ty: &TypeDef, label: &str) -> R<()> {
    let unsafe_value = TRANSACTION.with_borrow(|transaction| {
        transaction
            .as_ref()
            .and_then(|transaction| transaction.0.get(ty.schema()))
            .and_then(|schema| schema.get(ty.name()))
            .is_some_and(|changes| changes.unsafe_labels.contains(label))
    });
    if unsafe_value {
        user_types::view_replace_error(format!(
            "unsafe use of new value \"{label}\" of enum type {}",
            type_name(ty)
        ));
        user_types::sql_error_hint("New enum values must be committed before they can be used.");
        return Err(SqlError::UnsafeNewEnumValue);
    }
    Ok(())
}

pub(super) fn check_value(ty: &TypeDef, labels: &[String], label: &str) -> R<()> {
    if !labels.iter().any(|value| value == label) {
        user_types::view_replace_error(format!(
            "invalid input value for enum {}: \"{label}\"",
            type_name(ty)
        ));
        return Err(SqlError::InvalidTextRepresentation);
    }
    check_use(ty, label)
}

fn default_label(tokens: &[Token]) -> Option<&str> {
    match tokens {
        [Token::String(label)]
        | [
            Token::String(label),
            Token::Symbol(':'),
            Token::Symbol(':'),
            Token::Word(_),
        ]
        | [
            Token::String(label),
            Token::Symbol(':'),
            Token::Symbol(':'),
            Token::QuotedIdent(_),
        ] => Some(label),
        _ => None,
    }
}

pub(super) fn validate_default(ty: Option<&TypeDef>, sql: Option<&str>) -> R<()> {
    if let (Some(ty @ TypeDef::Enum { labels, .. }), Some(sql)) = (ty, sql)
        && let Some(label) = default_label(&lex(sql)?)
    {
        check_value(ty, labels, label)?;
    }
    Ok(())
}

pub(super) fn present_default(ty: Option<&TypeDef>, sql: &str) -> R<Option<String>> {
    let Some(ty @ TypeDef::Enum { .. }) = ty else {
        return Ok(None);
    };
    Ok(default_label(&lex(sql)?)
        .map(|label| format!("{}::{}", defaults::quoted(label), type_name(ty))))
}

fn type_name(ty: &TypeDef) -> String {
    let name = query::constraint_quote(ty.name());
    if ty.schema() == "public" {
        name
    } else {
        format!("{}.{name}", query::constraint_quote(ty.schema()))
    }
}

pub(super) fn claims(p: &Parser) -> bool {
    matches!(&p.tokens[p.at..], [Token::Word(a), Token::Word(b), ..] if a == "alter" && b == "type")
}

pub(super) fn parse(p: &mut Parser) -> R<Alter> {
    p.need("alter")?;
    p.need("type")?;
    let first = p.raw_name()?;
    let name = if p.sym('.') {
        format!("{first}\0{}", p.raw_name()?)
    } else {
        first
    };
    let action = if p.word("add") {
        p.need("value")?;
        let optional = p.word("if");
        if optional {
            p.need("not")?;
            p.need("exists")?;
        }
        let label = string(p)?;
        let before = p.word("before");
        let neighbor = if before || p.word("after") {
            Some((before, string(p)?))
        } else {
            None
        };
        Action::Add {
            label,
            optional,
            neighbor,
        }
    } else {
        p.need("rename")?;
        p.need("value")?;
        let old = string(p)?;
        p.need("to")?;
        Action::Rename {
            old,
            new: string(p)?,
        }
    };
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(Alter { name, action })
}

fn string(p: &mut Parser) -> R<String> {
    let Some(Token::String(value)) = p.tokens.get(p.at) else {
        return Err(SqlError::Syntax);
    };
    p.at += 1;
    Ok(value.clone())
}

fn locate(labels: &[String], label: &str) -> R<usize> {
    labels.iter().position(|v| v == label).ok_or_else(|| {
        user_types::view_replace_error(format!("\"{label}\" is not an existing enum label"));
        SqlError::InvalidParameterValue
    })
}

fn duplicate(label: &str) -> String {
    format!("enum label \"{label}\" already exists")
}

fn check_length(label: &str) -> R<()> {
    if label.len() > 63 {
        user_types::view_replace_error(format!("invalid enum label \"{label}\""));
        user_types::sql_error_detail("Labels must be 63 bytes or less.".into());
        return Err(SqlError::InvalidName);
    }
    Ok(())
}

fn resolve<'a>(state: &'a Relational, name: &str, path: &[String]) -> R<(u64, &'a TypeDef)> {
    let user = if name.starts_with("pg_catalog\0") {
        None
    } else {
        state.resolve_type(name, path)?
    };
    if let Some((id, ty)) = user {
        if matches!(ty, TypeDef::Enum { .. }) {
            return Ok((id, ty));
        }
        user_types::view_replace_error(format!("{} is not an enum", type_name(ty)));
        return Err(SqlError::WrongObjectType);
    }
    let builtin = name.strip_prefix("pg_catalog\0").unwrap_or(name);
    let mut parser = Parser {
        tokens: vec![Token::Word(builtin.into())],
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
        privilege_seen: false,
        privilege_casts: false,
    };
    if let Ok(ty) = ddl_type::parse(&mut parser, ddl_type::Site::Alter) {
        user_types::view_replace_error(format!(
            "{} is not an enum",
            defaults::type_name(&ty.data_type)
        ));
        return Err(SqlError::WrongObjectType);
    }
    if let Some(table) = state.tables.get(&state.resolve_relation_key(name, path)) {
        user_types::view_replace_error(format!(
            "{} is not an enum",
            query::constraint_quote(&table.name)
        ));
        return Err(SqlError::WrongObjectType);
    }
    user_types::view_replace_error(format!("type \"{}\" does not exist", name.replace('\0', ".")));
    Err(SqlError::UndefinedObject)
}

impl Alter {
    pub(super) fn rewrite_tables(&self, state: &Relational, path: &[String]) -> Vec<String> {
        if !matches!(self.action, Action::Rename { .. }) {
            return Vec::new();
        }
        let Ok(Some((id, _))) = state.resolve_type(&self.name, path) else {
            return Vec::new();
        };
        state
            .tables
            .iter()
            .filter(|(_, table)| {
                table
                    .columns
                    .iter()
                    .any(|column| column_uses(state, column.id, id))
            })
            .map(|(key, _)| key.clone())
            .collect()
    }
}

fn column_uses(state: &Relational, column: u64, ty: u64) -> bool {
    state.ext.columns.get(&column).and_then(|c| c.type_id) == Some(ty)
}

fn unsupported_dependency(kind: &str) -> SqlError {
    user_types::view_replace_error(format!("cannot rename enum value with a dependent {kind}"));
    SqlError::Unsupported
}

fn identifier_token(token: &Token, name: &str) -> bool {
    matches!(token, Token::Word(w) | Token::QuotedIdent(w) if w == name)
}

fn mentions_type(tokens: &[Token], ty: &TypeDef) -> bool {
    tokens.iter().enumerate().any(|(i, token)| {
        if !identifier_token(token, ty.name()) {
            return false;
        }
        let prefix = &tokens[..i];
        let prefix = match prefix {
            [qualifier @ .., schema, Token::Symbol('.')] => {
                if !identifier_token(schema, ty.schema()) {
                    return false;
                }
                qualifier
            }
            _ => prefix,
        };
        matches!(prefix, [.., Token::Symbol(':'), Token::Symbol(':')])
            || matches!(prefix, [.., Token::Word(as_word)] if as_word == "as"
                && prefix.iter().any(|token| matches!(token, Token::Word(word) if word == "cast")))
    })
}

impl Prepared {
    pub(crate) fn check_enum_rename_views<'a>(
        &self,
        state: &Relational,
        sql_views: impl IntoIterator<Item = &'a str>,
    ) -> R<()> {
        let Statement::TypeDdl(user_types::Ddl::Alter(Alter {
            name,
            action: Action::Rename { .. },
        })) = self.statement.get()
        else {
            return Ok(());
        };
        let Some((_, ty)) = state.resolve_type(name, &self.search_path)? else {
            return Ok(());
        };
        for sql in sql_views {
            if mentions_type(&lex(sql)?, ty) {
                return Err(unsupported_dependency("view"));
            }
        }
        Ok(())
    }
}

fn rewrite(
    state: &mut Relational,
    id: u64,
    ty: &TypeDef,
    old: &str,
    new: &str,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let mut rewritten_checks = Vec::new();
    for (owner, constraints) in &state.ext.constraints {
        for constraint in constraints {
            match &constraint.kind {
                ConstraintKind::Check { expr, .. } => {
                    let tokens = lex(expr)?;
                    if mentions_type(&tokens, ty) {
                        return Err(unsupported_dependency("CHECK constraint"));
                    }
                    let table = state.tables.values().find(|table| table.id == *owner)
                        .ok_or(SqlError::InternalError)?;
                    let columns = table.columns.iter()
                        .filter(|column| column_uses(state, column.id, id))
                        .map(|column| column.name.as_str())
                        .collect::<BTreeSet<_>>();
                    if columns.iter().any(|name| tokens.iter().any(|token| identifier_token(token, name)))
                        && let Some(expr) = query::rename_enum_check(state, table, expr, &columns, old, new)
                            .map_err(|error| {
                                if error == SqlError::Unsupported { unsupported_dependency("CHECK constraint") } else { error }
                            })?
                    {
                        rewritten_checks.push((*owner, constraint.id, expr));
                    }
                }
                ConstraintKind::ForeignKey {
                    columns,
                    ref_columns,
                    ..
                } if columns.iter().zip(ref_columns).any(|(left, right)| {
                    column_uses(state, *left, id) != column_uses(state, *right, id)
                }) =>
                {
                    return Err(unsupported_dependency(
                        "foreign key with different column types",
                    ));
                }
                _ => (),
            }
        }
    }
    for other in state.ext.types.values() {
        if let TypeDef::Domain {
            check: Some(check), ..
        } = other
            && mentions_type(&lex(check)?, ty)
        {
            return Err(unsupported_dependency("domain"));
        }
    }
    if state
        .tables
        .values()
        .flat_map(|t| &t.columns)
        .any(|c| c.data_type == DataType::TextArray && column_uses(state, c.id, id))
    {
        return Err(unsupported_dependency("enum array"));
    }
    let mut rewrite_defaults = Vec::new();
    for (&column, metadata) in &state.ext.columns {
        let Some(sql) = &metadata.default else {
            continue;
        };
        let tokens = lex(sql)?;
        if metadata.type_id == Some(id) {
            if let Some(label) = default_label(&tokens) {
                if label == old {
                    rewrite_defaults.push((column, format!("{}::text", defaults::quoted(new))));
                }
            } else if !matches!(tokens.as_slice(), [Token::Word(null)] if null == "null") {
                return Err(unsupported_dependency("enum default expression"));
            }
        } else if mentions_type(&tokens, ty) {
            return Err(unsupported_dependency("default expression"));
        }
    }
    for (column, default) in rewrite_defaults {
        state.set_column_default(column, Some(default))?;
    }
    for (table, id, updated) in rewritten_checks {
        let constraint = Arc::make_mut(&mut state.ext).constraints.get_mut(&table)
            .and_then(|constraints| constraints.iter_mut().find(|constraint| constraint.id == id));
        let Some(Constraint { kind: ConstraintKind::Check { expr, .. }, .. }) = constraint else {
            return Err(SqlError::InternalError);
        };
        *expr = updated;
    }
    let tables = state.tables.clone();
    for table in tables.values() {
        let offsets = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| column_uses(state, c.id, id))
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if offsets.is_empty() {
            continue;
        }
        if state.view_names.values().any(|(_, projection)| {
            projection.as_ref().is_ok_and(|p| {
                offsets.iter().any(|i| {
                    let id = table.columns[*i].id;
                    p.columns.iter().any(|(c, _)| c.id == id) || p.dependencies.contains(&id)
                })
            })
        }) {
            return Err(unsupported_dependency("view"));
        }
        let rows = state
            .rows
            .get_mut(&table.id)
            .ok_or(SqlError::UnknownTable)?;
        let mut rewritten = BTreeMap::new();
        for mut row in std::mem::take(rows).into_values() {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            let mut changed = false;
            for offset in &offsets {
                if matches!(&row.values[*offset], Value::Text(label) if label == old) {
                    row.values[*offset] = Value::Text(new.into());
                    changed = true;
                }
            }
            if changed {
                row_check(table, &row.values)?;
            }
            rewritten.insert(pk(table, &row.values)?, row);
        }
        *rows = rewritten;
    }
    let descriptors = state.secondary_descriptors_engine().to_vec();
    state.secondary = state.secondary_build_complete_engine(&descriptors, cancel)?;
    Ok(())
}

pub(super) fn execute(
    state: &mut Relational,
    alter: &Alter,
    path: &[String],
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let (id, ty) = resolve(state, &alter.name, path)?;
    let TypeDef::Enum { labels, .. } = ty else {
        unreachable!()
    };
    let mut labels = labels.clone();
    let mut candidate = state.clone();
    match &alter.action {
        Action::Add {
            label,
            optional,
            neighbor,
        } => {
            check_length(label)?;
            if labels.contains(label) {
                let message = duplicate(label);
                if *optional {
                    user_types::sql_notice(format!("{message}, skipping"), "42710");
                    return Ok(());
                }
                user_types::view_replace_error(message);
                return Err(SqlError::DuplicateObject);
            }
            let position = match neighbor {
                Some((before, neighbor)) => locate(&labels, neighbor)? + usize::from(!before),
                None => labels.len(),
            };
            if labels.len() >= MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            let label_id = allocate(&mut candidate.next_id)?;
            type_oid(label_id)?;
            Arc::make_mut(&mut candidate.ext)
                .added_enum_labels.entry(id).or_default().0.insert(label.clone(), label_id);
            labels.insert(position, label.clone());
        }
        Action::Rename { old, new } => {
            check_length(new)?;
            let position = locate(&labels, old)?;
            if labels.contains(new) {
                user_types::view_replace_error(duplicate(new));
                return Err(SqlError::DuplicateObject);
            }
            rewrite(&mut candidate, id, ty, old, new, cancel)?;
            if let Some(added) = Arc::make_mut(&mut candidate.ext).added_enum_labels.get_mut(&id)
                && let Some(label_id) = added.0.remove(old)
            {
                added.0.insert(new.clone(), label_id);
            }
            labels[position] = new.clone();
        }
    }
    let Some(TypeDef::Enum { labels: stored, .. }) =
        Arc::make_mut(&mut candidate.ext).types.get_mut(&id)
    else {
        return Err(SqlError::Type);
    };
    *stored = labels;
    Arc::make_mut(&mut candidate.ext).ext_version = candidate.ext.wire_version();
    candidate.validate_ext()?;
    candidate.epoch = state.epoch.checked_add(1).ok_or(SqlError::Limit)?;
    if cancel() {
        return Err(SqlError::Aborted);
    }
    match &alter.action {
        Action::Add { label, .. } => change(ty, |changes| {
            if !changes.created_at_top_level || IN_SAVEPOINT.get() {
                changes.unsafe_labels.insert(label.clone());
            }
        }),
        Action::Rename { old, new } => change(ty, |changes| {
            if changes.unsafe_labels.remove(old) {
                changes.unsafe_labels.insert(new.clone());
            }
        }),
    }
    *state = candidate;
    Ok(())
}

#[cfg(test)]
#[path = "enum_alter_tests.rs"]
mod tests;
