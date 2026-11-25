//! Bounded ALTER actions, staged together before catalog publication.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct AlterTable {
    pub table: String,
    pub actions: Vec<Action>,
    pub resolution_error: Option<SqlError>,
    pub column_names: ColumnNamePolicy,
}
#[derive(Debug, Clone)]
pub(super) enum Action {
    Add(
        ColumnDef,
        Option<i32>,
        Option<String>,
        Option<SqlError>,
        bool,
        Option<String>,
        Option<user_types::TypeReference>,
        Option<generated_columns::Definition>,
        AddConstraints,
    ),
    Type(
        String,
        DataType,
        Option<i32>,
        Option<String>,
        Option<user_types::TypeReference>,
        Option<Box<query::Query>>,
    ),
    Drop(String, bool, bool),
    RenameColumn(String, String),
    RenameConstraint(String, String),
    RenameTable(String),
    Default(String, Option<String>),
    Expression(String, Option<generated_columns::Definition>, bool),
    NotNull(String, bool),
    Unique(Option<String>, Vec<String>),
    PrimaryKey(Option<String>, Vec<String>),
    Constraint(constraints::Definition),
    Validate(String),
    DropConstraint(String, bool, bool),
    TriggerState(triggers::Target, u8),
}
#[derive(Debug, Clone, Default)]
pub(super) struct AddConstraints {
    pub primary: Option<Option<String>>,
    pub definitions: Vec<constraints::Definition>,
}
impl AddConstraints {
    pub(super) fn is_empty(&self) -> bool {
        self.primary.is_none() && self.definitions.is_empty()
    }
}
impl AlterTable {
    pub fn sequence_dependencies(&self) -> Vec<SequenceDependency> {
        self.actions
            .iter()
            .filter_map(|action| match action {
                Action::Add(_, _, Some(sql), ..) | Action::Default(_, Some(sql)) => Some(sql),
                _ => None,
            })
            .flat_map(|sql| defaults::dependencies(sql).unwrap_or_default().to_vec())
            .map(SequenceDependency::Named)
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

    loop {
        if p.at == p.tokens.len() || matches!(p.tokens.get(p.at), Some(Token::Symbol(';'))) {
            return Err(SqlError::Syntax);
        }
        let action = if matches!(p.tokens.get(p.at), Some(Token::Word(word)) if matches!(word.as_str(), "enable" | "disable")) {
            let (target, enabled) = triggers::parse_state(p)?;
            Action::TriggerState(target, enabled)
        } else if p.word("add") {
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
                let if_not_exists = p.word("if");
                if if_not_exists {
                    p.need("not")?;
                    p.need("exists")?;
                }
                let name = raw_name(p)?;
                if !matches!(p.tokens.get(p.at), Some(Token::Word(_) | Token::QuotedIdent(_))) {
                    return Err(SqlError::Syntax);
                }
                let (
                    ddl_type::Parsed {
                        data_type,
                        typmod,
                        semantic_error,
                    },
                    not_null,
                    default,
                    collation,
                    user_type,
                    generation,
                    constraints,
                ) = parse_add_definition(p, if_not_exists, &table, &name)?;
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
                    if_not_exists,
                    collation,
                    user_type,
                    generation,
                    constraints,
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
            } else if p.word("constraint") {
                let name = raw_name(p)?;
                p.need("to")?;
                Action::RenameConstraint(name, raw_name(p)?)
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
                let (parsed,user_type)=ddl_type::parse_column_type(p, ddl_type::Site::Alter)?;
                let collation=if p.word("collate") {Some(column_collations::parse_name(p)?)} else {None};
                if let Some(error)=parsed.semantic_error {return Err(error);}
                let conversion = if parsed.data_type.is_text_search() && p.word("using") {
                    Some(Box::new(query::Query::parse_text_search_conversion(p, &table, parsed.data_type.clone())?))
                } else if user_type.is_some() && p.word("using") {
                    Some(Box::new(query::Query::alter_type_assignment(p, &table)?))
                } else {
                    None
                };
                actions.push(Action::Type(name, parsed.data_type, parsed.typmod, collation, user_type, conversion));
                if p.sym(',') {continue;}
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
            } else if p.word("expression") {
                let expression = if set {
                    p.need("as")?;
                    let (tokens, locations) = generated_columns::expression_tokens(p)?;
                    Some(generated_columns::Definition { tokens, locations, stored: false })
                } else { None };
                let if_exists = !set && p.word("if");
                if if_exists { p.need("exists")?; }
                Action::Expression(name, expression, if_exists)
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
        column_names: ColumnNamePolicy::Sql,
    })
}
#[allow(clippy::type_complexity)]
fn parse_add_definition(
    p: &mut Parser,
    if_not_exists: bool,
    table: &str,
    column: &str,
) -> R<(ddl_type::Parsed, bool, Option<String>, Option<String>, Option<user_types::TypeReference>, Option<generated_columns::Definition>, AddConstraints)> {
    let type_start = p.at;
    let (mut parsed, user_type) = match ddl_type::parse_column_type(p, ddl_type::Site::Alter) {
        Ok(parsed) => parsed,
        // An unknown type name is semantic, but its modifiers must still have
        // valid syntax. Defer the existing Unsupported error until execution.
        Err(SqlError::Unsupported) if if_not_exists && p.at == type_start + 1 => {
            if p.sym('.') {
                raw_name(p)?;
            }
            if p.sym('(') {
                p.expr().map_err(add_modifier_syntax)?;
                while p.sym(',') {
                    p.expr().map_err(add_modifier_syntax)?;
                }
                p.punct(')')?;
            }
            while p.sym('[') {
                p.punct(']')?;
            }
            (ddl_type::Parsed {
                data_type: DataType::Text,
                typmod: None,
                semantic_error: Some(SqlError::Unsupported),
            }, None)
        }
        Err(error) => return Err(error),
    };
    let mut collation=None;
    let mut not_null = false;
    let mut default = None;
    let mut generation = None;
    let mut constraints = AddConstraints::default();
    loop {
        let named = if p.word("constraint") { Some(raw_name(p)?) } else { None };
        if p.word("collate") { collation=Some(column_collations::parse_name(p)?);
        } else if p.word("not") {
            p.need("null")?;
            not_null = true;
        } else if p.word("default") {
            if generation.is_some() {
                generated_columns::error_at(p.at - 1);
                return Err(generated_columns::conflicting_default(table, column));
            }
            if default.is_some() {
                return Err(SqlError::Syntax);
            }
            let default_start = p.at;
            match defaults::parse_alter_untyped(p) {
                Ok(sql) => default = Some(sql),
                // The bounded default evaluator rejects functions/expressions
                // that the general grammar can parse. A skipped ADD must not
                // evaluate them, but malformed expressions still fail parsing.
                Err(SqlError::Unsupported) if if_not_exists => {
                    p.at = default_start;
                    parse_skippable_default(p)?;
                    parsed.semantic_error = Some(SqlError::Unsupported);
                    default = Some(String::new());
                }
                Err(error) => return Err(error),
            }
        } else if p.word("generated") {
            if default.is_some() {
                generated_columns::error_at(p.at - 1);
                return Err(generated_columns::conflicting_default(table, column));
            }
            if generation.is_some() {
                return Err(SqlError::Syntax);
            }
            generation = Some(generated_columns::parse(p)?);
        } else if p.word("check") {
            constraints.definitions.push(constraints::parse_check(p, named)?);
        } else if p.word("references") {
            constraints.definitions.push(constraints::parse_foreign(p, named, vec![column.into()])?);
        } else if p.word("unique") {
            constraints.definitions.push(constraints::Definition::Unique { name: named, columns: vec![column.into()] });
        } else if p.word("primary") {
            p.need("key")?;
            if constraints.primary.replace(named).is_some() { return Err(SqlError::InvalidTableDefinition); }
        } else if p.word("null") {
            // An explicit NULL clause has the same default as an omitted clause.
        } else {
            if named.is_some() { return Err(SqlError::Syntax); }
            break;
        }
    }
    Ok((parsed, not_null, default, collation, user_type, generation, constraints))
}
fn parse_skippable_default(p: &mut Parser) -> R<()> {
    let start = p.at;
    let mut depth = 0usize;
    while let Some(token) = p.tokens.get(p.at) {
        match token {
            Token::Symbol(',' | ';') if depth == 0 => break,
            Token::Word(word)
                if depth == 0
                    && word == "not"
                    && matches!(p.tokens.get(p.at + 1), Some(Token::Word(w)) if w == "null") =>
            {
                break;
            }
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth = depth.checked_sub(1).ok_or(SqlError::Syntax)?,
            _ => {}
        }
        p.at += 1;
    }
    if depth != 0 || start == p.at {
        return Err(SqlError::Syntax);
    }
    let mut tokens = vec![Token::Word("select".into())];
    tokens.extend_from_slice(&p.tokens[start..p.at]);
    let mut syntax = Parser { privilege_seen: false, privilege_casts: false,
        tokens,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    query::Query::parse(&mut syntax)?;
    Ok(())
}
fn add_modifier_syntax(error: SqlError) -> SqlError {
    match error {
        SqlError::Unsupported => SqlError::Syntax,
        other => other,
    }
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
    col(table, name).inspect_err(|error| {
        if *error == SqlError::UnknownColumn {
            user_types::view_replace_error(format!(
                "column \"{name}\" of relation \"{}\" does not exist", table.name
            ));
        }
    })
}

fn null_column_error(table: &TableDef, name: &str) -> SqlError {
    user_types::view_replace_error(format!(
        "column \"{name}\" of relation \"{}\" contains null values", table.name
    ));
    SqlError::Null
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
            Action::TriggerState(target, enabled) => triggers::set_enabled(&mut candidate, &table, target, *enabled)?,
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
                constraints::drop_named_cascade(&mut candidate, &table, name, *optional, *cascade, cancel)?
            }
            Action::Add(column, typmod, default, semantic_error, if_not_exists, collation, user_type, generation, constraints) => {
                if *if_not_exists
                    && column.name != "ctid"
                    && table.columns.iter().any(|c| c.name == column.name)
                {
                    alter.column_names.validate(&column.name)?;
                    user_types::duplicate_column_notice(&column.name, &table.name);
                    continue;
                }
                if let Some(error) = semantic_error {
                    return Err(error.clone());
                }
                if let Some(typmod) = typmod {
                    validate_typmod(&column.data_type, *typmod)
                        .map_err(|_| SqlError::InvalidParameterValue)?;
                }
                alter.column_names.validate(&column.name)?;
                if column.name == "ctid" {
                    return Err(SqlError::DuplicateColumn);
                }
                if table.columns.iter().any(|c| c.name == column.name) {
                    user_types::view_replace_error(format!(
                        "column \"{}\" of relation \"{}\" already exists", column.name, table.name
                    ));
                    return Err(SqlError::DuplicateColumn);
                }
                if table.column_slots() >= MAX_TABLE_COLUMNS {
                    return Err(table_column_limit());
                }
                let mut column = column.clone();
                let mut typmod=*typmod;
                let type_id=if let Some(n)=user_type {
                    let (id,ty)=n.resolve(&candidate, path)?;
                    column.data_type=user_types::storage_type(ty, n.array)?;
                    if let TypeDef::Domain{typmod:t,..}=ty {typmod = *t;}
                    Some(id)
                } else {None};
                let reference=column_collations::resolve_column(&candidate,&column,type_id,collation.as_deref(),path)?;
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
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
                std::sync::Arc::make_mut(&mut candidate.ext).column_collations.remove(&table.id);
                if let Some(id)=type_id {std::sync::Arc::make_mut(&mut candidate.ext).columns.entry(column.id).or_default().type_id=Some(id);}
                if let Some(r)=reference {std::sync::Arc::make_mut(&mut candidate.ext).column_collations.insert(column.id,column_collations::Binding::Column(r));}
                Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
                let logical = match column.data_type {
                    DataType::Uuid => Some(catalog_ext::LogicalKind::Uuid),
                    DataType::Jsonb => Some(catalog_ext::LogicalKind::Jsonb),
                    DataType::IntArray => Some(catalog_ext::LogicalKind::IntArray),
                    DataType::BigIntArray => Some(catalog_ext::LogicalKind::BigIntArray),
                    DataType::TextArray => Some(catalog_ext::LogicalKind::TextArray),
                    DataType::UuidArray => Some(catalog_ext::LogicalKind::UuidArray),
                    DataType::TsVector => Some(catalog_ext::LogicalKind::TsVector),
                    DataType::TsQuery => Some(catalog_ext::LogicalKind::TsQuery),
                    _ => None,
                };
                if let Some(kind) = logical {
                    std::sync::Arc::make_mut(&mut candidate.ext).logical_kinds.insert(column.id, kind);
                    Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
                }
                let kind = match column.data_type {
                    DataType::Varchar => Some(catalog_ext::TextKind::Varchar),
                    DataType::Bpchar => Some(catalog_ext::TextKind::Bpchar),

                    _ => None,
                };
                if let Some(kind) = kind {
                    std::sync::Arc::make_mut(&mut candidate.ext)
                        .columns.entry(column.id)
                        .or_default()
                        .text_kind = Some(kind);
                    // ext_version follows content (v3 iff any text_kind); validate_ext checks it.
                    Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
                }
                if let Some(typmod) = typmod {
                    candidate.set_column_typmod(column.id, Some(typmod))?;
                }
                defaults::install_alter(&mut candidate, &column, default.as_deref(), path)?;
                if let Some(generation) = generation {
                    generated_columns::install(&mut candidate, &table, &column, generation)?;
                    if !generation.stored {
                        column.not_null = false;
                        table.columns[at].not_null = false;
                        Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
                    }
                }
                let mut rows = candidate.rows.get(&table.id).ok_or(SqlError::Type)?.clone();
                for row in rows.values_mut() {
                    if cancel() {
                        return Err(SqlError::Aborted);
                    }
                    let value = defaults::evaluate(&candidate, &column, clock)?;
                    if column.not_null && value == Value::Null {
                        return Err(null_column_error(&table, &column.name));
                    }
                    row.values.insert(at, value);
                    candidate.materialize_generated(&table, &mut row.values)?;
                    row_check(&table, &row.values)?;
                }
                candidate.rows.insert(table.id, rows);
                if let Some(explicit) = &constraints.primary {
                    generated_columns::check_virtual_primary(&candidate, &column)?;
                    constraint_primary_key::add(&mut candidate, &table, explicit.as_deref(), std::slice::from_ref(&column.name), &mut *cancel)?;
                    table = candidate.tables[&name].clone();
                }
                for definition in &constraints.definitions {
                    constraints::install(&mut candidate, &table, definition, &mut *cancel)?;
                }
            }
            Action::Type(column, ty, typmod, collation, user_type, conversion) => {
                let at = position(&table, column)?;
                let old = &table.columns[at];
                generated_columns::check_referenced_type(&candidate, &table, old)?;
                let mut new = old.clone();
                new.data_type = ty.clone();
                let mut typmod = *typmod;
                let mut type_id = None;
                if let Some(target) = user_type {
                    let (id, ty) = target.resolve(&candidate, path)?;
                    new.data_type = user_types::storage_type(ty, target.array)?;
                    type_id = Some(id);
                    if let TypeDef::Domain { typmod: domain_typmod, .. } = ty {
                        typmod = *domain_typmod;
                    }
                    let same_type = candidate.ext.columns.get(&old.id).and_then(|c| c.type_id) == type_id
                        && old.data_type == new.data_type;
                    if !matches!(ty, TypeDef::Enum { .. }) || !same_type {
                        query::check_enum_text_retype(&candidate, &table, old)?;
                    }
                    if matches!(ty, TypeDef::Enum { .. }) && !same_type {
                        if conversion.is_none() {
                            user_types::view_replace_error(format!(
                                "column \"{column}\" cannot be cast automatically to type {}", target.sql()
                            ));
                            return Err(SqlError::Type);
                        }
                        if defaults::default_sql(&candidate, old).is_some() {
                            user_types::view_replace_error(format!(
                                "default for column \"{column}\" cannot be cast automatically to type {}", target.sql()
                            ));
                            return Err(SqlError::Type);
                        }
                    }
                } else {
                    if !matches!(ty, DataType::Text | DataType::Varchar) {
                        query::check_enum_text_retype(&candidate, &table, old)?;
                    }
                    if collation.is_none()
                        && !ty.is_text_search()
                        && !matches!(ty, DataType::Text | DataType::Varchar | DataType::Bpchar)
                        && candidate.generated_column(old.id).is_none()
                    {
                        return Err(SqlError::Unsupported);
                    }
                }
                // The v2 layout rewrite leaves same-width rows and existing
                // index tuples in place. Admit only a byte-preserving text change.
                let representation_unchanged = matches!(
                    (&old.data_type, &new.data_type),
                    (DataType::Text, DataType::Varchar) | (DataType::Varchar, DataType::Text)
                ) && typmod.is_none() && type_id.is_none() && collation.is_none() && conversion.is_none()
                    && candidate.column_collation(old.id).is_none()
                    && candidate.ext.columns.get(&old.id).is_none_or(|c| c.type_id.is_none());
                if !representation_unchanged && candidate.secondary_descriptors_engine().iter().any(|d| {
                    d.table == table.id && d.predicate.as_ref().is_some_and(|p| p.columns.contains(&old.id))
                }) {
                    return Err(SqlError::Unsupported);
                }
                let reference = column_collations::resolve_column(
                    &candidate, &new, type_id, collation.as_deref(), path
                )?;
                if new.data_type.is_text_search() && conversion.is_none() && old.data_type != new.data_type {
                    return Err(text_search_types::error_message("42804", format!(
                        "column \"{column}\" cannot be cast automatically to type {}",
                        new.data_type.text_search_name().unwrap()
                    )));
                }
                if new.data_type.is_text_search() && (table.primary_key.contains(&at)
                    || candidate.secondary_descriptors_engine().iter().any(|index|
                        index.table == table.id && index.columns.contains(&old.id))) {
                    return Err(SqlError::Unsupported);
                }
                let previous = candidate.clone();
                let rows = previous.rows.get(&table.id).ok_or(SqlError::Type)?;
                let conversion = conversion.as_ref().map(|query| {
                    let mut query = query.as_ref().clone();
                    if new.data_type.is_text_search() {
                        return Ok(query);
                    }
                    query.bind_schema_names(&|name| *name = previous.resolve_relation_key(name, path), &previous, path);
                    query.admit_assignment_literals(&mut 0)?;
                    Ok::<_, SqlError>(query)
                }).transpose()?;
                let conversion = conversion.as_ref().map(|query| {
                    if new.data_type.is_text_search() {
                        return query.bind(&previous, 0, &[]).map(|query| query.with_clock(clock));
                    }
                    let query = query.bind_assignment(&previous, vec![], &new.data_type)?.with_clock(clock);
                    query.validate_immutable_constants()?;
                    Ok::<_, SqlError>(query)
                }).transpose()?;

                let old_type = old.data_type.clone();
                table.columns[at] = new.clone();
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
                let ext = std::sync::Arc::make_mut(&mut candidate.ext);
                let metadata = ext.columns.entry(new.id).or_default();
                metadata.type_id = type_id;
                metadata.typmod = typmod;
                metadata.text_kind = match new.data_type {
                    DataType::Varchar => Some(catalog_ext::TextKind::Varchar),
                    DataType::Bpchar => Some(catalog_ext::TextKind::Bpchar),
                    _ => None,
                };
                ext.column_collations.remove(&new.id);
                if let Some(reference) = reference {
                    ext.column_collations.insert(new.id, column_collations::Binding::Column(reference));
                }
                let logical = match new.data_type {
                    DataType::Uuid => Some(catalog_ext::LogicalKind::Uuid),
                    DataType::Jsonb => Some(catalog_ext::LogicalKind::Jsonb),
                    DataType::IntArray => Some(catalog_ext::LogicalKind::IntArray),
                    DataType::BigIntArray => Some(catalog_ext::LogicalKind::BigIntArray),
                    DataType::TextArray => Some(catalog_ext::LogicalKind::TextArray),
                    DataType::TsVector => Some(catalog_ext::LogicalKind::TsVector),
                    DataType::TsQuery => Some(catalog_ext::LogicalKind::TsQuery),
                    _ => None,
                };
                if let Some(logical) = logical {
                    ext.logical_kinds.insert(new.id, logical);
                } else {
                    ext.logical_kinds.remove(&new.id);
                }
                Relational::normalize_ext(ext);

                let mut converted = BTreeMap::new();
                let (mut work, mut bytes) = (0, 0);
                for row in rows.values() {
                    if cancel() {
                        return Err(SqlError::Aborted);
                    }
                    let mut row = row.clone();
                    let value = if let Some(query) = &conversion {
                        query.project_row(&row.values, row.version, &[], &mut work, &mut bytes)?
                            .pop().ok_or(SqlError::Type)?
                    } else {
                        query::assignment::convert(row.values[at].clone(), &old_type, &new.data_type, clock)?
                    };
                    row.values[at] = coerce_to_column(&candidate, &new, value)?;
                    if converted.insert(pk(&table, &row.values)?, row).is_some() {
                        return Err(SqlError::Duplicate);
                    }
                }
                candidate.rows.insert(table.id, converted);
                let descriptors = candidate.secondary_descriptors_engine().to_vec();
                candidate.secondary = candidate.secondary_build_complete_engine(&descriptors, &mut *cancel)?;
            }
            Action::Drop(column, optional, cascade) => {
                let at = match position(&table, column) {
                    Ok(at) => at,
                    Err(SqlError::UnknownColumn) if *optional => continue,
                    Err(error) => return Err(error),
                };
                triggers::drop_column(&mut candidate, &table, &table.columns[at], *cascade)?;
                let id = table.columns[at].id;
                let dependents = generated_columns::dependent_columns(&candidate, &table, id);
                views::drop_column_views(
                    &mut candidate, &table, &table.columns[at], &dependents, *cascade, path,
                )?;
                if !dependents.is_empty() {
                    let dependent_drops = AlterTable {
                        table: name.clone(),
                        actions: dependents.into_iter().map(|column| Action::Drop(column.name.clone(), false, true)).collect(),
                        resolution_error: None,
                        column_names: alter.column_names,
                    };
                    execute(&mut candidate, &dependent_drops, path, clock, cancel)?;
                    table = candidate.tables[&name].clone();
                }
                let at = position(&table, column)?;
                let was_generated = Arc::make_mut(&mut candidate.ext).generated_columns.remove(&id).is_some();
                Arc::make_mut(&mut candidate.ext).gin_indexes.retain(|_, index| !index.columns.contains(&id));
                let collation_cascade=was_generated || *cascade && candidate.column_collation(id).is_some();
                let replace_primary=table.primary_key.contains(&at);
                if (table.logical_columns().count() <= 1 || replace_primary) && !collation_cascade {return Err(SqlError::Unsupported);}
                if collation_cascade {
                    for (owner,cs) in &mut std::sync::Arc::make_mut(&mut candidate.ext).constraints {
                        cs.retain(|c|match &c.kind {
                            ConstraintKind::ForeignKey {columns,ref_table,ref_columns,..}=>!columns.contains(&id) && !(*ref_table==table.id && ref_columns.contains(&id)),
                            ConstraintKind::PrimaryKey=>*owner!=table.id || !replace_primary,
                            _=>true
                        });
                    }
                    if replace_primary {std::sync::Arc::make_mut(&mut candidate.ext).primary_index_birth.remove(&table.id);}
                }
                if candidate.ext.constraints.values().flatten().any(|constraint| matches!(&constraint.kind, ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } if columns.contains(&id) || (*ref_table == table.id && ref_columns.contains(&id)))) { return Err(SqlError::Unsupported); }
                if let Some(cs) = std::sync::Arc::make_mut(&mut candidate.ext).constraints.get_mut(&table.id) {
                    cs.retain(|c| !matches!(&c.kind, ConstraintKind::Unique { columns, .. } if columns.contains(&id)));
                }
                constraints::drop_check_column(&mut candidate, &table, column)?;
                let descriptors = candidate
                    .secondary_descriptors_engine()
                    .iter()
                    .filter(|d| !(d.table == table.id && d.uses_column(id)))
                    .cloned()
                    .collect::<Vec<_>>();
                let removed_indexes=candidate.secondary_descriptors_engine().iter().filter(|d|d.table==table.id && d.uses_column(id)).map(|d|d.id).collect::<Vec<_>>();
                for id in removed_indexes {std::sync::Arc::make_mut(&mut candidate.ext).column_collations.remove(&id);}
                table.columns.remove(at);
                if replace_primary {
                    table.primary_key=vec![table.columns.len()];
                    table.columns.push(ColumnDef{id:allocate(&mut candidate.next_id)?,name:HIDDEN_ROWID.into(),data_type:DataType::BigInt,not_null:true});
                } else {for key in &mut table.primary_key {if *key>at {*key-=1;}}}
                let old_rows=candidate.rows.remove(&table.id).ok_or(SqlError::Type)?;
                let mut rows=BTreeMap::new();
                for (_,mut row) in old_rows {
                    row.values.remove(at);
                    if replace_primary {row.values.push(Value::Int(i64::try_from(row.version).map_err(|_|SqlError::Limit)?));}
                    rows.insert(pk(&table,&row.values)?,row);
                }
                candidate.rows.insert(table.id,rows);
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
                if table.logical_columns().next().is_none() {std::sync::Arc::make_mut(&mut candidate.ext).column_collations.insert(table.id,column_collations::Binding::EmptyTable);}
                candidate.drop_column_ext(id)?;
                candidate.secondary =
                    candidate.secondary_build_complete_engine(&descriptors, &mut *cancel)?;
            }
            Action::RenameColumn(old, new) => {
                let at = position(&table, old)?;
                alter.column_names.validate(new)?;
                if table.columns.iter().any(|c| &c.name == new) {
                    return Err(SqlError::DuplicateColumn);
                }
                generated_columns::rename_reference(&mut candidate, &table, &table.columns[at], new)?;
                triggers::rename_when_column(&mut candidate, table.id, &table.columns[at], new)?;
                constraints::rename_check_column(&mut candidate, &table, old, new)?;
                table.columns[at].name = new.clone();
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
            }
            Action::RenameConstraint(old, new) => {
                let backed_by_index = candidate.primary_key_constraint_name(&table).as_ref() == Some(old)
                    || candidate.table_constraints(table.id).iter().any(|constraint|
                        constraint.name == *old && matches!(constraint.kind, ConstraintKind::Unique { .. }));
                let key = relation_key(candidate.table_schema(table.id), new);
                if backed_by_index && sql_index::name_taken(&candidate, &key) {
                    return Err(sql_index::duplicate_relation(&key));
                }
                return Err(SqlError::Syntax);
            }
            Action::RenameTable(new) => {
                ident::ident(new)?;
                let key = relation_key(candidate.table_schema(table.id), new);
                if sql_index::name_taken(&candidate, &key) {
                    return Err(sql_index::duplicate_relation(&key));
                }
                constraint_primary_key::retain_name_before_rename(&mut candidate, &table)?;
                std::sync::Arc::make_mut(&mut candidate.tables).remove(&name);
                name = key;
                table.name = new.clone();
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
            }
            Action::Default(column, default) => {
                let at = position(&table, column)?;
                if let Some(generation) = candidate.generated_column(table.columns[at].id) {
                    user_types::view_replace_error(format!("column \"{column}\" of relation \"{}\" is a generated column", table.name));
                    if default.is_some() {
                        user_types::sql_error_hint(
                            "Use ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION instead.",
                        );
                    } else if generation.stored {
                        user_types::sql_error_hint(
                            "Use ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION instead.",
                        );
                    }
                    return Err(SqlError::Syntax);
                }
                if default.is_none() && alter.actions.iter().any(|action| {
                    matches!(action, Action::Default(name, Some(_)) if name == column)
                }) {
                    continue;
                }
                defaults::install_alter(
                    &mut candidate,
                    &table.columns[at],
                    default.as_deref(),
                    path,
                )?;
            }
            Action::Expression(column, expression, if_exists) => {
                let target = &table.columns[position(&table, column)?];
                generated_columns::alter(&mut candidate, &table, target, expression.as_ref(), *if_exists, cancel)?;
            }
            Action::NotNull(column, set) => {
                let at = position(&table, column)?;
                if !set && table.primary_key.contains(&at) {
                    user_types::view_replace_error(format!("column \"{column}\" is in a primary key"));
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
                    return Err(null_column_error(&table, column));
                }
                table.columns[at].not_null = *set;
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
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
