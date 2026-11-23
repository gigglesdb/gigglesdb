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
        Option<String>,
    ),
    Type(String, DataType, Option<i32>, Option<String>, Option<String>),
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
                Action::Add(_, _, Some(sql), ..) | Action::Default(_, Some(sql)) => Some(sql),
                _ => None,
            })
            .flat_map(|sql| defaults::dependencies(sql).unwrap_or_default().to_vec())
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
    let mut unsupported_type=false;

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
                let if_not_exists = p.word("if");
                if if_not_exists {
                    p.need("not")?;
                    p.need("exists")?;
                }
                let name = raw_name(p)?;
                if !matches!(p.tokens.get(p.at), Some(Token::Word(_))) {
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
                ) = parse_add_definition(p, if_not_exists)?;
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
                let (parsed,user_type)=parse_type(p)?;
                let collation=if p.word("collate") {Some(column_collations::parse_name(p)?)} else {None};
                if let Some(error)=parsed.semantic_error {return Err(error);}
                unsupported_type |= collation.is_none() && user_type.is_none() && !matches!(parsed.data_type,DataType::Text|DataType::Varchar|DataType::Bpchar);
                actions.push(Action::Type(name,parsed.data_type,parsed.typmod,collation,user_type));
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

    if unsupported_type {return Err(SqlError::Unsupported);}
    Ok(AlterTable {
        table,
        actions,
        resolution_error,
        column_names: ColumnNamePolicy::Sql,
    })
}
fn parse_type(p:&mut Parser)->R<(ddl_type::Parsed,Option<String>)> {
    let start=p.at;
    match ddl_type::parse(p,ddl_type::Site::Alter) {
        Ok(parsed)=>Ok((parsed,None)),
        Err(e) if user_type_fallback(p, start, &e) => { p.at=start; let n=user_types::parse_name(p)?; Ok((ddl_type::Parsed {data_type:DataType::Text,typmod:None,semantic_error:None},Some(n))) },
        Err(e)=>Err(e),
    }
}
// Retry only an unknown or quoted type name. Built-in type errors (including
// unsupported arrays) must retain the error returned by the DDL type parser.
fn user_type_fallback(p: &Parser, start: usize, error: &SqlError) -> bool {
    match error {
        SqlError::Syntax => matches!(p.tokens.get(start), Some(Token::QuotedIdent(_))),
        SqlError::Unsupported if p.at == start + 1 => match p.tokens.get(p.at) {
            None | Some(Token::Symbol(',' | ';' | '.')) => true,
            Some(Token::Word(w)) => matches!(w.as_str(), "collate" | "not" | "null" | "default"),
            _ => false,
        },
        _ => false,
    }
}
#[allow(clippy::type_complexity)]
fn parse_add_definition(
    p: &mut Parser,
    if_not_exists: bool,
) -> R<(ddl_type::Parsed, bool, Option<String>, Option<String>, Option<String>)> {
    let type_start = p.at;
    let mut user_type = None;
    let mut parsed = match ddl_type::parse(p, ddl_type::Site::Alter) {
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
            ddl_type::Parsed {
                data_type: DataType::Text,
                typmod: None,
                semantic_error: Some(SqlError::Unsupported),
            }
        }
        Err(error) if user_type_fallback(p, type_start, &error) => {
            p.at=type_start;
            user_type=Some(user_types::parse_name(p)?);
            ddl_type::Parsed {data_type:DataType::Text,typmod:None,semantic_error:None}
        }
        Err(error) => return Err(error),
    };
    let mut collation=None;
    let mut not_null = false;
    let mut default = None;
    loop {
        if p.word("collate") { collation=Some(column_collations::parse_name(p)?);
        } else if p.word("not") {
            p.need("null")?;
            not_null = true;
        } else if p.word("default") {
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
        } else {
            break;
        }
    }
    Ok((parsed, not_null, default, collation, user_type))
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
            Action::Add(column, typmod, default, semantic_error, if_not_exists, collation, user_type) => {
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
                    return Err(SqlError::DuplicateColumn);
                }
                if table.columns.len() >= MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                let mut column = column.clone();
                let mut typmod=*typmod;
                let type_id=if let Some(n)=user_type {
                    let (id,ty)=candidate.resolve_type(n,path)?.ok_or(SqlError::Unsupported)?;
                    column.data_type=ty.storage_type()?;
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
            Action::Type(column,ty,typmod,collation,user_type)=>{
                let at=position(&table,column)?;
                let old=table.columns[at].clone();
                let mut new=old.clone(); new.data_type=ty.clone();
                let type_id=if let Some(n)=user_type {let (id,ty)=candidate.resolve_type(n,path)?.ok_or(SqlError::UndefinedObject)?;new.data_type=ty.storage_type()?;Some(id)} else {None};
                // The v2 layout rewrite leaves same-width rows and existing
                // index tuples in place. Admit only a byte-preserving text change.
                let representation_unchanged = matches!(
                    (&old.data_type, &new.data_type),
                    (DataType::Text, DataType::Varchar) | (DataType::Varchar, DataType::Text)
                ) && typmod.is_none() && type_id.is_none() && collation.is_none()
                    && candidate.column_collation(old.id).is_none()
                    && candidate.ext.columns.get(&old.id).is_none_or(|c| c.type_id.is_none());
                if !representation_unchanged && candidate.secondary_descriptors_engine().iter().any(|d| {
                    d.table == table.id && d.predicate.as_ref().is_some_and(|p| p.columns.contains(&old.id))
                }) {
                    return Err(SqlError::Unsupported);
                }
                let reference=column_collations::resolve_column(&candidate,&new,type_id,collation.as_deref(),path)?;
                let rows=candidate.rows.get(&table.id).ok_or(SqlError::Type)?.clone();
                table.columns[at]=new.clone();
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(),table.clone());
                let metadata=std::sync::Arc::make_mut(&mut candidate.ext).columns.entry(new.id).or_default();
                metadata.type_id=type_id;metadata.typmod = *typmod;
                metadata.text_kind=match new.data_type {DataType::Varchar=>Some(catalog_ext::TextKind::Varchar),DataType::Bpchar=>Some(catalog_ext::TextKind::Bpchar),_=>None};
                std::sync::Arc::make_mut(&mut candidate.ext).column_collations.remove(&new.id);
                if let Some(r)=reference {std::sync::Arc::make_mut(&mut candidate.ext).column_collations.insert(new.id,column_collations::Binding::Column(r));}
                Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));

                let mut converted=BTreeMap::new();
                for (_,mut row) in rows {
                    row.values[at]=coerce_to_column(&candidate,&new,query::assignment::convert(row.values[at].clone(),&old.data_type,&new.data_type,clock)?)?;
                    if converted.insert(pk(&table,&row.values)?,row).is_some() {return Err(SqlError::Duplicate);}
                }
                candidate.rows.insert(table.id,converted);
                let descriptors=candidate.secondary_descriptors_engine().to_vec();
                candidate.secondary=candidate.secondary_build_complete_engine(&descriptors,&mut *cancel)?;
            }
            Action::Drop(column, optional, cascade) => {
                let at = match position(&table, column) {
                    Ok(at) => at,
                    Err(SqlError::UnknownColumn) if *optional => continue,
                    Err(error) => return Err(error),
                };
                views::drop_column_views(&mut candidate, table.columns[at].id, *cascade)?;
                let id = table.columns[at].id;
                let collation_cascade=*cascade && candidate.column_collation(id).is_some();
                let replace_primary=table.primary_key.contains(&at);
                if (table.logical_columns().count() <= 1 || replace_primary) && !collation_cascade {return Err(SqlError::Unsupported);}
                if collation_cascade {
                    for (owner,cs) in &mut std::sync::Arc::make_mut(&mut candidate.ext).constraints {
                        cs.retain(|c|match &c.kind {
                            ConstraintKind::ForeignKey {columns,ref_table,ref_columns,..}=>!columns.contains(&id) && !(*ref_table==table.id && ref_columns.contains(&id)),
                            ConstraintKind::PrimaryKey=>*owner!=table.id || !replace_primary,
                            ConstraintKind::Check {expr,..} if *owner==table.id=>!lex(expr).unwrap_or_default().iter().any(|t|matches!(t,Token::Word(n)|Token::QuotedIdent(n) if n==column)),
                            _=>true
                        });
                    }
                    if replace_primary {std::sync::Arc::make_mut(&mut candidate.ext).primary_index_birth.remove(&table.id);}
                }
                if candidate.ext.constraints.values().flatten().any(|constraint| matches!(&constraint.kind, ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } if columns.contains(&id) || (*ref_table == table.id && ref_columns.contains(&id)))) { return Err(SqlError::Unsupported); }
                if let Some(cs) = std::sync::Arc::make_mut(&mut candidate.ext).constraints.get_mut(&table.id) {
                    cs.retain(|c| !matches!(&c.kind, ConstraintKind::Unique { columns, .. } if columns.contains(&id)));
                }
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
                table.columns[at].name = new.clone();
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
            }
            Action::RenameTable(new) => {
                ident::ident(new)?;
                if candidate
                    .relation_name_taken(&relation_key(candidate.table_schema(table.id), new))
                {
                    return Err(SqlError::DuplicateTable);
                }
                constraint_primary_key::retain_name_before_rename(&mut candidate, &table)?;
                std::sync::Arc::make_mut(&mut candidate.tables).remove(&name);
                name = relation_key(candidate.table_schema(table.id), new);
                table.name = new.clone();
                std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
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
