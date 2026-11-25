//! SQL spelling for blocking secondary index creation.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct CreateIndex {
    pub name: String,
    pub automatic_name: bool,
    pub table: String,
    pub columns: Vec<String>,
    pub collations: Vec<Option<String>>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub predicate: Option<Vec<Token>>,
    pub gin: Option<gin_indexes::Definition>,
}

fn opclass_name(p: &mut Parser) -> R<String> {
    let Some(Token::Word(name) | Token::QuotedIdent(name)) = p.tokens.get(p.at) else {
        return Err(SqlError::Syntax);
    };
    let name = ident::truncate(name, 63).to_owned();
    p.at += 1;
    Ok(name)
}

pub(super) fn parse(p: &mut Parser) -> R<CreateIndex> {
    p.need("create")?;
    let unique = p.word("unique");
    p.need("index")?;
    if p.word("concurrently") {
        return Err(SqlError::Unsupported);
    }
    let if_not_exists = p.word("if");
    if if_not_exists {
        p.need("not")?;
        p.need("exists")?;
    }
    let explicit_name = if p.word("on") {
        None
    } else {
        let value = p.name()?;
        p.need("on")?;
        Some(value)
    };
    p.word("only");
    let table = p.table_name()?;
    let mut gin = if p.word("using") {
        let method = opclass_name(p)?;
        match method.as_str() {
            "gin" => Some(gin_indexes::Definition::default()),
            "btree" => None,
            "brin" | "gist" | "hash" | "heap" | "spgist" => return Err(SqlError::Unsupported),
            _ => {
                user_types::view_replace_error(format!("access method \"{method}\" does not exist"));
                return Err(SqlError::UndefinedObject);
            }
        }
    } else { None };
    p.punct('(')?;
    if p.sym(')') {
        return Err(SqlError::Unsupported);
    }
    let mut columns = Vec::new();
    let mut collations = Vec::new();
    loop {
        if matches!(
            p.tokens.get(p.at),
            Some(Token::Symbol('(') | Token::Number(_) | Token::String(_) | Token::Param(_))
        ) || (matches!(p.tokens.get(p.at), Some(Token::Op(sign)) if sign == "+" || sign == "-")
            && matches!(p.tokens.get(p.at + 1), Some(Token::Number(_))))
        {
            if gin.is_some() {
                user_types::view_replace_error("expression GIN indexes are not supported".into());
            }
            return Err(SqlError::Unsupported);
        }
        columns.push(p.column_name()?);
        if gin.is_some() && p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
            user_types::view_replace_error("expression GIN indexes are not supported".into());
            return Err(SqlError::Unsupported);
        }
        collations.push(if p.word("collate") {Some(column_collations::parse_name(p)?)} else {None});
        let mut explicit_class = None;
        if matches!(p.tokens.get(p.at), Some(Token::QuotedIdent(_)))
            || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if !["asc", "desc", "nulls", "collate"].contains(&w.as_str()))
        {
            let mut opclass = opclass_name(p)?;
            if p.sym('.') {
                let schema = opclass;
                opclass = opclass_name(p)?;
                if schema != "pg_catalog" { return Err(SqlError::UndefinedObject); }
            }
            // Syntax-only compatibility: descriptors and indclass retain the default.
            if gin.is_none() && ![
                "text_pattern_ops", "varchar_pattern_ops", "bpchar_pattern_ops",
                "array_ops", "bit_ops", "bool_ops", "bpchar_ops", "bytea_ops", "char_ops",
                "date_ops", "enum_ops", "float4_ops", "float8_ops", "inet_ops", "int2_ops",
                "int4_ops", "int8_ops", "interval_ops", "jsonb_ops", "macaddr8_ops", "macaddr_ops",
                "money_ops", "multirange_ops", "name_ops", "numeric_ops", "oid_ops", "oidvector_ops",
                "pg_lsn_ops", "range_ops", "record_ops", "text_ops", "tid_ops", "time_ops",
                "timestamp_ops", "timestamptz_ops", "timetz_ops", "tsquery_ops", "tsvector_ops",
                "uuid_ops", "varbit_ops", "varchar_ops", "xid8_ops",
            ].contains(&opclass.as_str()) { return Err(SqlError::UndefinedObject); }
            explicit_class = Some(opclass);
        }
        if let Some(gin) = &mut gin {
            gin.opclasses.push(explicit_class);
            if matches!(p.tokens.get(p.at), Some(Token::Word(word)) if matches!(word.as_str(), "asc" | "desc" | "nulls")) {
                return Err(SqlError::Unsupported);
            }
        }
        p.word("asc");
        if p.word("nulls") {
            if p.word("first") {
                return Err(SqlError::Unsupported);
            }
            p.need("last")?;
        }
        if p.sym(',') {
            continue;
        }
        if p.sym(')') {
            break;
        }
        if p.tokens.get(p.at).is_none() || p.tokens.get(p.at) == Some(&Token::Symbol(';')) {
            return Err(SqlError::Syntax);
        }
        return Err(SqlError::Unsupported);
    }
    if let Some(gin) = &mut gin
        && p.word("with")
    {
        gin.options = gin_indexes::parse_options(p)?;
    }
    if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if ["include", "with", "tablespace"].contains(&w.as_str()) || (gin.is_some() && w == "where"))
    {
        return Err(SqlError::Unsupported);
    }
    let predicate = if p.word("where") {
        let tokens = partial_index::predicate_tokens(p, ";");
        if tokens.is_empty() {
            return Err(SqlError::Syntax);
        }
        Some(tokens)
    } else {
        None
    };
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    let automatic_name = explicit_name.is_none();
    let name = explicit_name.unwrap_or_else(|| {
        catalog_ext::constraint_object_name(split_key(&table).1, &columns.join("_"), "idx")
    });
    ident(&name)?;
    Ok(CreateIndex {
        name,
        automatic_name,
        table,
        columns,
        collations,
        unique,
        if_not_exists,
        predicate,
        gin,
    })
}

impl CreateIndex {
    pub(super) fn choose_name(&self, state: &Relational, table: &TableDef) -> String {
        let columns = self.columns.join("_");
        for attempt in 0u64.. {
            let label = if attempt == 0 { "idx".into() } else { format!("idx{attempt}") };
            let name = catalog_ext::constraint_object_name(&table.name, &columns, &label);
            if !name_taken(state, &relation_key(state.table_schema(table.id), &name)) {
                return name;
            }
        }
        unreachable!("bounded catalog cannot exhaust index names")
    }
}

#[derive(Debug, Clone)]
pub(super) struct DropIndex {
    pub name: String,
    pub table: Option<String>,
    pub optional: bool,
    pub cascade: bool,
    pub rename: Option<String>,
}

pub(super) fn primary_table<'a>(state: &'a Relational, name: &str) -> Option<&'a TableDef> {
    state.tables.values().find(|table| {
        constraint_primary_key::primary_key_name(state, table)
            .is_some_and(|key| relation_key(state.table_schema(table.id), &key) == name)
    })
}

pub(super) fn name_taken(state: &Relational, key: &str) -> bool {
    state.relation_name_taken(key) || primary_table(state, key).is_some()
}

pub(super) fn duplicate_relation(name: &str) -> SqlError {
    user_types::view_replace_error(format!("relation \"{}\" already exists", split_key(name).1));
    SqlError::DuplicateTable
}
pub(super) fn parse_drop(p: &mut Parser) -> R<DropIndex> {
    parse_drop_impl(p, false)
}

// Parse-only authorization hint: reserved names still fail the execution parser.
pub(super) fn parse_drop_authority_target(p: &mut Parser) -> R<DropIndex> {
    parse_drop_impl(p, true)
}

fn parse_drop_impl(p: &mut Parser, authority_only: bool) -> R<DropIndex> {
    let alter = p.word("alter");
    if !alter {
        p.need("drop")?;
    }
    p.need("index")?;
    if p.word("concurrently") {
        return Err(if alter { SqlError::Syntax } else { SqlError::Unsupported });
    }
    let optional = p.word("if");
    if optional {
        p.need("exists")?;
    }
    let name = if authority_only {
        let first = p.raw_name()?;
        if p.sym('.') { relation_key(&first, &p.raw_name()?) } else { first }
    } else if alter {
        // Resolve the index before applying name policy so a secondary-index
        // rename can diagnose an occupied destination.
        let first = p.raw_name()?;
        let (schema, name) = if p.sym('.') {
            (Some(first), p.raw_name()?)
        } else {
            (None, first)
        };
        p.relations.push(schemas::Reference::read(name.clone(), schema.clone()));
        schema.map_or(name.clone(), |schema| format!("{schema}\0{name}"))
    } else {
        p.table_name()?
    };
    let rename = if alter {
        p.need("rename")?;
        p.need("to")?;
        Some(p.raw_name()?)
    } else { None };
    let cascade = !alter && p.word("cascade");
    if !alter && !cascade {
        p.word("restrict");
    }
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(DropIndex {
        name,
        table: None,
        optional,
        cascade,
        rename,
    })
}

#[cfg(test)]
#[path = "sql_index_tests.rs"]
mod sql_index_tests;
