//! SQL spelling for blocking secondary index creation.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct CreateIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub collations: Vec<Option<String>>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub predicate: Option<Vec<Token>>,
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
    if p.word("using") && !p.word("btree") {
        return Err(SqlError::Unsupported);
    }
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
            return Err(SqlError::Unsupported);
        }
        columns.push(p.column_name()?);
        collations.push(if p.word("collate") {Some(column_collations::parse_name(p)?)} else {None});
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
            if ![
                "text_pattern_ops", "varchar_pattern_ops", "bpchar_pattern_ops",
                "array_ops", "bit_ops", "bool_ops", "bpchar_ops", "bytea_ops", "char_ops",
                "date_ops", "enum_ops", "float4_ops", "float8_ops", "inet_ops", "int2_ops",
                "int4_ops", "int8_ops", "interval_ops", "jsonb_ops", "macaddr8_ops", "macaddr_ops",
                "money_ops", "multirange_ops", "name_ops", "numeric_ops", "oid_ops", "oidvector_ops",
                "pg_lsn_ops", "range_ops", "record_ops", "text_ops", "tid_ops", "time_ops",
                "timestamp_ops", "timestamptz_ops", "timetz_ops", "tsquery_ops", "tsvector_ops",
                "uuid_ops", "varbit_ops", "varchar_ops", "xid8_ops",
            ].contains(&opclass.as_str()) { return Err(SqlError::UndefinedObject); }
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
    if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if ["include", "with", "tablespace"].contains(&w.as_str()))
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
    let name = match explicit_name {
        Some(value) => value,
        None => {
            let table_part = ident::truncate(split_key(&table).1, 58);
            let available = 63usize.saturating_sub(table_part.len() + 5);
            let column_part = columns.join("_");
            let column_part = ident::truncate(&column_part, available);
            let value = format!("{table_part}_{column_part}_idx");
            ident(&value)?;
            value
        }
    };
    Ok(CreateIndex {
        name,
        table,
        columns,
        collations,
        unique,
        if_not_exists,
        predicate,
    })
}

#[derive(Debug, Clone)]
pub(super) struct DropIndex {
    pub name: String,
    pub table: Option<String>,
    pub optional: bool,
    pub cascade: bool,
}

pub(super) fn primary_table<'a>(state: &'a Relational, name: &str) -> Option<&'a TableDef> {
    state.tables.values().find(|table| {
        constraint_primary_key::primary_key_name(state, table)
            .is_some_and(|key| relation_key(state.table_schema(table.id), &key) == name)
    })
}
pub(super) fn parse_drop(p: &mut Parser) -> R<DropIndex> {
    parse_drop_impl(p, false)
}

// Parse-only authorization hint: reserved names still fail the execution parser.
pub(super) fn parse_drop_authority_target(p: &mut Parser) -> R<DropIndex> {
    parse_drop_impl(p, true)
}

fn parse_drop_impl(p: &mut Parser, authority_only: bool) -> R<DropIndex> {
    p.need("drop")?;
    p.need("index")?;
    if p.word("concurrently") {
        return Err(SqlError::Unsupported);
    }
    let optional = p.word("if");
    if optional {
        p.need("exists")?;
    }
    let name = if authority_only {
        let first = p.raw_name()?;
        if p.sym('.') { relation_key(&first, &p.raw_name()?) } else { first }
    } else {
        p.table_name()?
    };
    let cascade = p.word("cascade");
    if !cascade {
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
    })
}

#[cfg(test)]
#[path = "sql_index_tests.rs"]
mod sql_index_tests;
