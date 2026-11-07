//! SQL spelling for blocking secondary index creation.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct CreateIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

fn name(p: &mut Parser) -> R<String> {
    match p.tokens.get(p.at) {
        Some(Token::QuotedIdent(value)) => {
            if value.bytes().any(|b| b.is_ascii_uppercase()) || ident(value).is_err() {
                return Err(SqlError::Unsupported);
            }
            let value = value.clone();
            p.at += 1;
            Ok(value)
        }
        _ => p.name(),
    }
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
        let value = name(p)?;
        p.need("on")?;
        Some(value)
    };
    p.word("only");
    let table = name(p)?;
    if p.word("using") && !p.word("btree") {
        return Err(SqlError::Unsupported);
    }
    p.punct('(')?;
    if p.sym(')') {
        return Err(SqlError::Unsupported);
    }
    let mut columns = Vec::new();
    loop {
        if matches!(
            p.tokens.get(p.at),
            Some(Token::Symbol('(') | Token::Number(_) | Token::String(_) | Token::Param(_))
        ) {
            return Err(SqlError::Unsupported);
        }
        columns.push(name(p)?);
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
    if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if ["include", "where", "with", "tablespace"].contains(&w.as_str()))
    {
        return Err(SqlError::Unsupported);
    }
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    let name = match explicit_name {
        Some(value) => value,
        None => {
            let mut column_part = columns.join("_");
            let available = 63usize.saturating_sub(table.len() + 5);
            column_part.truncate(available);
            let value = format!("{table}_{column_part}_idx");
            ident(&value).map_err(|_| SqlError::Unsupported)?;
            value
        }
    };
    Ok(CreateIndex {
        name,
        table,
        columns,
        unique,
        if_not_exists,
    })
}

#[cfg(test)]
#[path = "sql_index_tests.rs"]
mod sql_index_tests;
