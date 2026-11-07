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
    loop {
        if matches!(
            p.tokens.get(p.at),
            Some(Token::Symbol('(') | Token::Number(_) | Token::String(_) | Token::Param(_))
        ) || (matches!(p.tokens.get(p.at), Some(Token::Op(sign)) if sign == "+" || sign == "-")
            && matches!(p.tokens.get(p.at + 1), Some(Token::Number(_))))
        {
            return Err(SqlError::Unsupported);
        }
        columns.push(p.name()?);
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
            let table_part = ident::truncate(&table, 58);
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
        unique,
        if_not_exists,
    })
}

#[cfg(test)]
#[path = "sql_index_tests.rs"]
mod sql_index_tests;
