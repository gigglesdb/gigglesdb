//! COMMENT parsing is catalog-free so ownership precedes semantic validation.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct Comment {
    pub table: String,
    column: Option<String>,
    text: Option<String>,
}
pub(super) fn parse(p: &mut Parser) -> R<Comment> {
    p.need("on")?;
    let column = if p.word("table") { false } else if p.word("column") { true }
        else { return Err(SqlError::Unsupported); };
    let mut names = vec![alter_table::raw_name(p)?];
    while p.sym('.') { names.push(alter_table::raw_name(p)?); }
    let column = if column { Some(names.pop().ok_or(SqlError::Syntax)?) } else { None };
    let (schema, name) = match names.as_slice() {
        [name] => (None, name.clone()),
        [schema, name] => (Some(schema.clone()), name.clone()),
        _ => return Err(SqlError::Syntax),
    };
    p.relations.push(schemas::Reference::read(name.clone(), schema.clone()));
    let table = schema.map_or(name.clone(), |s| format!("{s}\0{name}"));
    p.need("is")?;
    let text = if p.word("null") { None } else {
        let Some(Token::String(text)) = p.tokens.get(p.at) else { return Err(SqlError::Syntax); };
        p.at += 1;
        Some(text.clone())
    };
    p.sym(';');
    if p.at != p.tokens.len() { return Err(SqlError::Syntax); }
    Ok(Comment { table, column, text })
}
impl Prepared {
    pub fn comment_table_target(sql: &str) -> R<Option<String>> {
        let mut tokens = Vec::new();
        let result = lex_into(sql, &mut tokens, false);
        if !matches!(tokens.first(), Some(Token::Word(w)) if w == "comment") { return Ok(None); }
        result?;
        let mut parser = Parser { tokens, at: 1, params: BTreeSet::new(), relations: Vec::new() };
        Ok(Some(parse(&mut parser)?.table))
    }
    pub(crate) fn apply_comment(&self, tables: &BTreeMap<String, TableDef>, comments: &mut BTreeMap<(u64, u64), String>) -> R<()> {
        let Statement::Comment(comment) = &self.statement else { return Ok(()); };
        let table = tables.get(&comment.table).ok_or(SqlError::UnknownTable)?;
        let column = match &comment.column {
            None => 0,
            Some(name) => table.logical_columns().find(|c| c.name == *name)
                .ok_or(SqlError::UnknownColumn)?.id,
        };
        if comment.text.as_ref().is_some_and(|s| s.len() > 65_536) { return Err(SqlError::Limit); }
        let key = (table.id, column);
        match &comment.text {
            Some(text) if !text.is_empty() => { comments.insert(key, text.clone()); }
            _ => { comments.remove(&key); }
        }
        Ok(())
    }
}
