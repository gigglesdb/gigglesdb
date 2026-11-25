//! COMMENT parsing is catalog-free so ownership precedes semantic validation.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct Comment {
    pub table: String,
    column: Option<String>,
    view: bool,
    text: Option<String>,
}
pub(super) fn parse(p: &mut Parser) -> R<Comment> {
    p.need("on")?;
    let view = p.word("view");
    let column = if view || p.word("table") { false } else if p.word("column") { true }
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
    Ok(Comment { table, column, text, view })
}
impl Prepared {
    pub fn comment_table_target(sql: &str) -> R<Option<String>> {
        let mut tokens = Vec::new();
        let result = lex_into(sql, &mut tokens, false);
        if !matches!(tokens.first(), Some(Token::Word(w)) if w == "comment") { return Ok(None); }
        result?;
        if collations::claims(&tokens) || admin_claims(&tokens) { return Ok(None); }
        let mut parser = Parser { privilege_seen: false, privilege_casts: false, tokens, at: 1, params: BTreeSet::new(), relations: Vec::new() };
        Ok(Some(parse(&mut parser)?.table))
    }
    pub(crate) fn apply_comment(&self, state: &Relational, parts: &mut crate::storage::CatalogParts) -> R<()> {
        let Statement::Comment(comment) = self.statement.get() else { return Ok(()); };
        self.validate_schemas(state)?;
        let (id, columns) = if let Some(table) = parts.tables.get(&comment.table) {
            if comment.view { return Err(SqlError::WrongObjectType); }
            (table.id, table.logical_columns().map(|c| (c.id, c.name.as_str())).collect::<Vec<_>>())
        } else {
            let id = state.view_names.get(&comment.table).map(|(id, _)| id).ok_or(SqlError::UnknownTable)?;
            let view = parts.sql_views.get(id).ok_or(SqlError::UnknownTable)?;
            (*id, view.columns.iter().map(|c| (c.id, c.name.as_str())).collect())
        };
        let column = match &comment.column {
            None => 0,
            Some(name) => columns.iter().find(|(_, n)| *n == name).ok_or(SqlError::UnknownColumn)?.0,
        };
        if comment.text.as_ref().is_some_and(|s| s.len() > 65_536) { return Err(SqlError::Limit); }
        let key = (id, column);
        match &comment.text {
            Some(text) if !text.is_empty() => { parts.comments.insert(key, text.clone()); }
            _ => { parts.comments.remove(&key); }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) enum AdminTarget {
    Tablespace(String),
    Extension(String),
    Language(String),
    Transform(TransformType, String),
}

#[derive(Debug, Clone)]
pub(crate) enum TransformType {
    Builtin(String),
    Named(user_types::TypeReference),
}

pub(super) fn admin_claims(tokens: &[Token]) -> bool {
    matches!(tokens, [Token::Word(verb), Token::Word(on), Token::Word(kind), ..]
        if verb == "comment" && on == "on"
            && matches!(kind.as_str(), "tablespace" | "extension" | "language" | "transform"))
}

pub(super) fn parse_admin(p: &mut Parser) -> R<AdminTarget> {
    p.need("on")?;
    let kind = p.raw_name()?;
    let name = |p: &mut Parser| -> R<String> {
        Ok(ident::truncate(&p.raw_name()?, 63).to_owned())
    };
    let target = match kind.as_str() {
        "tablespace" => AdminTarget::Tablespace(name(p)?),
        "extension" => AdminTarget::Extension(name(p)?),
        "language" => AdminTarget::Language(name(p)?),
        "transform" => {
            p.need("for")?;
            let ty = if let Some(display) = query::transform_builtin_type(p)? {
                TransformType::Builtin(display)
            } else {
                TransformType::Named(user_types::TypeReference::parse(p)?)
            };
            p.need("language")?;
            AdminTarget::Transform(ty, name(p)?)
        }
        _ => return Err(SqlError::Syntax),
    };
    p.need("is")?;
    if !p.word("null") {
        admin_utility::string(p)?;
    }
    Ok(target)
}

fn language_exists(name: &str) -> bool {
    giggles_pg_types::admin_catalog::LANGUAGES.iter().any(|&(_, builtin, ..)| builtin == name)
}

pub(super) fn execute_admin(
    target: &AdminTarget, prepared: &Prepared, state: &Relational,
    actor: Option<&crate::roles::CatalogRole>, roles: &crate::roles::RoleCatalog,
) -> Result<(), crate::CoreError> {
    let (kind, name, owner) = match target {
        AdminTarget::Tablespace(name) if tablespace::exists(name) => ("tablespace", name, None),
        AdminTarget::Tablespace(name) => return Err(crate::CoreError::Sql(admin_utility::missing("tablespace", name))),
        AdminTarget::Extension(name) if name == "plpgsql" => ("extension", name, None),
        AdminTarget::Extension(name) => {
            let extension = state.ext.extensions.get(name)
                .ok_or_else(|| crate::CoreError::Sql(admin_utility::missing("extension", name)))?;
            ("extension", name, Some(extension.owner))
        }
        AdminTarget::Language(name) if language_exists(name) => ("language", name, None),
        AdminTarget::Language(name) => return Err(crate::CoreError::Sql(admin_utility::missing("language", name))),
        AdminTarget::Transform(ty, language) => {
            let name = match ty {
                TransformType::Builtin(display) => display.clone(),
                TransformType::Named(reference) => {
                    let path = &prepared.search_path;
                    if let Some((_, ty)) = state.resolve_type(&reference.name, path).map_err(crate::CoreError::Sql)? {
                        let _display = user_types::TypeDisplayScope::enter(path, Some(state));
                        user_types::enum_display(ty, reference.array)
                    } else {
                        let key = state.resolve_relation_key(&reference.name, path);
                        let (schema, name) = schema_binding::split_key(&key);
                        if !state.tables.contains_key(&key) && !state.view_names.contains_key(&key) {
                            return Err(crate::CoreError::Sql(
                                admin_utility::missing("type", &reference.name.replace('\0', "."))
                            ));
                        }
                        let visible = state.resolve_relation_key(name, path) == key;
                        user_types::display_type_name(name, (!visible).then_some(schema), reference.array)
                    }
                }
            };
            if !language_exists(language) {
                return Err(crate::CoreError::Sql(admin_utility::missing("language", language)));
            }
            return Err(crate::CoreError::Sql(user_types::exact_sql_error(
                SqlError::UndefinedObject,
                format!("transform for type {name} language \"{language}\" does not exist"),
                None,
            )));
        }
    };
    if let Some(actor) = actor.filter(|actor| !actor.superuser) {
        let image = roles.image().map_err(admin_utility::role_error)?;
        let privileges = image.privileges_of(&actor.name).map_err(admin_utility::role_error)?;
        let owns_object = if let Some(oid) = owner {
            let candidates = image.list().map_err(admin_utility::role_error)?;
            candidates.iter().any(|role| role.oid == oid && privileges.contains(&role.name))
        } else {
            // These rows use the synthetic bootstrap identity (OID 10).
            privileges.contains("giggles")
        };
        if !owns_object {
            return Err(crate::CoreError::Sql(admin_utility::must_own(kind, name)));
        }
    }
    Err(crate::CoreError::Sql(SqlError::Unsupported))
}
