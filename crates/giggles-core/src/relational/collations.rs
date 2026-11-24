//! User collation definitions shared by the legacy and v2 catalogs.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Collation {
    #[serde(deserialize_with = "super::catalog_ext::name")]
    pub name: String,
    #[serde(deserialize_with = "super::catalog_ext::name")]
    pub schema: String,
    #[serde(deserialize_with = "super::catalog_ext::name")]
    pub owner: String,
    pub provider: String,
    pub deterministic: bool,
    pub encoding: i32,
    pub collate: Option<String>,
    pub ctype: Option<String>,
    pub locale: Option<String>,
    pub rules: Option<String>,
    pub version: Option<String>,
    pub comment: Option<String>,
}
pub(crate) fn validate(c: &Collation) -> R<()> {
    ident(&c.name)?;
    ident(&c.schema)?;
    if c.owner.is_empty() || c.owner.len() > 63 || c.owner.contains('\0') {
        return Err(SqlError::Type);
    }
    if !matches!(c.provider.as_str(), "i" | "c" | "b")
        || !(c.encoding == if c.provider == "i" { -1 } else { 6 }
            || (c.provider == "c"
                && c.encoding == -1
                && matches!(c.collate.as_deref(), Some("C" | "POSIX"))
                && c.collate == c.ctype))
        || (c.provider == "c" && (c.collate.is_none() || c.ctype.is_none() || c.locale.is_some()))
        || (c.provider != "c" && (c.locale.is_none() || c.collate.is_some() || c.ctype.is_some()))
        || (c.provider != "i" && (!c.deterministic || c.rules.is_some()))
        || (c.provider == "b"
            && !matches!(
                c.locale.as_deref(),
                Some("C" | "C.UTF-8" | "PG_UNICODE_FAST")
            ))
        || (c.provider == "c"
            && [&c.collate, &c.ctype]
                .into_iter()
                .any(|v| !matches!(v.as_deref(), Some("C" | "POSIX" | "C.utf8" | "en_US.utf8"))))
        || [
            &c.collate, &c.ctype, &c.locale, &c.rules, &c.version, &c.comment,
        ]
        .into_iter()
        .flatten()
        .any(|s| s.len() > 65536 || s.contains('\0'))
    {
        return Err(SqlError::Type);
    }
    Ok(())
}
fn error<T>(code: SqlError, message: String) -> R<T> {
    user_types::view_replace_error(message);
    Err(code)
}
#[derive(Debug, Clone)]
pub(super) enum Action {
    Create {
        optional: bool,
        options: BTreeMap<String, String>,
        from: Option<String>,
    },
    Drop {
        optional: bool,
        names: Vec<String>,
        cascade: bool,
    },
    Rename(String),
    Owner(String),
    Schema(String),
    Refresh,
    Comment(Option<String>),
}
#[derive(Debug, Clone)]
pub(super) struct Ddl {
    name: String,
    action: Action,
}
impl Ddl {
    pub(super) fn command(&self) -> &'static str {
        match self.action {
            Action::Create { .. } => "CREATE COLLATION",
            Action::Drop { .. } => "DROP COLLATION",
            Action::Comment(_) => "COMMENT",
            _ => "ALTER COLLATION",
        }
    }
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens.first(), Some(Token::Word(w)) if matches!(w.as_str(), "create" | "alter" | "drop"))
        && matches!(tokens.get(1), Some(Token::Word(w)) if w == "collation")
        || matches!(tokens.first(), Some(Token::Word(w)) if w == "comment")
            && matches!(tokens.get(1), Some(Token::Word(w)) if w == "on")
            && matches!(tokens.get(2), Some(Token::Word(w)) if w == "collation")
}
fn option(p: &mut Parser) -> R<String> {
    let v = match p.tokens.get(p.at) {
        Some(Token::Word(v) | Token::String(v)) => v.clone(),
        _ => return Err(SqlError::Syntax),
    };
    p.at += 1;
    Ok(v)
}
pub(super) fn parse(p: &mut Parser) -> R<Ddl> {
    let create = p.word("create");
    let drop = p.word("drop");
    let comment = p.word("comment");
    if comment {
        p.need("on")?;
    } else if !create && !drop {
        p.need("alter")?;
    }
    p.need("collation")?;
    let optional = (create || drop) && p.word("if");
    if optional {
        if create {
            p.need("not")?;
        }
        p.need("exists")?;
    }
    let name = user_types::parse_name(p)?;
    let action = if create {
        if p.word("from") {
            Action::Create {
                optional,
                options: BTreeMap::new(),
                from: Some(user_types::parse_name(p)?),
            }
        } else {
            p.punct('(')?;
            let mut options = BTreeMap::new();
            loop {
                let key = p.name()?;
                if matches!(p.tokens.get(p.at), Some(Token::Op(op)) if op == "=") {
                    p.at += 1;
                }
                let v = option(p)?;
                if options.insert(key, v).is_some() {
                    return Err(SqlError::Syntax);
                }
                if !p.sym(',') {
                    p.punct(')')?;
                    break;
                }
            }
            Action::Create {
                optional,
                options,
                from: None,
            }
        }
    } else if drop {
        let mut names = vec![name.clone()];
        while p.sym(',') {
            names.push(user_types::parse_name(p)?);
        }
        let cascade=p.word("cascade");
        if !cascade {p.word("restrict");}
        Action::Drop { optional, names, cascade }
    } else if comment {
        p.need("is")?;
        Action::Comment(if p.word("null") {
            None
        } else {
            let Some(Token::String(v)) = p.tokens.get(p.at) else {
                return Err(SqlError::Syntax);
            };
            let v = v.clone();
            p.at += 1;
            Some(v)
        })
    } else if p.word("rename") {
        p.need("to")?;
        Action::Rename(p.name()?)
    } else if p.word("owner") {
        p.need("to")?;
        Action::Owner(p.name()?)
    } else if p.word("set") {
        p.need("schema")?;
        Action::Schema(p.name()?)
    } else {
        p.need("refresh")?;
        p.need("version")?;
        Action::Refresh
    };
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(Ddl { name, action })
}
pub(crate) fn builtin(name: &str) -> Option<Collation> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    include_str!("../../../giggles-icu/src/pg_collation.txt")
        .lines()
        .find_map(|r| {
            let v: Vec<_> = r.split('|').collect();
            if v.get(1).copied() != Some(name) {
                return None;
            }
            let field = |i: usize| v.get(i).filter(|s| !s.is_empty()).map(|s| s.to_string());
            Some(Collation {
                name: name.into(),
                schema: "pg_catalog".into(),
                owner: "postgres".into(),
                provider: v[4].into(),
                deterministic: v[5] == "t",
                encoding: v[6].parse().ok()?,
                collate: field(7),
                ctype: field(8),
                locale: field(9),
                rules: field(10),
                version: field(11),
                comment: None,
            })
        })
}
pub(crate) fn resolve<'a>(
    state: &'a Relational,
    name: &str,
    path: &[String],
) -> Option<(u64, &'a Collation)> {
    if let Some((schema, bare)) = name.split_once('.') {
        state
            .ext
            .collations
            .iter()
            .find(|(_, c)| c.schema == schema && c.name == bare)
            .map(|(id, c)| (*id, c))
    } else {
        path.iter().find_map(|s| {
            state
                .ext
                .collations
                .iter()
                .find(|(_, c)| &c.schema == s && c.name == name)
                .map(|(id, c)| (*id, c))
        })
    }
}
fn actual(c: &Collation) -> R<Option<String>> {
    if c.provider == "i" {
        Ok(Some(
            giggles_icu::Collator::open(
                c.locale.as_deref().ok_or(SqlError::Type)?,
                c.rules.as_deref(),
            )
            .map_err(|_| SqlError::InvalidParameterValue)?
            .version(),
        ))
    } else if c.provider == "c" && c.collate.as_deref() == Some("en_US.utf8") {
        Ok(Some("2.41".into()))
    } else if c.provider == "b" {
        Ok(Some("1".into()))
    } else {
        Ok(None)
    }
}
pub(super) fn execute(state: &mut Relational, ddl: &Ddl, prepared: &Prepared) -> R<()> {
    match &ddl.action {
        Action::Create {
            optional,
            options,
            from,
        } => {
            let (schema, name) = ddl
                .name
                .split_once('.')
                .map(|(s, n)| (s.to_owned(), n.to_owned()))
                .map_or_else(
                    || {
                        prepared
                            .creation_schema(state)
                            .map(|s| (s, ddl.name.clone()))
                    },
                    Ok,
                )?;
            if schema != "public" && !state.ext.schemas.contains_key(&schema) {
                return Err(SqlError::InvalidSchema);
            }
            if state
                .ext
                .collations
                .values()
                .any(|c| c.schema == schema && c.name == name)
            {
                let message = format!("collation \"{name}\" already exists");
                if *optional {
                    user_types::sql_notice(format!("{message}, skipping"), "42710");
                    return Ok(());
                }
                return error(SqlError::DuplicateObject, message);
            }
            for key in options.keys() {
                if !matches!(
                    key.as_str(),
                    "provider"
                        | "locale"
                        | "lc_collate"
                        | "lc_ctype"
                        | "deterministic"
                        | "rules"
                        | "version"
                ) {
                    return error(
                        SqlError::Syntax,
                        format!("unrecognized collation parameter: \"{key}\""),
                    );
                }
            }
            let mut c = if let Some(from) = from {
                resolve(state, from, &prepared.search_path)
                    .map(|(_, c)| c.clone())
                    .or_else(|| builtin(from))
                    .ok_or(SqlError::UndefinedObject)?
            } else {
                let provider = options.get("provider").map_or("libc", String::as_str);
                let deterministic = match options.get("deterministic").map(String::as_str) {
                    None | Some("true" | "on" | "yes" | "1") => true,
                    Some("false" | "off" | "no" | "0") => false,
                    Some(_) => {
                        return error(
                            SqlError::Syntax,
                            "deterministic requires a Boolean value".into(),
                        );
                    }
                };
                let locale = options.get("locale").cloned();
                let mut c = Collation {
                    name: name.clone(),
                    schema: schema.clone(),
                    owner: prepared.schema_user.clone(),
                    provider: String::new(),
                    deterministic,
                    encoding: 6,
                    collate: None,
                    ctype: None,
                    locale: None,
                    rules: options.get("rules").cloned(),
                    version: None,
                    comment: None,
                };
                match provider {
                    "icu" => {
                        c.provider = "i".into();
                        c.encoding = -1;
                        let locale = locale.ok_or_else(|| {
                            user_types::view_replace_error(
                                "parameter \"locale\" must be specified".into(),
                            );
                            SqlError::InvalidTableDefinition
                        })?;
                        let tag = giggles_icu::canonical_language_tag(&locale)
                            .map_err(|_| SqlError::InvalidParameterValue)?;
                        if let Some(n) = tag.notice {
                            user_types::sql_notice(n, "00000");
                        }
                        for w in tag.warnings {
                            user_types::sql_notice(w, "22023");
                        }
                        if let Err(e) = giggles_icu::Collator::open(&tag.tag, c.rules.as_deref()) {
                            return error(
                                SqlError::InvalidParameterValue,
                                format!(
                                    "could not open collator for locale \"{}\" with rules \"{}\": {}",
                                    tag.tag,
                                    c.rules.as_deref().unwrap_or(""),
                                    e.0.split(": ").last().unwrap_or(&e.0)
                                ),
                            );
                        }
                        c.locale = Some(tag.tag);
                    }
                    "libc" => {
                        c.provider = "c".into();
                        c.collate = options
                            .get("lc_collate")
                            .cloned()
                            .or_else(|| locale.clone());
                        c.ctype = options.get("lc_ctype").cloned().or(locale);
                        for (key, v) in [("lc_collate", &c.collate), ("lc_ctype", &c.ctype)] {
                            let Some(v) = v else {
                                return error(
                                    SqlError::InvalidTableDefinition,
                                    format!("parameter \"{key}\" must be specified"),
                                );
                            };
                            if !matches!(v.as_str(), "C" | "POSIX" | "C.utf8" | "en_US.utf8") {
                                user_types::sql_error_detail(format!(
                                    "The operating system could not find any locale data for the locale name \"{v}\"."
                                ));
                                return error(
                                    SqlError::InvalidParameterValue,
                                    format!(
                                        "could not create locale \"{v}\": No such file or directory"
                                    ),
                                );
                            }
                        }
                    }
                    "builtin" => {
                        c.provider = "b".into();
                        let locale = locale.ok_or(SqlError::InvalidTableDefinition)?;
                        if !matches!(locale.as_str(), "C" | "C.UTF-8" | "PG_UNICODE_FAST") {
                            return error(
                                SqlError::WrongObjectType,
                                format!("invalid locale name \"{locale}\" for builtin provider"),
                            );
                        }
                        c.locale = Some(locale);
                    }
                    _ => {
                        return error(
                            SqlError::InvalidTableDefinition,
                            format!("unrecognized collation provider: {provider}"),
                        );
                    }
                }
                c.version = options.get("version").cloned().or(actual(&c)?);
                c
            };
            c.name = name;
            c.schema = schema;
            c.owner = prepared.schema_user.clone();
            c.comment = None;
            validate(&c)?;
            if let Some(actual) = actual(&c)?
                && c.version.as_ref().is_some_and(|v| *v != actual)
            {
                user_types::sql_notice_detail(
                    format!("collation \"{}\" has version mismatch", c.name),
                    format!(
                        "The collation in the database was created using version {}, but the operating system provides version {}.",
                        c.version.as_deref().unwrap_or(""),
                        actual
                    ),
                    "01000",
                );
            }
            state.mutate_ext(|ext, next| {
                let id = allocate(next)?;
                ext.collations.insert(id, c);
                Ok(())
            })?;
        }
        Action::Drop { optional, names, cascade } => {
            let mut ids = Vec::new();
            for n in names {
                if let Some((id, _)) = resolve(state, n, &prepared.search_path) {
                    ids.push(id);
                } else if builtin(n).is_some() {
                    return error(
                        SqlError::DependentObjects,
                        format!(
                            "cannot drop collation \"{n}\" because it is required by the database system"
                        ),
                    );
                } else if *optional {
                    user_types::sql_notice(
                        format!("collation \"{n}\" does not exist, skipping"),
                        "42704",
                    );
                } else {
                    return error(
                        SqlError::UndefinedObject,
                        format!("collation \"{n}\" does not exist"),
                    );
                }
            }
            let dependents:Vec<_>=state.tables.iter().flat_map(|(name,t)|t.columns.iter().filter_map(|c| {
                match state.column_collation(c.id) {Some(column_collations::Reference::User(id)) if ids.contains(id)=>Some((name.clone(),c.name.clone(),*id)),_=>None}
            })).collect();
            if !dependents.is_empty() && !cascade {
                let id=dependents[0].2;
                let c=&state.ext.collations[&id];
                let label=if c.schema=="public" {c.name.clone()} else {format!("{}.{}",c.schema,c.name)};
                user_types::view_replace_error(format!("cannot drop collation {label} because other objects depend on it"));
                user_types::sql_error_detail(dependents.iter().map(|(t,c,_)|format!("column {c} of table {} depends on collation {label}",t.replace('\0',"."))).collect::<Vec<_>>().join("\n"));
                return Err(SqlError::DependentObjects);
            }
            for (table,column,_) in dependents {
                alter_table::execute(state,&alter_table::AlterTable {table,actions:vec![alter_table::Action::Drop(column,false,true)],resolution_error:None,column_names:ColumnNamePolicy::Sql},&prepared.search_path,None,&mut ||crate::cancellation::check().is_err())?;
            }
            let index_ids:Vec<_>=state.ext.column_collations.iter().filter_map(|(id,b)|match b {
                column_collations::Binding::Index(rs) if rs.iter().flatten().any(|r|matches!(r,column_collations::Reference::User(id) if ids.contains(id)))=>Some(*id),_=>None
            }).collect();
            if !index_ids.is_empty() && !cascade {return Err(SqlError::DependentObjects);}
            let descriptors=state.secondary_descriptors_engine().iter().filter(|d|!index_ids.contains(&d.id)).cloned().collect::<Vec<_>>();
            state.secondary=state.secondary_build_complete_engine(&descriptors,&mut ||crate::cancellation::check().is_err())?;
            state.mutate_ext(|ext, _| {
                for id in index_ids {ext.column_collations.remove(&id);}
                for id in ids {
                    ext.collations.remove(&id);
                }
                Ok(())
            })?;
        }
        action => {
            let (id, c) = resolve(state, &ddl.name, &prepared.search_path)
                .ok_or(SqlError::UndefinedObject)?;
            let mut c = c.clone();
            match action {
                Action::Rename(n) => c.name = n.clone(),
                Action::Owner(n) => c.owner = n.clone(),
                Action::Schema(s) => {
                    if s != "public" && !state.ext.schemas.contains_key(s) {
                        return Err(SqlError::InvalidSchema);
                    }
                    c.schema = s.clone();
                }
                Action::Comment(v) => c.comment = v.clone().filter(|s| !s.is_empty()),
                Action::Refresh => {
                    let v = actual(&c)?;
                    user_types::sql_notice(
                        if v == c.version {
                            "version has not changed".into()
                        } else {
                            format!(
                                "changing version from {} to {}",
                                c.version.as_deref().unwrap_or(""),
                                v.as_deref().unwrap_or("")
                            )
                        },
                        "00000",
                    );
                    c.version = v;
                }
                _ => unreachable!(),
            }
            if state
                .ext
                .collations
                .iter()
                .any(|(other, x)| *other != id && x.name == c.name && x.schema == c.schema)
            {
                return error(
                    SqlError::DuplicateObject,
                    format!(
                        "collation \"{}\" already exists in schema \"{}\"",
                        c.name, c.schema
                    ),
                );
            }
            validate(&c)?;
            if let Some(actual) = actual(&c)?
                && c.version.as_ref().is_some_and(|v| *v != actual)
            {
                user_types::sql_notice_detail(
                    format!("collation \"{}\" has version mismatch", c.name),
                    format!(
                        "The collation in the database was created using version {}, but the operating system provides version {}.",
                        c.version.as_deref().unwrap_or(""),
                        actual
                    ),
                    "01000",
                );
            }
            state.mutate_ext(|ext, _| {
                ext.collations.insert(id, c);
                Ok(())
            })?;
        }
    }
    state.epoch = state.epoch.checked_add(1).ok_or(SqlError::Limit)?;
    Ok(())
}

impl Prepared {
    pub fn collation_permission_error(&self, schema: Option<&str>) {
        if let Statement::CollationDdl(ddl) = self.statement.get() {
            user_types::view_replace_error(schema.map_or_else(
                || format!("must be owner of collation {}", ddl.name),
                |s| format!("permission denied for schema {s}"),
            ));
        }
    }
    pub(crate) fn collation_drop_tables(&self,state:&Relational)->Option<Vec<String>> {
        let Statement::CollationDdl(Ddl {action:Action::Drop {names,cascade:true,..},..})=self.statement.get() else {return None};
        let ids=names.iter().filter_map(|n|resolve(state,n,&self.search_path).map(|(id,_)|id)).collect::<BTreeSet<_>>();
        Some(state.tables.iter().filter(|(_,t)|t.columns.iter().any(|c|matches!(state.column_collation(c.id),Some(column_collations::Reference::User(id)) if ids.contains(id)))).map(|(n,_)|n.clone()).collect())
    }
    pub fn is_collation_ddl(&self) -> bool {
        matches!(self.statement.get(), Statement::CollationDdl(_))
    }
    pub fn collation_new_owner(&self) -> Option<&str> {
        match self.statement.get() {
            Statement::CollationDdl(Ddl {
                action: Action::Owner(owner),
                ..
            }) => Some(owner),
            _ => None,
        }
    }
    pub(crate) fn collation_authority(
        &self,
        state: &Relational,
    ) -> R<Vec<(String, Option<String>)>> {
        let Statement::CollationDdl(ddl) = self.statement.get() else {
            return Ok(Vec::new());
        };
        if matches!(ddl.action, Action::Create { .. }) {
            let schema = ddl
                .name
                .split_once('.')
                .map(|(s, _)| s.to_owned())
                .map_or_else(|| self.creation_schema(state), Ok)?;
            return Ok(vec![(schema, None)]);
        }
        let names = match &ddl.action {
            Action::Drop { names, .. } => names.clone(),
            _ => vec![ddl.name.clone()],
        };
        let mut targets = Vec::new();
        for name in names {
            if let Some((_, c)) = resolve(state, &name, &self.search_path) {
                targets.push((c.schema.clone(), Some(c.owner.clone())));
            }
        }
        if let Action::Schema(s) = &ddl.action {
            targets.push((s.clone(), None));
        }
        Ok(targets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_and_v2_codecs_preserve_old_bytes_and_reject_bad_records() {
        use crate::storage::engine::catalog_codec::{decode_collation, encode_collation};
        let mut ext = CatalogExt::default();
        let old = bincode::serialize(&ext).unwrap();
        assert_eq!(
            old,
            bincode::serialize(&(
                1u32,
                BTreeMap::<u64, ColumnExt>::new(),
                BTreeMap::<u64, Vec<Constraint>>::new(),
                BTreeMap::<u64, SequenceDef>::new(),
                BTreeMap::<u64, TypeDef>::new()
            ))
            .unwrap()
        );
        let mut c = builtin("C").unwrap();
        c.name = "mine".into();
        c.schema = "public".into();
        ext.collations.insert(1, c.clone());
        ext.ext_version = 9;
        let wire = bincode::serialize(&ext).unwrap();
        assert_eq!(bincode::deserialize::<CatalogExt>(&wire).unwrap(), ext);
        let v2 = encode_collation(&c).unwrap();
        assert_eq!(v2[0], 1);
        assert_eq!(decode_collation(&v2).unwrap(), c);
        let mut bad = v2.clone();
        bad[0] = 2;
        assert!(decode_collation(&bad).is_err());
        c.provider = "z".into();
        assert!(encode_collation(&c).is_err());
        ext.collations.clear();
        ext.ext_version = 1;
        assert_eq!(bincode::serialize(&ext).unwrap(), old);
    }
}
