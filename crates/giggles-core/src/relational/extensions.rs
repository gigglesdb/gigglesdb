//! Built-in extension registry and DDL.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledExtension {
    #[serde(deserialize_with = "super::catalog_ext::name")]
    pub schema: String,
    #[serde(deserialize_with = "super::catalog_ext::name")]
    pub version: String,
    pub oid: u32,
    pub owner: u32,
}
#[derive(Debug)]
pub struct ExtensionFunction {
    pub name: &'static str,
    pub arguments: &'static [DataType],
    pub result: DataType,
    pub volatility: char,
    pub strict: bool,
    pub parallel_safe: bool,
}
const fn function(
    name: &'static str,
    arguments: &'static [DataType],
    volatility: char,
) -> ExtensionFunction {
    ExtensionFunction {
        name,
        arguments,
        result: DataType::Uuid,
        volatility,
        strict: true,
        parallel_safe: true,
    }
}
#[derive(Debug)]
pub struct BuiltinExtension {
    pub name: &'static str,
    pub default_version: &'static str,
    pub versions: &'static [&'static str],
    pub comment: &'static str,
    pub relocatable: bool,
    pub superuser: bool,
    pub trusted: bool,
    pub schema: Option<&'static str>,
    pub requires: &'static [&'static str],
    pub functions: &'static [ExtensionFunction],
}
/// Parse-only routing: installed-extension visibility is resolved by core later.
pub(super) fn routine_name(name: &str) -> bool {
    let name = name.strip_prefix("\0uuid-ossp:").unwrap_or(name);
    let name = name.rsplit('\0').next().unwrap_or(name);
    BUILTINS
        .iter()
        .any(|e| e.functions.iter().any(|f| f.name == name))
}
pub(super) fn routine_call(sql: &str) -> bool {
    lex(sql).is_ok_and(|tokens| tokens.windows(2).any(|pair| {
        matches!(pair, [Token::Word(n) | Token::QuotedIdent(n), Token::Symbol('(')] if routine_name(n))
    }))
}
pub const BUILTINS: &[BuiltinExtension] = &[
    BuiltinExtension {
        name: "plpgsql",
        default_version: "1.0",
        versions: &["1.0"],
        comment: "PL/pgSQL procedural language",
        relocatable: false,
        superuser: true,
        trusted: true,
        schema: Some("pg_catalog"),
        requires: &[],
        functions: &[],
    },
    BuiltinExtension {
        name: "uuid-ossp",
        default_version: "1.1",
        versions: &["1.1"],
        comment: "generate universally unique identifiers (UUIDs)",
        relocatable: true,
        superuser: true,
        trusted: true,
        schema: None,
        requires: &[],
        functions: &[
            function("uuid_nil", &[], 'i'),
            function("uuid_ns_dns", &[], 'i'),
            function("uuid_ns_url", &[], 'i'),
            function("uuid_ns_oid", &[], 'i'),
            function("uuid_ns_x500", &[], 'i'),
            function("uuid_generate_v1", &[], 'v'),
            function("uuid_generate_v1mc", &[], 'v'),
            function("uuid_generate_v4", &[], 'v'),
            function("uuid_generate_v3", &[DataType::Uuid, DataType::Text], 'i'),
            function("uuid_generate_v5", &[DataType::Uuid, DataType::Text], 'i'),
        ],
    },
];
pub(crate) fn validate(ext: &CatalogExt) -> R<()> {
    if ext.extensions.len() > 64 {
        return Err(SqlError::Limit);
    }
    let mut oids = BTreeSet::new();
    for (name, installed) in &ext.extensions {
        let builtin = BUILTINS
            .iter()
            .find(|b| b.name == name)
            .ok_or(SqlError::Type)?;
        let versions = builtin.versions;
        if name.len() > 63
            || name == "plpgsql"
            || !versions.contains(&installed.version.as_str())
            || !(matches!(installed.schema.as_str(), "public" | "pg_catalog")
                || ext.schemas.contains_key(&installed.schema))
            || installed.oid == 0
            || !oids.insert(installed.oid)
        {
            return Err(SqlError::Type);
        }
    }
    Ok(())
}
#[derive(Debug, Clone)]
pub(super) struct Ddl {
    create: bool,
    optional: bool,
    names: Vec<String>,
    schema: Option<String>,
    version: Option<String>,
    cascade: bool,
}
impl Ddl {
    pub(super) fn command(&self) -> &'static str {
        if self.create {
            "CREATE EXTENSION"
        } else {
            "DROP EXTENSION"
        }
    }
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens, [Token::Word(a), Token::Word(b), ..] if matches!(a.as_str(), "create" | "drop") && b == "extension")
}
pub(super) fn parse(p: &mut Parser) -> R<Ddl> {
    let create = p.word("create");
    if !create {
        p.need("drop")?;
    }
    p.need("extension")?;
    let optional = p.word("if");
    if optional {
        if create {
            p.need("not")?;
        }
        p.need("exists")?;
    }
    let mut names = vec![alter_table::raw_name(p)?];
    let (mut schema, mut version, mut cascade) = (None, None, false);
    if create {
        p.word("with");
        loop {
            if p.word("schema") && schema.is_none() {
                schema = Some(alter_table::raw_name(p)?);
            } else if p.word("version") && version.is_none() {
                version = Some(match p.tokens.get(p.at) {
                    Some(Token::String(s) | Token::Word(s) | Token::QuotedIdent(s)) => {
                        p.at += 1;
                        s.clone()
                    }
                    _ => return Err(SqlError::Syntax),
                });
            } else if !cascade && p.word("cascade") {
                cascade = true;
            } else {
                break;
            }
        }
    } else {
        while p.sym(',') {
            names.push(alter_table::raw_name(p)?);
        }
        cascade = p.word("cascade");
        if !cascade {
            p.word("restrict");
        }
    }
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(Ddl {
        create,
        optional,
        names,
        schema,
        version,
        cascade,
    })
}
fn error<T>(code: SqlError, message: String) -> R<T> {
    user_types::view_replace_error(message);
    Err(code)
}
pub(super) fn execute(state: &mut Relational, ddl: &Ddl, prepared: &Prepared) -> R<()> {
    if ddl.create {
        let name = &ddl.names[0];
        if name == "plpgsql" || state.ext.extensions.contains_key(name) {
            let message = format!("extension \"{name}\" already exists");
            if ddl.optional {
                user_types::sql_notice(format!("{message}, skipping"), "42710");
                return Ok(());
            }
            return error(SqlError::DuplicateObject, message);
        }
        let builtin = match BUILTINS.iter().find(|b| b.name == name) {
            Some(b) => b,
            None => {
                user_types::sql_error_hint(
                    "The extension must first be installed on the system where PostgreSQL is running.",
                );
                return error(
                    SqlError::Unsupported,
                    format!("extension \"{name}\" is not available"),
                );
            }
        };
        let versions = builtin.versions;
        let version = ddl.version.as_deref().unwrap_or(builtin.default_version);
        if !versions.contains(&version) {
            return error(
                SqlError::InvalidParameterValue,
                format!(
                    "extension \"{name}\" has no installation script nor update path for version \"{version}\""
                ),
            );
        }
        let schema = match &ddl.schema {
            Some(s) => s.clone(),
            None => prepared
                .search_path
                .iter()
                .find(|s| {
                    matches!(s.as_str(), "public" | "pg_catalog")
                        || state.ext.schemas.contains_key(*s)
                })
                .cloned()
                .ok_or(SqlError::InvalidSchema)?,
        };
        if !matches!(schema.as_str(), "public" | "pg_catalog")
            && !state.ext.schemas.contains_key(&schema)
        {
            return error(
                SqlError::InvalidSchema,
                format!("schema \"{schema}\" does not exist"),
            );
        }
        let oid = user_types::type_oid(allocate(&mut state.next_id)?)?;
        std::sync::Arc::make_mut(&mut state.ext).extensions.insert(
            name.clone(),
            InstalledExtension {
                schema,
                version: version.into(),
                oid,
                owner: prepared
                    .extension_owner
                    .load(std::sync::atomic::Ordering::Relaxed),
            },
        );
    } else {
        for name in &ddl.names {
            if name == "plpgsql" {
                return Err(SqlError::Unsupported);
            }
            if !state.ext.extensions.contains_key(name) && !ddl.optional {
                return error(
                    SqlError::UndefinedObject,
                    format!("extension \"{name}\" does not exist"),
                );
            }
        }
        let defaults = if ddl.names.iter().any(|n| n == "uuid-ossp") {
            extension_defaults(state, &prepared.search_path)?
        } else { Vec::new() };
        if !defaults.is_empty() && !ddl.cascade {
            user_types::sql_error_hint("Use DROP ... CASCADE to drop the dependent objects too.");
            user_types::sql_error_detail(format!(
                "{} depends on function {}()",
                defaults[0].1, defaults[0].2
            ));
            return error(
                SqlError::DependentObjects,
                "cannot drop extension uuid-ossp because other objects depend on it".into(),
            );
        }
        for (id, description, _) in defaults {
            std::sync::Arc::make_mut(&mut state.ext).columns.get_mut(&id).unwrap().default = None;
            user_types::sql_notice(format!("drop cascades to {description}"), "00000");
        }
        for name in &ddl.names {
            if std::sync::Arc::make_mut(&mut state.ext).extensions.remove(name).is_none() {
                user_types::sql_notice(
                    format!("extension \"{name}\" does not exist, skipping"),
                    "00000",
                );
            }
        }
    }
    std::sync::Arc::make_mut(&mut state.ext).ext_version = state.ext.wire_version();
    Ok(())
}

pub(super) fn extension_defaults(state: &Relational, path: &[String]) -> R<Vec<(u64, String, String)>> {
    let mut defaults = Vec::new();
    if state.ext.extensions.contains_key("uuid-ossp") {
            for (id, column) in &state.ext.columns {
                if let Some(sql) = &column.default
                    && let Some(function) =
                        query::extension_call(sql, state, path)?
                {
                    for table in state.tables.values() {
                        if let Some(c) = table.columns.iter().find(|c| c.id == *id) {
                            defaults.push((
                                *id,
                                format!(
                                    "default value for column {} of table {}",
                                    c.name, table.name
                                ),
                                function.clone(),
                            ));
                        }
                    }
                }
            }
    }
    Ok(defaults)
}

impl Prepared {
    pub fn extension_ddl(&self) -> Option<(bool, &[String])> {
        if let Statement::Extension(d) = &self.statement {
            Some((d.create, &d.names))
        } else {
            None
        }
    }
    pub fn set_extension_owner(&self, oid: u32) {
        self.extension_owner
            .store(oid, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Prepared {
    pub(crate) fn check_extension_views<'a>(
        &self,
        state: &Relational,
        views: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> R<()> {
        let cascade = match &self.statement {
            Statement::Extension(d) if !d.create && d.names.iter().any(|n| n == "uuid-ossp") => d.cascade,
            Statement::Schema(d) if !d.create && state.ext.extensions.get("uuid-ossp").is_some_and(|e| e.schema == d.name) => d.cascade,
            _ => return Ok(()),
        };
        if !state.ext.extensions.contains_key("uuid-ossp") { return Ok(()); }
        for (name, sql) in views {
            if let Some(function) = query::extension_view_call(sql, state, &self.search_path)? {
                user_types::sql_error_detail(format!(
                    "view {name} depends on function {function}()"
                ));
                user_types::sql_error_hint(
                    "Use DROP ... CASCADE to drop the dependent objects too.",
                );
                return error(
                    if cascade {
                        SqlError::Unsupported
                    } else {
                        SqlError::DependentObjects
                    },
                    "cannot drop extension uuid-ossp because other objects depend on it".into(),
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn installed() -> InstalledExtension {
        InstalledExtension {
            schema: "public".into(),
            version: "1.1".into(),
            oid: 70000,
            owner: 10,
        }
    }
    #[test]
    fn v11_roundtrip() {
        let mut ext = CatalogExt::default();
        ext.extensions.insert("uuid-ossp".into(), installed());
        ext.ext_version = 11;
        let bytes = bincode::serialize(&ext).unwrap();
        assert_eq!(bincode::deserialize::<CatalogExt>(&bytes).unwrap(), ext);
        validate(&ext).unwrap();
    }
    #[test]
    fn stored_view_dependency_is_restricted() {
        let mut state = Relational::default();
        std::sync::Arc::make_mut(&mut state.ext).extensions.insert("uuid-ossp".into(), installed());
        let p = Prepared::parse("DROP EXTENSION \"uuid-ossp\"", 1, 0).unwrap();
        assert_eq!(
            p.check_extension_views(&state, [("v", "SELECT uuid_nil() AS u")].into_iter()),
            Err(SqlError::DependentObjects)
        );
        assert_eq!(
            user_types::take_sql_error_detail().as_deref(),
            Some("view v depends on function uuid_nil()")
        );
    }
    #[test]
    fn v10_bytes_unchanged_without_extensions() {
        let mut ext = CatalogExt::default();
        ext.column_collations
            .insert(1, super::super::column_collations::Binding::EmptyTable);
        ext.ext_version = 10;
        let before = bincode::serialize(&ext).unwrap();
        ext.extensions.insert("uuid-ossp".into(), installed());
        assert_eq!(ext.wire_version(), 11);
        ext.extensions.clear();
        assert_eq!(ext.wire_version(), 10);
        assert_eq!(bincode::serialize(&ext).unwrap(), before);
    }
    #[test]
    fn unknown_extension_rejected() {
        let mut ext = CatalogExt::default();
        ext.extensions.insert("unknown".into(), installed());
        assert_eq!(validate(&ext), Err(SqlError::Type));
    }
    #[test]
    fn old_catalog_bytes_unchanged() {
        let ext = CatalogExt::default();
        let before = bincode::serialize(&ext).unwrap();
        let mut ext = ext;
        ext.extensions.insert("uuid-ossp".into(), installed());
        ext.extensions.clear();
        assert_eq!(bincode::serialize(&ext).unwrap(), before);
    }
}
