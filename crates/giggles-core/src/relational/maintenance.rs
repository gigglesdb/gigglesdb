//! PostgreSQL maintenance commands: validate catalog targets without physical heap work.
use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Command {
    verb: String,
    kind: String,
    concurrently: bool,
    analyze: bool,
    targets: Vec<(String, Vec<String>)>,
    index: Option<String>,
}
fn error(code: SqlError, message: impl Into<String>) -> SqlError {
    user_types::exact_sql_error(code, message.into(), None)
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens.first(), Some(Token::Word(w)) if matches!(w.as_str(), "vacuum"|"analyze"|"analyse"|"checkpoint"|"reindex"|"cluster"))
}
fn name(p: &mut Parser) -> R<String> {
    let first = p.raw_name()?;
    if p.sym('.') {
        Ok(format!("{first}\0{}", p.raw_name()?))
    } else {
        Ok(first)
    }
}
fn value(p: &mut Parser) -> R<Option<String>> {
    let sign = if matches!(p.tokens.get(p.at), Some(Token::Op(op)) if op == "-" || op == "+") {
        let Token::Op(op) = &p.tokens[p.at] else {
            unreachable!()
        };
        let s = op.clone();
        p.at += 1;
        s
    } else {
        String::new()
    };
    if let Some(Token::Word(v) | Token::String(v) | Token::Number(v)) = p.tokens.get(p.at) {
        let v = format!("{sign}{v}");
        p.at += 1;
        Ok(Some(v))
    } else if sign.is_empty() {
        Ok(None)
    } else {
        Err(SqlError::Syntax)
    }
}
fn boolean(key: &str, value: Option<&str>) -> R<bool> {
    match value {
        None => Ok(true),
        Some(v) if matches!(v.to_ascii_lowercase().as_str(), "true" | "on" | "1") => Ok(true),
        Some(v) if matches!(v.to_ascii_lowercase().as_str(), "false" | "off" | "0") => Ok(false),
        _ => Err(error(
            SqlError::Syntax,
            format!("{key} requires a Boolean value"),
        )),
    }
}
pub(super) fn parse(p: &mut Parser) -> R<Command> {
    let mut verb = p.raw_name()?;
    if verb == "analyse" {
        verb = "analyze".into();
    }
    let mut c = Command {
        analyze: verb == "analyze",
        verb,
        kind: String::new(),
        concurrently: false,
        targets: Vec::new(),
        index: None,
    };
    let mut full = false;
    let mut disable = false;
    let mut parallel = 0;
    let mut only_stats = false;
    let mut process_toast = true;
    let mut buffer = false;
    let mut other_options = false;
    if c.verb != "checkpoint" && p.sym('(') {
        loop {
            let key = p.raw_name()?;
            let allowed = match c.verb.as_str() {
                "vacuum" => matches!(
                    key.as_str(),
                    "full"
                        | "freeze"
                        | "verbose"
                        | "analyze"
                        | "disable_page_skipping"
                        | "skip_locked"
                        | "index_cleanup"
                        | "process_main"
                        | "process_toast"
                        | "truncate"
                        | "parallel"
                        | "skip_database_stats"
                        | "only_database_stats"
                        | "buffer_usage_limit"
                ),
                "analyze" => matches!(
                    key.as_str(),
                    "verbose" | "skip_locked" | "buffer_usage_limit"
                ),
                "reindex" => matches!(key.as_str(), "verbose" | "concurrently" | "tablespace"),
                "cluster" => key == "verbose",
                _ => false,
            };
            if !allowed {
                return Err(error(
                    SqlError::Syntax,
                    format!("unrecognized {} option \"{key}\"", c.tag()),
                ));
            }
            let numeric = matches!(p.tokens.get(p.at), Some(Token::Number(_) | Token::Op(_)));
            let v = value(p)?;
            if !numeric
                && v.as_deref().is_some_and(|v| matches!(v, "0" | "1"))
                && !matches!(
                    key.as_str(),
                    "parallel" | "buffer_usage_limit" | "tablespace"
                )
            {
                return Err(error(
                    SqlError::Syntax,
                    format!("{key} requires a Boolean value"),
                ));
            }
            match key.as_str() {
                "parallel" => {
                    let Some(v) = v else {
                        return Err(error(
                            SqlError::Syntax,
                            "parallel option requires a value between 0 and 1024",
                        ));
                    };
                    parallel = v.parse::<i32>().map_err(|_| {
                        error(SqlError::Syntax, "parallel requires an integer value")
                    })?;
                    if !(0..=1024).contains(&parallel) {
                        return Err(error(
                            SqlError::Syntax,
                            "parallel workers for vacuum must be between 0 and 1024",
                        ));
                    }
                }
                "buffer_usage_limit" => {
                    buffer = true;
                    let v = v.ok_or_else(|| {
                        error(SqlError::Syntax, "buffer_usage_limit requires a parameter")
                    })?;
                    let end = v.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(v.len());
                    let n = v[..end].trim().parse::<f64>().unwrap_or(-1.0);
                    let unit = v[end..].trim();
                    let factor = match unit {
                        "" | "kB" => Some(1.0),
                        "B" => Some(1.0 / 1024.0),
                        "MB" => Some(1024.0),
                        "GB" => Some(1024.0 * 1024.0),
                        "TB" => Some(1024.0 * 1024.0 * 1024.0),
                        _ => None,
                    };
                    let kb = n * factor.unwrap_or(-1.0);
                    if factor.is_none() || (kb != 0.0 && !(128.0..=16777216.0).contains(&kb)) {
                        let e = error(
                            SqlError::InvalidParameterValue,
                            "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB",
                        );
                        if factor.is_none() {
                            user_types::sql_error_hint(
                                "Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".",
                            );
                        }
                        return Err(e);
                    }
                }
                "tablespace" => tablespace(&v.ok_or(SqlError::Syntax)?)?,
                "index_cleanup" if v.as_deref() == Some("auto") => (),
                _ => {
                    let enabled = boolean(&key, v.as_deref())?;
                    other_options |= enabled
                        && matches!(
                            key.as_str(),
                            "full"
                                | "freeze"
                                | "analyze"
                                | "disable_page_skipping"
                                | "skip_locked"
                                | "skip_database_stats"
                        );
                    match key.as_str() {
                        "full" => full = enabled,
                        "process_toast" => process_toast = enabled,
                        "analyze" => c.analyze = enabled,
                        "disable_page_skipping" => disable = enabled,
                        "only_database_stats" => only_stats = enabled,
                        "concurrently" => c.concurrently = enabled,
                        _ => (),
                    }
                }
            }
            if !p.sym(',') {
                break;
            }
        }
        p.punct(')')?;
    } else if c.verb == "vacuum" || c.verb == "analyze" || c.verb == "cluster" {
        loop {
            if p.word("verbose") || (c.verb == "vacuum" && p.word("freeze")) {
                continue;
            }
            if c.verb == "vacuum" && p.word("full") {
                full = true;
                continue;
            }
            if c.verb == "vacuum" && p.word("analyze") {
                c.analyze = true;
                continue;
            }
            break;
        }
    }
    if full && parallel > 0 {
        return Err(error(
            SqlError::Unsupported,
            "VACUUM FULL cannot be performed in parallel",
        ));
    }
    if full && buffer {
        return Err(error(
            SqlError::Unsupported,
            "BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL",
        ));
    }
    if full && disable {
        return Err(error(
            SqlError::Unsupported,
            "VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL",
        ));
    }
    if full && !process_toast {
        return Err(error(
            SqlError::Unsupported,
            "PROCESS_TOAST required with VACUUM FULL",
        ));
    }
    if c.verb == "reindex" {
        c.kind = p.raw_name()?;
        if !matches!(
            c.kind.as_str(),
            "table" | "index" | "schema" | "database" | "system"
        ) {
            return Err(SqlError::Syntax);
        }
        c.concurrently |= p.word("concurrently");
    }
    if c.verb != "checkpoint"
        && p.at < p.tokens.len()
        && !matches!(p.tokens.get(p.at), Some(Token::Symbol(';')))
    {
        loop {
            if c.verb == "vacuum" || c.verb == "analyze" {
                p.word("only");
            }
            let target = name(p)?;
            let mut columns = Vec::new();
            if matches!(c.verb.as_str(), "vacuum" | "analyze") && p.sym('(') {
                loop {
                    columns.push(p.raw_name()?);
                    if !p.sym(',') {
                        break;
                    }
                }
                p.punct(')')?;
            }
            c.targets.push((target, columns));
            if !matches!(c.verb.as_str(), "vacuum" | "analyze") || !p.sym(',') {
                break;
            }
        }
    }
    if c.verb == "reindex"
        && c.targets.is_empty()
        && !matches!(c.kind.as_str(), "database" | "system")
    {
        return Err(SqlError::Syntax);
    }
    if c.verb == "cluster" && p.word("using") {
        c.index = Some(name(p)?);
    }
    if only_stats && !c.targets.is_empty() {
        return Err(error(
            SqlError::Unsupported,
            "ONLY_DATABASE_STATS cannot be specified with a list of tables",
        ));
    }
    if only_stats && other_options {
        return Err(error(
            SqlError::Unsupported,
            "ONLY_DATABASE_STATS cannot be specified with other VACUUM options",
        ));
    }
    if c.verb == "vacuum" && !c.analyze && c.targets.iter().any(|(_, cols)| !cols.is_empty()) {
        return Err(error(
            SqlError::Unsupported,
            "ANALYZE option must be specified when a column list is provided",
        ));
    }
    if c.verb == "checkpoint"
        && p.at < p.tokens.len()
        && !matches!(p.tokens.get(p.at), Some(Token::Symbol(';')))
    {
        let token = match &p.tokens[p.at] {
            Token::Symbol(c) => c.to_string(),
            Token::Word(s)
            | Token::QuotedIdent(s)
            | Token::String(s)
            | Token::Number(s)
            | Token::Op(s) => s.clone(),
            _ => String::new(),
        };
        return Err(error(
            SqlError::Syntax,
            format!("syntax error at or near \"{token}\""),
        ));
    }
    Ok(c)
}
impl Command {
    pub(super) fn tag(&self) -> &'static str {
        match self.verb.as_str() {
            "vacuum" => "VACUUM",
            "analyze" => "ANALYZE",
            "reindex" => "REINDEX",
            "cluster" => "CLUSTER",
            _ => "CHECKPOINT",
        }
    }
    pub(super) fn block_command(&self) -> Option<&'static str> {
        match self.verb.as_str() {
            "vacuum" => Some("VACUUM"),
            "cluster" if self.targets.is_empty() => Some("CLUSTER"),
            "reindex" => {
                if self.concurrently {
                    Some("REINDEX CONCURRENTLY")
                } else {
                    match self.kind.as_str() {
                        "schema" => Some("REINDEX SCHEMA"),
                        "database" => Some("REINDEX DATABASE"),
                        "system" => Some("REINDEX SYSTEM"),
                        _ => None,
                    }
                }
            }
            _ => None,
        }
    }
    pub(super) fn execute(
        &self,
        prepared: &Prepared,
        state: &Relational,
        actor: Option<&crate::roles::CatalogRole>,
        image: &crate::roles::RoleCatalogImage,
        database: &str,
        private_owner: Option<(&str, u64)>,
    ) -> Result<Option<storage_options::ClusterMarker>, crate::CoreError> {
        let superuser = actor.is_none_or(|a| a.superuser);
        let privileges = actor
            .map(|a| image.privileges_of(&a.name))
            .transpose()
            .map_err(admin_utility::role_error)?;
        let maintain = superuser
            || privileges
                .as_ref()
                .is_some_and(|p| p.contains("pg_maintain"));
        if self.verb == "checkpoint" {
            if !superuser
                && !privileges
                    .as_ref()
                    .is_some_and(|p| p.contains("pg_checkpoint"))
            {
                return Err(crate::CoreError::Sql(user_types::exact_sql_error(
                    SqlError::InsufficientPrivilege,
                    "permission denied to execute CHECKPOINT command",
                    Some(
                        "Only roles with privileges of the \"pg_checkpoint\" role may execute this command.",
                    ),
                )));
            }
            return Ok(None);
        }
        if self.kind == "system" && self.concurrently {
            return Err(crate::CoreError::Sql(error(
                SqlError::Unsupported,
                "cannot reindex system catalogs concurrently",
            )));
        }
        if self.kind == "schema" {
            let name = &self.targets[0].0;
            if name != "public" && !state.ext.schemas.contains_key(name) {
                return Err(crate::CoreError::Sql(error(
                    SqlError::InvalidSchema,
                    format!("schema \"{name}\" does not exist"),
                )));
            }
            let owns_schema = state.ext.schemas.get(name).is_some_and(|schema| {
                privileges
                    .as_ref()
                    .is_some_and(|p| p.contains(&schema.owner))
            });
            if !superuser && !owns_schema {
                return Err(crate::CoreError::Sql(admin_utility::must_own(
                    "schema", name,
                )));
            }
            return Ok(None);
        }
        if matches!(self.kind.as_str(), "database" | "system") {
            // Core defaults to postgres; pgwire supplies the connection database identity.
            if self
                .targets
                .first()
                .is_some_and(|(name, _)| name != database)
            {
                return Err(crate::CoreError::Sql(error(
                    SqlError::Unsupported,
                    "can only reindex the currently open database",
                )));
            }
            if !maintain {
                return Err(crate::CoreError::Sql(admin_utility::must_own(
                    "database", database,
                )));
            }
            return Ok(None);
        }
        let all;
        let targets =
            if self.targets.is_empty() && matches!(self.verb.as_str(), "vacuum" | "analyze") {
                all = state
                    .tables
                    .keys()
                    .map(|k| (k.clone(), Vec::new()))
                    .collect::<Vec<_>>();
                &all
            } else {
                &self.targets
            };
        let can_use_schema = |schema: &str| -> Result<bool, crate::CoreError> {
            if superuser || matches!(schema, "public" | "pg_catalog") {
                return Ok(true);
            }
            for role in privileges.iter().flatten() {
                if state
                    .ext
                    .schemas
                    .get(schema)
                    .is_some_and(|s| s.owner == *role)
                    || image
                        .schema_privileges(schema, role)
                        .map_err(admin_utility::role_error)?
                        .contains(&crate::roles::SchemaPrivilege::Usage)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        let mut visible_path = Vec::new();
        for schema in &prepared.search_path {
            if can_use_schema(schema)? {
                visible_path.push(schema.clone());
            }
        }
        for (target, columns) in targets {
            if let Some((schema, _)) = target.split_once('\0')
                && !matches!(schema, "public" | "pg_catalog")
                && !state.ext.schemas.contains_key(schema)
            {
                return Err(crate::CoreError::Sql(error(
                    SqlError::InvalidSchema,
                    format!("schema \"{schema}\" does not exist"),
                )));
            }
            if !self.targets.is_empty()
                && let Some((schema, _)) = target.split_once('\0')
                && state.ext.schemas.contains_key(schema)
                && !can_use_schema(schema)?
            {
                return Err(crate::CoreError::Sql(error(
                    SqlError::InsufficientPrivilege,
                    format!("permission denied for schema {schema}"),
                )));
            }
            let key = if self.targets.is_empty() {
                target.clone()
            } else {
                state.resolve_index_key(target, &visible_path)
            };
            let index_table = state.index_table_key(&key);
            let table_key = if self.kind == "index" {
                index_table.as_deref().unwrap_or(&key)
            } else {
                &key
            };
            let table = state.tables.get(table_key);
            let display = target.replace('\0', ".");
            let local = target.rsplit('\0').next().unwrap_or(target);
            let Some(table) = table else {
                let special = state.view_names.contains_key(&key)
                    || state.sequence_by_name(&key).is_some()
                    || index_table.is_some();
                if special && !maintain && matches!(self.verb.as_str(), "vacuum" | "analyze") {
                    let owner = image
                        .owner_in_image(&key)
                        .map_err(admin_utility::role_error)?;
                    if !owner
                        .as_ref()
                        .is_some_and(|o| privileges.as_ref().is_some_and(|p| p.contains(o)))
                    {
                        user_types::sql_notice(
                            format!(
                                "permission denied to {} \"{local}\", skipping it",
                                self.verb
                            ),
                            "01000",
                        );
                        continue;
                    }
                }
                if special {
                    if matches!(self.verb.as_str(), "vacuum" | "analyze") {
                        user_types::sql_notice(
                            format!(
                                "skipping \"{local}\" --- cannot {} non-tables or special system tables",
                                self.verb
                            ),
                            "01000",
                        );
                        continue;
                    }
                    return Err(crate::CoreError::Sql(error(
                        SqlError::WrongObjectType,
                        format!(
                            "\"{local}\" is not {}",
                            if self.kind == "index" {
                                "an index"
                            } else {
                                "a table or materialized view"
                            }
                        ),
                    )));
                }
                // Built-in catalogs have no physical heap to maintain.
                if (!target.contains('\0') || target.starts_with("pg_catalog\0"))
                    && matches!(
                        local,
                        "pg_class"
                            | "pg_attribute"
                            | "pg_index"
                            | "pg_namespace"
                            | "pg_type"
                            | "pg_proc"
                            | "pg_constraint"
                            | "pg_database"
                            | "pg_authid"
                            | "pg_statistic"
                    )
                    && matches!(self.verb.as_str(), "vacuum" | "analyze")
                {
                    if !maintain {
                        user_types::sql_notice(
                            format!(
                                "permission denied to {} \"{local}\", skipping it",
                                self.verb
                            ),
                            "01000",
                        );
                    }
                    continue;
                }
                return Err(crate::CoreError::Sql(error(
                    SqlError::UnknownTable,
                    format!("relation \"{display}\" does not exist"),
                )));
            };
            if self.kind == "index" && index_table.is_none() {
                return Err(crate::CoreError::Sql(error(
                    SqlError::WrongObjectType,
                    format!("\"{local}\" is not an index"),
                )));
            }
            let owner = image
                .owner_in_image(table_key)
                .map_err(admin_utility::role_error)?;
            let owns = maintain
                || private_owner.is_some_and(|(o, first)| {
                    table.id >= first && privileges.as_ref().is_some_and(|p| p.contains(o))
                })
                || owner
                    .as_ref()
                    .is_some_and(|o| privileges.as_ref().is_some_and(|p| p.contains(o)));
            if !owns {
                if matches!(self.verb.as_str(), "vacuum" | "analyze") {
                    user_types::sql_notice(
                        format!(
                            "permission denied to {} \"{}\", skipping it",
                            self.verb, table.name
                        ),
                        "01000",
                    );
                    continue;
                }
                return Err(crate::CoreError::Sql(error(
                    SqlError::InsufficientPrivilege,
                    format!(
                        "permission denied for {} {local}",
                        if self.kind == "index" {
                            "index"
                        } else {
                            "table"
                        }
                    ),
                )));
            }
            let mut seen = BTreeSet::new();
            for col in columns {
                if !table.logical_columns().any(|c| c.name == *col) {
                    return Err(crate::CoreError::Sql(error(
                        SqlError::UnknownColumn,
                        format!(
                            "column \"{col}\" of relation \"{}\" does not exist",
                            table.name
                        ),
                    )));
                }
                if !seen.insert(col) {
                    return Err(crate::CoreError::Sql(error(
                        SqlError::DuplicateColumn,
                        format!(
                            "column \"{col}\" of relation \"{}\" appears more than once",
                            table.name
                        ),
                    )));
                }
            }
            if self.verb == "cluster" {
                let previous = state.ext.table_storage.get(&table.id).and_then(|storage| storage.clustered_index);
                if let Some(requested) = &self.index {
                    let index = if requested.contains('\0') {
                        state.resolve_index_key(requested, &visible_path)
                    } else {
                        relation_key(state.table_schema(table.id), requested)
                    };
                    if !state.v2_unique_reporting() {
                        if state.index_table_key(&index).as_deref() != Some(table_key) {
                            return Err(crate::CoreError::Sql(error(
                                SqlError::UndefinedObject,
                                format!(
                                    "index \"{}\" for table \"{}\" does not exist",
                                    requested.replace('\0', "."),
                                    table.name
                                ),
                            )));
                        }
                        return Ok(None);
                    }
                    let id = storage_options::cluster_index(state, table, &index).map_err(crate::CoreError::Sql)?;
                    if previous == Some(id) {
                        return Ok(None);
                    }
                    return Ok(Some(storage_options::ClusterMarker { key: table_key.into(), table: table.id, index: id }));
                }
                if previous.is_none() {
                    return Err(crate::CoreError::Sql(error(
                        SqlError::UndefinedObject,
                        format!(r#"there is no previously clustered index for table "{}""#, table.name),
                    )));
                }
            }
        }
        Ok(None)
    }
}

pub(super) fn tablespace(name: &str) -> R<()> {
    if !super::tablespace::exists(name) {
        return Err(admin_utility::missing("tablespace", name));
    }
    if name == "pg_global" {
        return Err(error(
            SqlError::InvalidParameterValue,
            "only shared relations can be placed in pg_global tablespace",
        ));
    }
    Ok(())
}
pub(super) fn access_method(name: &str) -> R<()> {
    match name {
        "heap" | "default" => Ok(()),
        "btree" | "hash" | "gist" | "spgist" | "gin" | "brin" => Err(error(
            SqlError::ObjectNotInPrerequisiteState,
            format!("access method \"{name}\" is not of type TABLE"),
        )),
        _ => Err(admin_utility::missing("access method", name)),
    }
}
