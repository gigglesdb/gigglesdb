//! Narrow role/ACL and DROP TABLE SQL surface. Password-bearing syntax has no Debug.
use super::scram;
use giggles_core::{
    Admin, CoreError,
    relational::SqlError,
    roles::{CatalogRole, NewRole, RoleChanges, RoleError, SchemaPrivilege, TablePrivilege},
};

thread_local! {
    static MEMBERSHIP_ERROR: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}
pub(crate) fn take_membership_error() -> Option<String> {
    MEMBERSHIP_ERROR.with(|error| error.borrow_mut().take())
}

#[derive(Clone)]
pub(crate) enum AdminStatement {
    Error(&'static str),
    /// The third field is TASKONLY (`Some(true)`) or NOTASKONLY (`Some(false)`).
    Create(String, RoleChanges, Option<bool>, CreateMemberships),
    Alter(String, RoleChanges, Option<bool>),
    Membership { grant: bool, roles: Vec<String>, members: Vec<String>, options: giggles_core::roles::MembershipOptions, revoke: Option<giggles_core::roles::MembershipOption>, grantor: Option<String>, cascade: bool },
    Drop(bool, Vec<String>),
    /// Existence and authorization are validated for all names before dropping.
    /// Multi-name drops execute as one core statement after authorization.
    DropTable(bool, Vec<String>),
    Grant {
        grant: bool,
        schema: bool,
        objects: Vec<String>,
        roles: Vec<String>,
        privileges: Vec<String>,
    },
}
#[derive(PartialEq)]
enum Token {
    Word(String),
    Ident(String),
    Text(String),
    Comma,
    Dot,
    LParen,
    RParen,
    Semi,
    Star,
    Other,
}
fn lex(sql: &str) -> Result<Vec<Token>, &'static str> {
    let mut chars = sql.chars().peekable();
    let mut out = Vec::new();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        out.push(match c {
            '*' => Token::Star,
            ',' => Token::Comma,
            '.' => Token::Dot,
            '(' => Token::LParen,
            ')' => Token::RParen,
            ';' => Token::Semi,
            '\'' | '"' => {
                let mut text = String::new();
                loop {
                    match chars.next() {
                        Some(x) if x == c => {
                            if chars.peek() == Some(&c) {
                                chars.next();
                                text.push(c);
                            } else {
                                break;
                            }
                        }
                        Some(x) => text.push(x),
                        None => return Err("42601"),
                    }
                }
                if c == '\'' {
                    Token::Text(text)
                } else {
                    Token::Ident(text)
                }
            }
            x if x.is_alphanumeric() || x == '_' => {
                let mut word = String::from(x);
                while chars
                    .peek()
                    .is_some_and(|x| x.is_alphanumeric() || *x == '_' || *x == '$')
                {
                    word.push(chars.next().unwrap());
                }
                Token::Word(word.to_lowercase())
            }
            _ => Token::Other,
        });
    }
    if out.last() == Some(&Token::Semi) {
        out.pop();
    }
    Ok(out)
}
type CreateMemberships = Vec<(String, Vec<String>)>;
type ParsedRoleOptions = (RoleChanges, Option<bool>, CreateMemberships);
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}
impl Parser {
    fn kw(&mut self, s: &str) -> bool {
        if self.tokens.get(self.pos) == Some(&Token::Word(s.into())) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn need(&mut self, s: &str) -> Result<(), &'static str> {
        if self.kw(s) { Ok(()) } else { Err("42601") }
    }
    fn ident(&mut self) -> Result<String, &'static str> {
        let name = match self.tokens.get(self.pos) {
            Some(Token::Word(s)) if !s.starts_with(|c: char| c.is_alphabetic() || c == '_') => {
                return Err("fallback");
            }
            Some(Token::Word(s) | Token::Ident(s)) => s.clone(),
            _ => return Err("42601"),
        };
        self.pos += 1;
        if name.is_empty() || name.len() > 63 || name.contains('\0') {
            return Err("42601");
        }
        Ok(name)
    }
    fn role_spec(&mut self) -> Result<String, &'static str> {
        if let Some(Token::Word(s)) = self.tokens.get(self.pos)
            && matches!(s.as_str(), "current_user" | "session_user" | "current_role") {
            let result = format!("\0{s}"); self.pos += 1; return Ok(result);
        }
        self.ident()
    }
    fn role_specs(&mut self) -> Result<Vec<String>, &'static str> {
        let mut names = vec![self.role_spec()?];
        while self.comma() { names.push(self.role_spec()?); }
        Ok(names)
    }
    fn comma(&mut self) -> bool {
        if self.tokens.get(self.pos) == Some(&Token::Comma) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn names(&mut self, tables: bool) -> Result<Vec<String>, &'static str> {
        let mut names = Vec::new();
        loop {
            let mut name = self.ident()?;
            if tables && self.tokens.get(self.pos) == Some(&Token::Dot) {
                self.pos += 1;
                name = format!("{name}\0{}", self.ident()?);
            }
            names.push(name);
            if !self.comma() {
                break;
            }
        }
        Ok(names)
    }
    fn options(&mut self, user: bool) -> Result<ParsedRoleOptions, &'static str> {
        self.kw("with");
        let mut c = RoleChanges::default();
        let mut task_only = None;
        let mut memberships = Vec::new();
        while self.pos < self.tokens.len() {
            let option = match self.tokens.get(self.pos) {
                Some(Token::Word(word)) => {
                    self.pos += 1;
                    word.clone()
                }
                Some(Token::Ident(_)) => return Err("fallback"),
                _ => return Err("42601"),
            };
            let target = match option.as_str() {
                "superuser" | "nosuperuser" => Some((&mut c.superuser, option == "superuser")),
                "login" | "nologin" => Some((&mut c.login, option == "login")),
                "createrole" | "nocreaterole" => Some((&mut c.createrole, option == "createrole")),
                "inherit" | "noinherit" => Some((&mut c.inherit, option == "inherit")),
                "taskonly" | "notaskonly" => Some((&mut task_only, option == "taskonly")),
                "in" | "role" | "user" | "admin" => {
                    let kind = if option == "in" {
                        if !self.kw("role") { self.need("group")?; } "in"
                    } else { option.as_str() };
                    memberships.push((kind.to_owned(), self.names(false)?));
                    None
                }
                "password" => {
                    if c.password.is_some() {
                        return Err("42601");
                    }
                    c.password = Some(if self.kw("null") {
                        None
                    } else {
                        match self.tokens.get(self.pos) {
                            Some(Token::Text(s)) => {
                                self.pos += 1;
                                Some(s.clone())
                            }
                            _ => return Err("42601"),
                        }
                    });
                    None
                }
                "createdb" | "nocreatedb" | "replication"
                | "noreplication" | "bypassrls" | "nobypassrls" | "connection" | "valid"
                | "sysid" | "encrypted" | "unencrypted" | "createuser"
                | "nocreateuser" => return Err("0A000"),
                _ => return Err("fallback"),
            };
            if let Some((slot, value)) = target
                && slot.replace(value).is_some()
            {
                return Err("42601");
            }
        }
        if user && c.login.is_none() {
            c.login = Some(true);
        }
        Ok((c, task_only, memberships))
    }
    fn statement(&mut self) -> Result<AdminStatement, &'static str> {
        if self.kw("create") || self.kw("alter") {
            let create = self.tokens[0] == Token::Word("create".into());
            let user = if self.kw("role") {
                false
            } else {
                self.need("user")?;
                true
            };
            let name = self.ident()?;
            let (changes, task_only, memberships) = self.options(user)?;
            if !create && !memberships.is_empty() { return Err("42601"); }
            return Ok(if create {
                AdminStatement::Create(name, changes, task_only, memberships)
            } else {
                AdminStatement::Alter(name, changes, task_only)
            });
        }
        if self.kw("drop") {
            let table = self.kw("table");
            if !table && !self.kw("role") {
                self.need("user")?;
            }
            let exists = if self.kw("if") {
                self.need("exists")?;
                true
            } else {
                false
            };
            let names = self
                .names(table)
                .map_err(|e| if table && e == "fallback" { "42601" } else { e })?;
            if table && (self.kw("cascade") || self.kw("restrict")) && self.pos == self.tokens.len() {
                return Err("fallback"); // Core SQL owns dependency actions and transactional DDL.
            }
            if self.pos != self.tokens.len() {
                return Err(if !table {
                    "0A000"
                } else {
                    "42601"
                });
            }
            return Ok(if table {
                AdminStatement::DropTable(exists, names)
            } else {
                AdminStatement::Drop(exists, names)
            });
        }
        let grant = self.kw("grant");
        if !grant {
            self.need("revoke")?;
        }
        // Membership grants have no ON clause; quoted names stay identifiers.
        if !self.tokens[self.pos..].contains(&Token::Word("on".into())) {
            use giggles_core::roles::{MembershipOptions, MembershipOption};
            let mut revoke = None;
            if !grant {
                let saved = self.pos;
                let option = if self.kw("admin") { Some(MembershipOption::Admin) }
                    else if self.kw("inherit") { Some(MembershipOption::Inherit) }
                    else if self.kw("set") { Some(MembershipOption::Set) } else { None };
                if option.is_some() && self.kw("option") {
                    self.need("for")?; revoke = option;
                } else { self.pos = saved; }
            }
            let roles = self.names(false)?;
            self.need(if grant { "to" } else { "from" })?;
            let members = self.role_specs()?;
            let mut options = MembershipOptions::default();
            if grant && self.kw("with") {
                loop {
                    let slot = if self.kw("admin") { &mut options.admin }
                        else if self.kw("inherit") { &mut options.inherit }
                        else if self.kw("set") { &mut options.set } else { return Err("42601"); };
                    let value = if self.kw("option") || self.kw("true") { true }
                        else if self.kw("false") { false } else { return Err("42601"); };
                    if slot.replace(value).is_some() { return Err("42601"); }
                    if !self.comma() { break; }
                }
            }
            let grantor = if self.kw("granted") { self.need("by")?; Some(self.role_spec()?) } else { None };
            let cascade = !grant && self.kw("cascade");
            if !grant && !cascade { self.kw("restrict"); }
            if self.pos != self.tokens.len() { return Err("42601"); }
            return Ok(AdminStatement::Membership { grant, roles, members, options, revoke, grantor, cascade });
        }
        let mut privileges = Vec::new();
        loop {
            let p = match self.tokens.get(self.pos) {
                Some(Token::Word(word)) => {
                    self.pos += 1;
                    word.clone()
                }
                _ => return Err("fallback"),
            };
            if p == "all" {
                self.kw("privileges");
            }
            privileges.push(p);
            if !self.comma() {
                break;
            }
        }
        if !self.kw("on") {
            return Err("0A000");
        }
        let schema = self.kw("schema");
        if !schema {
            self.kw("table");
        }
        let objects = self.names(!schema)?;
        self.need(if grant { "to" } else { "from" })?;
        let mut roles = Vec::new();
        loop {
            roles.push(if self.kw("public") { String::new() } else { self.ident()? });
            if !self.comma() { break; }
        }
        if self.pos != self.tokens.len() { return Err("0A000"); }
        if privileges.iter().any(|p| {
            if schema {
                !matches!(p.as_str(), "usage" | "create" | "all")
            } else {
                !matches!(
                    p.as_str(),
                    "select" | "insert" | "update" | "delete" | "truncate" | "trigger" | "all"
                )
            }
        }) {
            return Err("0A000");
        }
        Ok(AdminStatement::Grant {
            grant,
            schema,
            objects,
            roles,
            privileges,
        })
    }
}
pub(crate) fn parse(sql: &str) -> Option<AdminStatement> {
    // Classify before lexing so ordinary core SQL remains entirely untouched.
    let mut rest = sql;
    let mut head = Vec::new();
    for _ in 0..2 {
        rest = rest.trim_start();
        let end = rest
            .find(|c: char| !c.is_alphanumeric() && c != '_' && c != '$')
            .unwrap_or(rest.len());
        if end == 0 {
            break;
        }
        head.push(rest[..end].to_ascii_lowercase());
        rest = &rest[end..];
    }
    let recognized = matches!(head.first().map(String::as_str), Some("grant" | "revoke"))
        || (matches!(
            head.first().map(String::as_str),
            Some("create" | "alter" | "drop")
        ) && matches!(head.get(1).map(String::as_str), Some("role" | "user")))
        || (head.first().map(String::as_str) == Some("drop")
            && head.get(1).map(String::as_str) == Some("table"));
    if !recognized {
        return None;
    }
    let result = match lex(sql) {
        Ok(tokens) => {
            let mut p = Parser { tokens, pos: 0 };
            p.statement().unwrap_or_else(AdminStatement::Error)
        }
        Err(e) => AdminStatement::Error(e),
    };
    if matches!(result, AdminStatement::Error("fallback")) {
        None
    } else {
        Some(result)
    }
}
// Match the bootstrap identity already used to expose pg_auth_members grantors.
pub(crate) fn bootstrap_grantor(catalog: &giggles_core::roles::RoleCatalog) -> Result<Option<String>, RoleError> {
    Ok(catalog.list()?.into_iter().filter(|r| r.superuser).min_by_key(|r| r.oid).map(|r| r.name))
}

fn state(e: RoleError) -> &'static str {
    match e {
        RoleError::ReservedName | RoleError::ReservedAlter => "42939",
        RoleError::ReservedDrop | RoleError::DependentMembership { .. } => "2BP01",
        RoleError::MembershipCycle { .. } => "0LP01",
        RoleError::DatabaseOwnerMembers | RoleError::DatabaseOwnerMember => "0A000",
        RoleError::Duplicate => "42710",
        RoleError::Missing => "42704",
        RoleError::Dependent => "2BP01",
        RoleError::Invalid => "22023",
        RoleError::Limit => "54000",
        RoleError::Core(CoreError::Unsupported) => "0A000",
        RoleError::Core(_) => "XX000",
    }
}
/// A task-only role conflicts with the requested role state (GD-0061 D6, section 7).
const TASK_ONLY_CONFLICT: &str = "42P17";
/// Apply TASKONLY / NOTASKONLY. Setting it on a superuser, an owner or a member
/// fails the whole-image D6 check in the role catalog (`RoleError::Invalid`),
/// which is a role-state conflict here.
fn apply_task_only(
    catalog: &giggles_core::roles::RoleCatalog,
    name: &str,
    value: bool,
) -> Result<(), &'static str> {
    match catalog.set_task_only(name, value) {
        Ok(_) => Ok(()),
        Err(RoleError::Invalid) if value => Err(TASK_ONLY_CONFLICT),
        Err(e) => Err(state(e)),
    }
}
fn password(c: &RoleChanges) -> RoleChanges {
    let mut result = c.clone();
    if let Some(Some(p)) = &mut result.password
        && !scram::is_verifier(p)
    {
        *p = scram::Verifier::generate(p);
    }
    result
}
pub(crate) fn execute(
    stmt: &AdminStatement,
    admin: &Admin,
    actor: &CatalogRole,
    notices: &mut Vec<(bool, String)>,
) -> Result<&'static str, &'static str> {
    let catalog = admin.roles();
    match stmt {
        AdminStatement::Error(e) => Err(*e),
        AdminStatement::Create(name, c, task_only, memberships) => {
            if !actor.superuser
                && (!actor.createrole || c.superuser == Some(true))
            {
                return Err("42501");
            }
            if *task_only == Some(true) && (c.superuser == Some(true) || c.createrole == Some(true))
            {
                return Err("22023");
            }
            for (kind, names) in memberships {
                for role in names {
                    catalog.get(role).map_err(state)?.ok_or("42704")?;
                    if kind == "in" && !actor.superuser && !catalog.admin_of(&actor.name, role).map_err(state)? { return Err("42501"); }
                }
            }
            let c = password(c);
            catalog
                .create(NewRole {
                    inherit: c.inherit,
                    name: name.clone(),
                    superuser: c.superuser.unwrap_or(false),
                    login: c.login.unwrap_or(false),
                    createrole: c.createrole.unwrap_or(false),
                    password: c.password.flatten(),
                })
                .map_err(state)?;
            if *task_only == Some(true)
                && let Err(e) = apply_task_only(&catalog, name, true)
            {
                // Never leave a half-created role behind.
                let _ = catalog.drop(name);
                return Err(e);
            }
            if admin.engine() == giggles_core::Engine::V2 && !actor.superuser && *task_only != Some(true) {
                let bootstrap = giggles_core::roles::BOOTSTRAP_GRANTOR;
                catalog.grant_membership_options(name, &actor.name, bootstrap,
                    giggles_core::roles::MembershipOptions { admin: Some(true), inherit: Some(false), set: Some(false) }).map_err(state)?;
            }
            let bootstrap = giggles_core::roles::BOOTSTRAP_GRANTOR;
            for (kind, names) in memberships {
                for role in names {
                    let (group, member) = if kind == "in" { (role.as_str(), name.as_str()) } else { (name.as_str(), role.as_str()) };
                    if let Err(error) = catalog.grant_membership_options(group, member, if actor.superuser { bootstrap } else { &actor.name },
                        giggles_core::roles::MembershipOptions { admin: (kind == "admin").then_some(true), ..Default::default() }) {
                        let _ = catalog.drop(name); return Err(state(error));
                    }
                }
            }
            Ok("CREATE ROLE")
        }
        AdminStatement::Alter(name, c, task_only) => {
            let target = catalog.get(name).map_err(state)?.ok_or("42704")?;
            let own_password = name == &actor.name
                && c.password.is_some()
                && c.superuser.is_none()
                && c.createrole.is_none()
                && c.login.is_none()
                && c.inherit.is_none()
                && task_only.is_none();
            if !actor.superuser
                && !own_password
                && !(actor.createrole
                    && !target.superuser
                    && c.superuser.is_none()
                    && c.createrole.is_none()
                    && name != &actor.name)
            {
                return Err("42501");
            }
            if *task_only == Some(true) && (c.superuser == Some(true) || c.createrole == Some(true))
            {
                return Err("22023");
            }
            // Section 7: the role would end up task-only and SUPERUSER or CREATEROLE.
            let ends_task_only = task_only.unwrap_or(target.task_only);
            if ends_task_only
                && (c.superuser.unwrap_or(target.superuser)
                    || c.createrole.unwrap_or(target.createrole))
            {
                return Err(TASK_ONLY_CONFLICT);
            }
            // An administrator never turns its own session's role task-only.
            if *task_only == Some(true) && name == &actor.name {
                return Err(TASK_ONLY_CONFLICT);
            }
            // NOTASKONLY is cleared before the other changes (so NOTASKONLY
            // CREATEROLE passes D6); TASKONLY is set after them. A failure in
            // the second step restores the first, best effort.
            if *task_only == Some(false) {
                apply_task_only(&catalog, name, false)?;
            }
            if let Err(e) = catalog.alter(name, password(c)) {
                if *task_only == Some(false) && target.task_only {
                    let _ = catalog.set_task_only(name, true);
                }
                return Err(state(e));
            }
            if *task_only == Some(true)
                && let Err(e) = apply_task_only(&catalog, name, true)
            {
                let _ = catalog.alter(
                    name,
                    RoleChanges {
                        inherit: None,
                        superuser: Some(target.superuser),
                        login: Some(target.login),
                        createrole: Some(target.createrole),
                        password: Some(target.password.clone()),
                    },
                );
                return Err(e);
            }
            Ok("ALTER ROLE")
        }
        AdminStatement::Drop(exists, names) => {
            if !actor.superuser && !actor.createrole {
                return Err("42501");
            }
            let tables = admin.catalog().map_err(|_| "XX000")?.1;
            let schemas = admin.catalog_schema_metadata().map_err(|_| "XX000")?;
            let mut targets = std::collections::BTreeSet::new();
            for name in names {
                let Some(target) = catalog.get(name).map_err(state)? else {
                    if *exists {
                        continue;
                    }
                    return Err("42704");
                };
                if name == &actor.name {
                    return Err("55006");
                }
                if !actor.superuser && target.superuser {
                    return Err("42501");
                }
                for t in &tables {
                    if catalog
                        .table_owner(&giggles_core::relational::relation_key(
                            schemas.tables.get(&t.id).map_or("public", String::as_str),
                            &t.name,
                        ))
                        .map_err(state)?
                        .as_deref()
                        == Some(name)
                    {
                        return Err("2BP01");
                    }
                }
                targets.insert(name);
            }
            for name in targets {
                catalog.drop(name).map_err(state)?;
            }
            Ok("DROP ROLE")
        }
        AdminStatement::DropTable(if_exists, names) => {
            let tables = admin.catalog().map_err(|e| crate::service::sqlstate(&e))?.1;
            let schemas = admin.catalog_schema_metadata().map_err(|_| "XX000")?;
            for name in names {
                let (schema, _) = name.split_once('\0').unwrap_or(("public", name.as_str()));
                if !schema.is_empty() && schema != "public" && !crate::service::is_builtin_catalog_schema(schema) && !schemas.schemas.contains_key(schema) {
                    if *if_exists {
                        notices.push((false, format!("schema \"{schema}\" does not exist, skipping")));
                        continue;
                    }
                    return Err("3F000");
                }
                if !actor.superuser && !schema.is_empty() {
                    let inherited = catalog.privileges_of(&actor.name).map_err(state)?;
                    let capabilities = catalog.builtin_capabilities_of(&actor.name).map_err(state)?;
                    let usage = schema == "public" || inherited.iter().any(|role|
                        schemas.schemas.get(schema).is_some_and(|(_, owner)| owner == role)
                        || (capabilities.contains(role) && matches!(role.as_str(), "pg_read_all_data" | "pg_write_all_data"))
                        || catalog.schema_privileges(schema, role).is_ok_and(|p| p.contains(&SchemaPrivilege::Usage)));
                    if !usage { return Err("42501"); }
                }
                if !tables.iter().any(|table| {
                    &giggles_core::relational::relation_key(
                        schemas
                            .tables
                            .get(&table.id)
                            .map_or("public", String::as_str),
                        &table.name,
                    ) == name
                }) {
                    if *if_exists {
                        let (schema, object) = name.split_once('\0').unwrap_or(("public", name.as_str()));
                        let (kind, missing) = if !schema.is_empty() && schema != "public" && !crate::service::is_builtin_catalog_schema(schema) && !schemas.schemas.contains_key(schema) { ("schema", schema) } else { ("table", object.trim_start_matches('\0')) };
                        notices.push((false, format!("{kind} \"{missing}\" does not exist, skipping")));
                        continue;
                    }
                    return Err("42P01");
                }
                if !actor.superuser
                    && catalog.table_owner(name).map_err(|_| "XX000")?.is_none_or(|owner| !catalog.privileges_of(&actor.name).is_ok_and(|roles| roles.contains(&owner)))
                {
                    return Err("42501");
                }
            }
            let sequence_names = admin
                .sql_session()
                .sequence_owners()
                .map_err(|e| crate::service::sqlstate(&e))?;
            if names.len() > 1 {
                // Preserve the authorization/notice order above, but publish the
                // complete set once. Per-table autocommits can lose earlier tables
                // when a later target has dependencies, and reject duplicates.
                let mut targets = Vec::new();
                for name in names {
                    if tables.iter().any(|table| {
                        giggles_core::relational::relation_key(
                            schemas.tables.get(&table.id).map_or("public", String::as_str),
                            &table.name,
                        ) == *name
                    }) && !targets.contains(name) {
                        targets.push(name.clone());
                    }
                }
                if !targets.is_empty() {
                    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
                    let target = targets.iter().map(|name| {
                        let (schema, table) = name.split_once('\0').unwrap_or(("public", name.as_str()));
                        format!("{}.{}", quote(schema), quote(table))
                    }).collect::<Vec<_>>().join(", ");
                    admin.sql_session().execute_unreleased(
                        &format!("DROP TABLE {}{target}", if *if_exists { "IF EXISTS " } else { "" }),
                        &[], None,
                    ).and_then(|result| result.untasked()).map_err(|e| match e {
                        CoreError::Sql(SqlError::Conflict) => "55P03",
                        _ => crate::service::sqlstate(&e),
                    })?;
                    // No role-catalog cleanup until the whole core drop commits.
                    for name in &targets {
                        catalog.forget_table(name).map_err(|_| "XX000")?;
                    }
                    let live_sequences = admin.sql_session().sequence_owners()
                        .map_err(|e| crate::service::sqlstate(&e))?;
                    for (sequence, _) in &sequence_names {
                        if !live_sequences.iter().any(|(name, _)| name == sequence) {
                            catalog.forget_table(sequence).map_err(|_| "XX000")?;
                        }
                    }
                }
                return Ok("DROP TABLE");
            }
            for name in names {
                let dropped = admin.drop_table(name, *if_exists).map_err(|e| match e {
                    CoreError::Sql(SqlError::Conflict) => "55P03",
                    CoreError::Sql(SqlError::UnknownTable) => "42P01",
                    _ => crate::service::sqlstate(&e),
                })?;
                if dropped {
                    catalog.forget_table(name).map_err(|_| "XX000")?;
                    let live_sequences = admin
                        .sql_session()
                        .sequence_owners()
                        .map_err(|e| crate::service::sqlstate(&e))?;
                    for (sequence, _) in &sequence_names {
                        if !live_sequences.iter().any(|(name, _)| name == sequence) {
                            catalog.forget_table(sequence).map_err(|_| "XX000")?;
                        }
                    }
                }
            }
            Ok("DROP TABLE")
        }
        AdminStatement::Membership { grant, roles, members, options, revoke, grantor, cascade } => {
            MEMBERSHIP_ERROR.with(|error| *error.borrow_mut() = None);
            let requested = grantor.as_deref().unwrap_or(&actor.name);
            let grantor_role = catalog.get(requested).map_err(state)?.ok_or_else(|| {
                MEMBERSHIP_ERROR.with(|error| *error.borrow_mut() = Some(requested.to_owned()));
                "42704"
            })?;
            if requested != actor.name && !actor.superuser && !catalog.can_set(&actor.name, requested).map_err(state)? { return Err("42501"); }
            let explicit = grantor.is_some();
            let bootstrap = grantor_role.superuser && bootstrap_grantor(&catalog).map_err(state)?.as_deref() == Some(requested);
            let grantor = if grantor_role.superuser && (!explicit || bootstrap) { giggles_core::roles::BOOTSTRAP_GRANTOR } else { requested };
            let display_grantor = if explicit { requested } else if grantor == giggles_core::roles::BOOTSTRAP_GRANTOR { "bootstrap" } else { grantor };
            let mut denied = false;
            let mut unchanged = Vec::new();
            let mut missing = None;
            let result = catalog.membership_batch_reporting(roles, members, grantor, *grant, *options, *revoke, *cascade, explicit, |role, edges| {
                let admin = |name| giggles_core::roles::RoleCatalog::admin_in_edges(edges, name, role);
                if (actor.superuser || admin(&actor.name))
                    && ((grantor_role.superuser && (!explicit || !*grant || bootstrap)) || admin(requested)) {
                    Ok(())
                } else {
                    denied = true;
                    MEMBERSHIP_ERROR.with(|error| *error.borrow_mut() = Some(role.to_owned()));
                    Err(RoleError::Invalid)
                }
            }, &mut unchanged, &mut missing).map_err(|error| {
                if let Some(name) = missing {
                    MEMBERSHIP_ERROR.with(|error| *error.borrow_mut() = Some(name));
                }
                if denied { "42501" } else {
                    if matches!(error, RoleError::MembershipCycle { .. }) {
                        MEMBERSHIP_ERROR.with(|message| *message.borrow_mut() = Some(error.to_string()));
                    }
                    state(error)
                }
            });
            for ((role, member), unchanged) in roles.iter().flat_map(|role| members.iter().map(move |member| (role, member))).zip(unchanged) {
                if unchanged {
                    let message = if *grant { "has already been granted" } else { "has not been granted" };
                    notices.push((!*grant, format!("role \"{member}\" {message} membership in role \"{role}\" by role \"{display_grantor}\"")));
                }
            }
            result?;
            Ok(if *grant { "GRANT ROLE" } else { "REVOKE ROLE" })
        }
        AdminStatement::Grant {
            grant,
            schema,
            objects,
            roles,
            privileges,
        } => {
            let tables = admin.catalog().map_err(|_| "XX000")?.1;
            let schemas = admin.catalog_schema_metadata().map_err(|_| "XX000")?;
            if *schema {
                let privileges = catalog.privileges_of(&actor.name).map_err(state)?;
                for schema in objects {
                    // The default public schema is implicit until schema DDL stores metadata.
                    if schema == "public" && !schemas.schemas.contains_key(schema) {
                        if !actor.superuser { return Err("42501"); }
                        continue;
                    }
                    let (_, owner) = schemas.schemas.get(schema).ok_or("3F000")?;
                    if !actor.superuser && !privileges.contains(owner) { return Err("42501"); }
                }
            }
            for object in objects {
                if !schema {
                    if !tables.iter().any(|t| {
                        &giggles_core::relational::relation_key(
                            schemas.tables.get(&t.id).map_or("public", String::as_str),
                            &t.name,
                        ) == object
                    }) {
                        return Err("42P01");
                    }
                    if !actor.superuser
                        && catalog.table_owner(object).map_err(state)?.is_none_or(|owner| !catalog.privileges_of(&actor.name).is_ok_and(|roles| roles.contains(&owner)))
                    {
                        return Err("42501");
                    }
                }
            }
            for role in roles {
                if !role.is_empty() { catalog.get(role).map_err(state)?.ok_or("42704")?; }
            }
            let all = privileges.iter().any(|p| p == "all");
            let tp: Vec<_> = [
                ("select", TablePrivilege::Select),
                ("insert", TablePrivilege::Insert),
                ("update", TablePrivilege::Update),
                ("delete", TablePrivilege::Delete),
                ("truncate", TablePrivilege::Delete),
                ("trigger", TablePrivilege::Trigger),
            ]
            .into_iter()
            // Legacy cannot write TRIGGER grants. REVOKE ALL can remove every privilege.
            .filter(|(s, _)| {
                (all && (*s != "trigger" || !*grant || admin.engine() == giggles_core::Engine::V2))
                    || privileges.iter().any(|p| p == s)
            })
            .map(|(_, p)| p)
            .collect();
            let sp: Vec<_> = [
                ("usage", SchemaPrivilege::Usage),
                ("create", SchemaPrivilege::Create),
            ]
            .into_iter()
            .filter(|(s, _)| all || privileges.iter().any(|p| p == s))
            .map(|(_, p)| p)
            .collect();
            for object in objects {
                for role in roles {
                    if role.is_empty() {
                        if *schema { catalog.change_public_schema(object, &sp, *grant) }
                        else { catalog.change_public_table(object, &tp, *grant) }
                    } else if *schema {
                        if *grant {
                            catalog.grant_schema(object, role, &sp)
                        } else {
                            catalog.revoke_schema(object, role, &sp)
                        }
                    } else if *grant {
                        catalog.grant_table(object, role, &tp)
                    } else {
                        catalog.revoke_table(object, role, &tp)
                    }
                    .map_err(state)?;
                }
            }
            Ok(if *grant { "GRANT" } else { "REVOKE" })
        }
    }
}

fn quote_relation(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Clone)]
pub(crate) struct TableLockRequest {
    pub(crate) tables: Vec<String>,
    pub(crate) mode: giggles_core::SqlTableLockMode,
    pub(crate) nowait: bool,
}

// Parse the bounded pg_dump lock surface without granting admin authority.
pub(crate) fn access_share_tables(sql: &str) -> Result<Option<TableLockRequest>, &'static str> {
    let tokens = match lex(sql) {
        Ok(tokens) => tokens,
        Err(_) => return Ok(None),
    };
    let mut p = Parser { tokens, pos: 0 };
    if !p.kw("lock") {
        return Ok(None);
    }
    p.kw("table");
    let mut tables = Vec::new();
    loop {
        p.kw("only");
        let mut name = p.ident()?;
        if p.tokens.get(p.pos) == Some(&Token::Dot) {
            p.pos += 1;
            name = format!("{}.{}", quote_relation(&name), quote_relation(&p.ident()?));
        } else {
            name = quote_relation(&name);
        }
        tables.push(name);
        if !p.comma() {
            break;
        }
    }
    use giggles_core::SqlTableLockMode::*;
    let mut lock_mode = AccessExclusive;
    if p.kw("in") {
        let start = p.pos;
        while p.pos < p.tokens.len() && p.tokens[p.pos] != Token::Word("mode".into()) {
            p.pos += 1;
        }
        let mode = p.tokens[start..p.pos]
            .iter()
            .map(|t| match t {
                Token::Word(w) => w.as_str(),
                _ => "?",
            })
            .collect::<Vec<_>>()
            .join(" ");
        lock_mode = match mode.as_str() {
            "access share" => AccessShare,
            "row share" => RowShare,
            "row exclusive" => RowExclusive,
            "share update exclusive" => ShareUpdateExclusive,
            "share" => Share,
            "share row exclusive" => ShareRowExclusive,
            "exclusive" => ExclusiveTable,
            "access exclusive" => AccessExclusive,
            _ => return Err("42601"),
        };
        if !p.kw("mode") {
            return Err("42601");
        }
    }
    let nowait = p.kw("nowait");
    if p.pos != p.tokens.len() {
        return Err("42601");
    }
    Ok(Some(TableLockRequest { tables, mode: lock_mode, nowait }))
}

#[derive(Clone)]
pub(crate) enum CopyDestination { Stdout, File(String), Program(String) }
#[derive(Clone)]
pub(crate) struct CopyOutput {
    pub encoding: Option<giggles_core::relational::CopyEncoding>,
    pub destination: CopyDestination,
    pub table_name: Option<String>,
    pub relation_key: String,
    pub columns: Vec<String>,
    pub all_select: Option<String>,
    pub select: String,
    // 0 = text, 1 = csv, 2 = binary.
    pub format: u8,
    pub header: bool,
    pub delimiter: u8,
    pub quote: u8,
    pub null: Vec<u8>,
    pub escape: u8,
    pub force_all: bool,
    pub force_columns: Vec<String>,
}

pub(crate) fn copy_stdout_select(sql: &str) -> Result<Option<CopyOutput>, &'static str> {
    let tokens = match lex(sql) {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    let mut p = Parser { tokens, pos: 0 };
    if !p.kw("copy") {
        return Ok(None);
    }
    let mut query = None;
    if p.tokens.get(p.pos) == Some(&Token::LParen) {
        // Find the balanced query envelope in the original SQL, preserving all
        // literals and expressions for the ordinary SELECT parser.
        let start = sql.find('(').ok_or("42601")?;
        let bytes = sql.as_bytes();
        let mut depth = 1;
        let mut i = start + 1;
        let mut quote = None;
        while i < bytes.len() && depth > 0 {
            let c = bytes[i];
            if let Some(q) = quote {
                if c == q {
                    if bytes.get(i + 1) == Some(&q) {
                        i += 1;
                    } else {
                        quote = None;
                    }
                }
            } else {
                match c {
                    b'\'' | b'"' => quote = Some(c),
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
            }
            i += 1;
        }
        if depth != 0 {
            return Err("42601");
        }
        query = Some(sql[start + 1..i - 1].to_owned());
        p.tokens = lex(&sql[i..])?;
        p.pos = 0;
    }
    let name = if query.is_some() { String::new() } else { p.ident()? };
    let (schema, name) = if p.tokens.get(p.pos) == Some(&Token::Dot) {
        p.pos += 1;
        (Some(name), p.ident()?)
    } else {
        (None, name)
    };
    let table = match &schema {
        Some(schema) => format!("{}.{}", quote_relation(schema), quote_relation(&name)),
        None => quote_relation(&name),
    };
    let relation_key = match schema {
        Some(schema) => format!("{schema}\0{name}"),
        None => name.clone(),
    };
    let mut columns = Vec::new();
    if p.tokens.get(p.pos) == Some(&Token::LParen) {
        p.pos += 1;
        columns = p.names(false)?;
        if p.tokens.get(p.pos) != Some(&Token::RParen) {
            return Err("42601");
        }
        p.pos += 1;
    }
    if !p.kw("to") {
        return Ok(None);
    }
    let destination = if p.kw("stdout") { CopyDestination::Stdout } else {
        let program = p.kw("program");
        let Some(Token::Text(value)) = p.tokens.get(p.pos) else { return Err("42601"); };
        let value = value.clone(); p.pos += 1;
        if program { CopyDestination::Program(value) } else { CopyDestination::File(value) }
    };
    let mut options = CopyOutput {
        encoding: None,
        destination,
        table_name: query.is_none().then_some(name),
        relation_key,
        columns: columns.clone(),
        all_select: query.is_none().then(|| format!("SELECT * FROM {table}")),
        select: String::new(),
        format: 0,
        header: false,
        delimiter: b'\t',
        quote: b'"',
        null: b"\\N".to_vec(),
        escape: b'"',
        force_all: false,
        force_columns: vec![],
    };
    let suffix = p.tokens[p.pos..].iter().map(|t| match t {
        Token::Word(v) => v.clone(), Token::Ident(v) => format!("\"{}\"", v.replace('"', "\"\"")),
        Token::Text(v) => format!("'{}'", v.replace('\'', "''")), Token::Comma => ",".into(), Token::Dot => ".".into(),
        Token::LParen => "(".into(), Token::RParen => ")".into(), Token::Semi => ";".into(), Token::Star => "*".into(), Token::Other => "?".into(),
    }).collect::<Vec<_>>().join(" ");
    let shared = giggles_core::relational::CopyOptions::parse(&suffix, false)
        .map_err(|e| crate::service::sqlstate(&CoreError::Sql(e)))?;
    options.format = match shared.format { giggles_core::relational::CopyFormat::Text => 0, giggles_core::relational::CopyFormat::Csv => 1, giggles_core::relational::CopyFormat::Binary => 2 };
    options.encoding = shared.encoding;
    options.header = shared.header != giggles_core::relational::CopyHeader::Off;
    options.delimiter = shared.delimiter;
    options.quote = shared.quote;
    options.escape = shared.escape;
    options.null = shared.null.as_bytes().to_vec();
    options.force_all = shared.force_quote.all;
    options.force_columns = shared.force_quote.names.clone();
    let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let projection = if columns.is_empty() {
        "*".into()
    } else {
        columns
            .iter()
            .map(|c| quoted(c))
            .collect::<Vec<_>>()
            .join(",")
    };
    options.select = query.unwrap_or_else(|| format!("SELECT {projection} FROM {table}"));
    Ok(Some(options))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn creator_authority_keeps_legacy_bytes_and_v2_admin_options() {
        let mut engines = vec![giggles_core::Engine::Legacy];
        if cfg!(feature = "engine-rocksdb") { engines.push(giggles_core::Engine::V2); }
        for engine in engines {
            let mut id = [0; 16];
            getrandom::fill(&mut id).unwrap();
            let path = std::env::temp_dir().join(format!("c4-creator-{:032x}", u128::from_be_bytes(id)));
            let admin = Admin::create_engine(&path, giggles_core::Limits::default(), engine).unwrap();
            let catalog = admin.roles();
            let actor = catalog.create(NewRole { name: "manager".into(), createrole: true, ..Default::default() }).unwrap();
            let mut notices = Vec::new();
            for sql in ["CREATE ROLE child", "ALTER ROLE child LOGIN"] {
                execute(&parse(sql).unwrap(), &admin, &actor, &mut notices).unwrap();
            }
            let edges: Vec<_> = catalog.edges().unwrap().into_iter().filter(|edge| edge.role == "child").collect();
            if engine == giggles_core::Engine::Legacy {
                assert!(edges.is_empty());
            } else {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].member, "manager");
                assert!(edges[0].admin && !edges[0].inherit && !edges[0].set);
            }
            execute(&parse("DROP ROLE child").unwrap(), &admin, &actor, &mut notices).unwrap();
            assert!(catalog.get("child").unwrap().is_none());
            drop(catalog);
            drop(admin);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    #[test]
    fn taskonly_parses() {
        let Some(AdminStatement::Create(n, c, t, _)) = parse("CREATE ROLE agent LOGIN TASKONLY") else {
            panic!()
        };
        assert_eq!(n, "agent");
        assert_eq!(c.login, Some(true));
        assert_eq!(t, Some(true));
        let Some(AdminStatement::Create(_, c, t, _)) =
            parse("CREATE USER a WITH PASSWORD 'x' TASKONLY")
        else {
            panic!()
        };
        assert_eq!(c.login, Some(true));
        assert_eq!(t, Some(true));
        let Some(AdminStatement::Alter(n, _, t)) = parse("alter role \"A\" notaskonly;") else {
            panic!()
        };
        assert_eq!(n, "A");
        assert_eq!(t, Some(false));
        let Some(AdminStatement::Alter(_, _, t)) = parse("ALTER ROLE a LOGIN") else {
            panic!()
        };
        assert_eq!(t, None);
    }
    #[test]
    fn taskonly_twice_42601() {
        for sql in [
            "CREATE ROLE a TASKONLY TASKONLY",
            "CREATE ROLE a TASKONLY NOTASKONLY",
            "ALTER ROLE a NOTASKONLY TASKONLY",
        ] {
            assert!(
                matches!(parse(sql), Some(AdminStatement::Error("42601"))),
                "{sql}"
            );
        }
    }
    #[test]
    fn core_statements_fall_through() {
        for sql in [
            "CREATE TABLE t (x int)",
            "SELECT 1",
            "GRANTED",
            "ALTER TABLE t ADD x int",
            "DROP VIEW t",
        ] {
            assert!(parse(sql).is_none());
        }
    }
    #[test]
    fn identifiers_and_passwords() {
        assert!(
            matches!(parse("CREATE ROLE\"Case\" LOGIN"), Some(AdminStatement::Create(n, _, _, _)) if n == "Case")
        );
        assert!(parse("CREATE ROLE r \"login\"").is_none());
        assert!(parse("GRANT \"select\" ON t TO r").is_none());
        let Some(AdminStatement::Create(n, c, _, _)) =
            parse("CrEaTe UsEr \"A\"\"B\" WITH PASSWORD 'a''b' NOLOGIN;")
        else {
            panic!()
        };
        assert_eq!(n, "A\"B");
        assert_eq!(c.password, Some(Some("a'b".into())));
        assert_eq!(c.login, Some(false));
        let Some(AdminStatement::Alter(n, c, _)) = parse("ALTER USER Mixed PASSWORD NULL") else {
            panic!()
        };
        assert_eq!(n, "mixed");
        assert_eq!(c.login, Some(true));
        assert_eq!(c.password, Some(None));
    }
    #[test]
    fn errors_are_ast_nodes() {
        for sql in [
            "CREATE ROLE r LOGIN NOLOGIN",
            "CREATE ROLE r PASSWORD NULL PASSWORD 'x'",
            "CREATE ROLE r LOGIN LOGIN",
            "CREATE ROLE r PASSWORD x",
            "CREATE ROLE \"\"",
            "CREATE ROLE r; SELECT 1",
        ] {
            assert!(
                matches!(parse(sql), Some(AdminStatement::Error("42601"))),
                "{sql}"
            );
        }
        for sql in [
            "CREATE ROLE r CREATEDB",
            "ALTER ROLE r NOCREATEUSER",
            "CREATE ROLE r CONNECTION LIMIT -1",
            "ALTER ROLE r CONNECTION LIMIT +1",
            "ALTER ROLE r VALID UNTIL 'tomorrow'",
            "GRANT SELECT ON t TO r WITH GRANT OPTION",
            "REVOKE SELECT ON t FROM r CASCADE",
        ] {
            assert!(
                matches!(parse(sql), Some(AdminStatement::Error("0A000"))),
                "{sql}"
            );
        }
    }
    #[test]
    fn role_membership_grammar() {
        assert!(matches!(parse("GRANT r TO s"), Some(AdminStatement::Membership { grant: true, roles, members, .. }) if roles == ["r"] && members == ["s"]));
        assert!(matches!(parse("GRANT r TO CURRENT_USER WITH ADMIN TRUE, INHERIT FALSE, SET FALSE GRANTED BY SESSION_USER"), Some(AdminStatement::Membership { options, grantor, .. }) if options.admin == Some(true) && options.inherit == Some(false) && options.set == Some(false) && grantor.as_deref() == Some("\0session_user")));
        assert!(matches!(parse("REVOKE SET OPTION FOR r FROM s CASCADE"), Some(AdminStatement::Membership { grant: false, revoke: Some(giggles_core::roles::MembershipOption::Set), cascade: true, .. })));
        assert!(matches!(parse("GRANT r TO s WITH BOGUS TRUE"), Some(AdminStatement::Error("42601"))));
    }
    #[test]
    fn grants_and_drops() {
        assert!(
            matches!(parse("DROP USER IF EXISTS a, \"B\";"),Some(AdminStatement::Drop(true,n)) if n == ["a","B"])
        );
        assert!(
            matches!(parse("GRANT SELECT, UPDATE ON TABLE public.t, u TO a,b"),Some(AdminStatement::Grant { grant:true,schema:false,objects,roles,.. }) if objects == ["public\0t","u"] && roles == ["a","b"])
        );
        assert!(matches!(
            parse("REVOKE ALL PRIVILEGES ON SCHEMA public FROM a"),
            Some(AdminStatement::Grant {
                grant: false,
                schema: true,
                ..
            })
        ));
        assert!(matches!(
            parse("GRANT USAGE ON SCHEMA other TO a"),
            Some(AdminStatement::Grant { schema: true, .. })
        ));
        assert!(matches!(
            parse("GRANT SELECT ON other.t TO a"),
            Some(AdminStatement::Grant { objects, .. }) if objects == ["other\0t"]
        ));
    }
    #[test]
    fn drop_table_identifiers() {
        for (sql, expected_exists, expected_names) in [
            ("DROP TABLE t", false, vec!["t"]),
            (
                "  DrOp TaBlE IF EXISTS PUBLIC.Mixed, \"public\".\"A\"\"B\", _t$1; ",
                true,
                vec!["public\0mixed", "public\0A\"B", "_t$1"],
            ),
            ("DROP TABLE\"Case\";", false, vec!["Case"]),
        ] {
            let Some(AdminStatement::DropTable(if_exists, names)) = parse(sql) else {
                panic!("{sql}");
            };
            assert_eq!(if_exists, expected_exists, "{sql}");
            assert_eq!(names, expected_names, "{sql}");
        }
    }
    #[test]
    fn drop_table_errors() {
        for sql in [
            "DROP TABLE",
            "DROP TABLE IF t",
            "DROP TABLE IF EXISTS",
            "DROP TABLE t,",
            "DROP TABLE ,t",
            "DROP TABLE t u",
            "DROP TABLE t; SELECT 1",
            "DROP TABLE t;;",
            "DROP TABLE 123",
            "DROP TABLE ''",
            "DROP TABLE \"\"",
            "DROP TABLE \"unterminated",
            "DROP TABLE public.",
            "DROP TABLE public.public.t",
            "DROP TABLE \"nul\0name\"",
        ] {
            assert!(
                matches!(parse(sql), Some(AdminStatement::Error("42601"))),
                "{sql}"
            );
        }
        for name in ["x".repeat(64), format!("\"{}\"", "ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©".repeat(32))] {
            assert!(matches!(
                parse(&format!("DROP TABLE {name}")),
                Some(AdminStatement::Error("42601"))
            ));
        }
        for sql in [
            "DROP TABLE t CASCADE",
            "DROP TABLE IF EXISTS public.t, u RESTRICT;",
        ] {
            assert!(
                parse(sql).is_none(),
                "{sql}"
            );
        }
        for sql in ["DROP TABLE other.t", "DROP TABLE \"Public\".t"] {
            assert!(
                matches!(parse(sql), Some(AdminStatement::DropTable(_, _))),
                "{sql}"
            );
        }
    }
    #[test]
    fn name_byte_limit_and_quoting() {
        assert!(parse("CREATE ROLE 123").is_none());
        assert!(parse("CREATE ROLE r UNKNOWN_OPTION").is_none());
        assert!(matches!(
            parse("GRANT SELECT ON t TO \"public\""),
            Some(AdminStatement::Grant { .. })
        ));
        assert!(matches!(
            parse(&format!("CREATE ROLE {}", "x".repeat(64))),
            Some(AdminStatement::Error("42601"))
        ));
        assert!(matches!(
            parse("CREATE ROLE \"unterminated"),
            Some(AdminStatement::Error("42601"))
        ));
        assert!(matches!(
            parse(&format!("CREATE ROLE \"{}\"", "ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©".repeat(32))),
            Some(AdminStatement::Error("42601"))
        ));
    }
}
