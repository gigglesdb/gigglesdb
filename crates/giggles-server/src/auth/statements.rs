//! Narrow role/ACL and DROP TABLE SQL surface. Password-bearing syntax has no Debug.
use super::scram;
use giggles_core::{
    Admin, CoreError,
    relational::SqlError,
    roles::{CatalogRole, NewRole, RoleChanges, RoleError, SchemaPrivilege, TablePrivilege},
};

#[derive(Clone)]
pub(crate) enum AdminStatement {
    Error(&'static str),
    Create(String, RoleChanges),
    Alter(String, RoleChanges),
    Drop(bool, Vec<String>),
    /// Existence and authorization are validated for all names before dropping.
    /// Multi-name drops are not atomic: a later conflict can leave earlier names dropped.
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
                if name != "public" {
                    return Err("3F000");
                }
                name = self.ident()?;
            }
            names.push(name);
            if !self.comma() {
                break;
            }
        }
        Ok(names)
    }
    fn options(&mut self, user: bool) -> Result<RoleChanges, &'static str> {
        self.kw("with");
        let mut c = RoleChanges::default();
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
                "createdb" | "nocreatedb" | "inherit" | "noinherit" | "replication"
                | "noreplication" | "bypassrls" | "nobypassrls" | "connection" | "valid" | "in"
                | "role" | "admin" | "sysid" | "encrypted" | "unencrypted" | "createuser"
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
        Ok(c)
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
            let changes = self.options(user)?;
            return Ok(if create {
                AdminStatement::Create(name, changes)
            } else {
                AdminStatement::Alter(name, changes)
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
            if self.pos != self.tokens.len() {
                return Err(if !table || self.kw("cascade") || self.kw("restrict") {
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
        let role_start = self.pos;
        let roles = self.names(false)?;
        if self.tokens[role_start..self.pos]
            .iter()
            .any(|t| *t == Token::Word("public".into()))
            || self.pos != self.tokens.len()
        {
            return Err("0A000");
        }
        if schema && objects.iter().any(|s| s != "public") {
            return Err("3F000");
        }
        if privileges.iter().any(|p| {
            if schema {
                !matches!(p.as_str(), "usage" | "create" | "all")
            } else {
                !matches!(
                    p.as_str(),
                    "select" | "insert" | "update" | "delete" | "all"
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
fn state(e: RoleError) -> &'static str {
    match e {
        RoleError::Duplicate => "42710",
        RoleError::Missing => "42704",
        RoleError::Dependent => "2BP01",
        RoleError::Invalid => "22023",
        RoleError::Limit => "54000",
        RoleError::Core(_) => "XX000",
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
) -> Result<&'static str, &'static str> {
    let catalog = admin.roles();
    match stmt {
        AdminStatement::Error(e) => Err(*e),
        AdminStatement::Create(name, c) => {
            if !actor.superuser
                && (!actor.createrole || c.superuser == Some(true) || c.createrole == Some(true))
            {
                return Err("42501");
            }
            let c = password(c);
            catalog
                .create(NewRole {
                    name: name.clone(),
                    superuser: c.superuser.unwrap_or(false),
                    login: c.login.unwrap_or(false),
                    createrole: c.createrole.unwrap_or(false),
                    password: c.password.flatten(),
                })
                .map_err(state)?;
            Ok("CREATE ROLE")
        }
        AdminStatement::Alter(name, c) => {
            let target = catalog.get(name).map_err(state)?.ok_or("42704")?;
            let own_password = name == &actor.name
                && c.password.is_some()
                && c.superuser.is_none()
                && c.createrole.is_none()
                && c.login.is_none();
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
            catalog.alter(name, password(c)).map_err(state)?;
            Ok("ALTER ROLE")
        }
        AdminStatement::Drop(exists, names) => {
            if !actor.superuser && !actor.createrole {
                return Err("42501");
            }
            let tables = admin.catalog().map_err(|_| "XX000")?.1;
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
                    if catalog.table_owner(&t.name).map_err(state)?.as_deref() == Some(name) {
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
            for name in names {
                if !tables.iter().any(|table| &table.name == name) {
                    if *if_exists {
                        continue;
                    }
                    return Err("42P01");
                }
                if !actor.superuser
                    && catalog.table_owner(name).map_err(|_| "XX000")?.as_deref()
                        != Some(&actor.name)
                {
                    return Err("42501");
                }
            }
            let sequence_names = admin
                .sql_session()
                .sequence_owners()
                .map_err(|e| crate::service::sqlstate(&e))?;
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
        AdminStatement::Grant {
            grant,
            schema,
            objects,
            roles,
            privileges,
        } => {
            if *schema && !actor.superuser {
                return Err("42501");
            }
            let tables = admin.catalog().map_err(|_| "XX000")?.1;
            for object in objects {
                if !schema {
                    if !tables.iter().any(|t| &t.name == object) {
                        return Err("42P01");
                    }
                    if !actor.superuser
                        && catalog.table_owner(object).map_err(state)?.as_deref()
                            != Some(&actor.name)
                    {
                        return Err("42501");
                    }
                }
            }
            for role in roles {
                catalog.get(role).map_err(state)?.ok_or("42704")?;
            }
            let all = privileges.iter().any(|p| p == "all");
            let tp: Vec<_> = [
                ("select", TablePrivilege::Select),
                ("insert", TablePrivilege::Insert),
                ("update", TablePrivilege::Update),
                ("delete", TablePrivilege::Delete),
            ]
            .into_iter()
            .filter(|(s, _)| all || privileges.iter().any(|p| p == s))
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
                    if *schema {
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

// Parse the bounded pg_dump lock surface without granting admin authority.
pub(crate) fn access_share_tables(sql: &str) -> Result<Option<Vec<String>>, &'static str> {
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
    if !p.kw("in") || !p.kw("access") || !p.kw("share") || !p.kw("mode") {
        return Err("0A000");
    }
    p.kw("nowait");
    if p.pos != p.tokens.len() {
        return Err("42601");
    }
    Ok(Some(tables))
}

// COPY TO accepts only the text STDOUT envelope used by pg_dump.
pub(crate) fn copy_stdout_select(sql: &str) -> Result<Option<String>, &'static str> {
    let tokens = match lex(sql) {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    let mut p = Parser { tokens, pos: 0 };
    if !p.kw("copy") || p.tokens.get(p.pos) == Some(&Token::LParen) {
        return Ok(None);
    }
    let mut table = p.ident()?;
    table = if p.tokens.get(p.pos) == Some(&Token::Dot) {
        p.pos += 1;
        format!("{}.{}", quote_relation(&table), quote_relation(&p.ident()?))
    } else {
        quote_relation(&table)
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
    if !p.kw("stdout") || p.pos != p.tokens.len() {
        return Err("0A000");
    }
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
    Ok(Some(format!("SELECT {projection} FROM {table}")))
}

#[cfg(test)]
mod tests {
    use super::*;
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
            matches!(parse("CREATE ROLE\"Case\" LOGIN"), Some(AdminStatement::Create(n, _)) if n == "Case")
        );
        assert!(parse("CREATE ROLE r \"login\"").is_none());
        assert!(parse("GRANT \"select\" ON t TO r").is_none());
        let Some(AdminStatement::Create(n, c)) =
            parse("CrEaTe UsEr \"A\"\"B\" WITH PASSWORD 'a''b' NOLOGIN;")
        else {
            panic!()
        };
        assert_eq!(n, "A\"B");
        assert_eq!(c.password, Some(Some("a'b".into())));
        assert_eq!(c.login, Some(false));
        let Some(AdminStatement::Alter(n, c)) = parse("ALTER USER Mixed PASSWORD NULL") else {
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
            "GRANT r TO s",
            "GRANT SELECT ON t TO PUBLIC",
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
    fn grants_and_drops() {
        assert!(
            matches!(parse("DROP USER IF EXISTS a, \"B\";"),Some(AdminStatement::Drop(true,n)) if n == ["a","B"])
        );
        assert!(
            matches!(parse("GRANT SELECT, UPDATE ON TABLE public.t, u TO a,b"),Some(AdminStatement::Grant { grant:true,schema:false,objects,roles,.. }) if objects == ["t","u"] && roles == ["a","b"])
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
            Some(AdminStatement::Error("3F000"))
        ));
        assert!(matches!(
            parse("GRANT SELECT ON other.t TO a"),
            Some(AdminStatement::Error("3F000"))
        ));
    }
    #[test]
    fn drop_table_identifiers() {
        for (sql, expected_exists, expected_names) in [
            ("DROP TABLE t", false, vec!["t"]),
            (
                "  DrOp TaBlE IF EXISTS PUBLIC.Mixed, \"public\".\"A\"\"B\", _t$1; ",
                true,
                vec!["mixed", "A\"B", "_t$1"],
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
        for name in ["x".repeat(64), format!("\"{}\"", "é".repeat(32))] {
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
                matches!(parse(sql), Some(AdminStatement::Error("0A000"))),
                "{sql}"
            );
        }
        for sql in ["DROP TABLE other.t", "DROP TABLE \"Public\".t"] {
            assert!(
                matches!(parse(sql), Some(AdminStatement::Error("3F000"))),
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
            parse(&format!("CREATE ROLE \"{}\"", "é".repeat(32))),
            Some(AdminStatement::Error("42601"))
        ));
    }
}
