//! PostgreSQL administrative statements that have no storage effects.
use super::*;

#[derive(Debug, Clone)]
pub(crate) enum Statement {
    Comment(comments::AdminTarget),
    Tablespace(tablespace::Command),
    Load(String),
    SecurityLabel(Option<String>),
}

pub(super) fn claims(tokens: &[Token]) -> bool {
    comments::admin_claims(tokens) || tablespace::claims(tokens) || load::claims(tokens)
}

pub(super) fn parse(p: &mut Parser) -> R<Statement> {
    let statement = if p.word("comment") {
        Statement::Comment(comments::parse_admin(p)?)
    } else if p.word("load") {
        Statement::Load(string(p)?)
    } else {
        Statement::Tablespace(tablespace::parse(p)?)
    };
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(statement)
}

pub(super) fn string(p: &mut Parser) -> R<String> {
    let Some(Token::String(value)) = p.tokens.get(p.at) else {
        return Err(SqlError::Syntax);
    };
    p.at += 1;
    Ok(value.clone())
}

impl Statement {
    pub(crate) fn command(&self) -> &'static str {
        match self {
            Self::Comment(_) => "COMMENT",
            Self::Tablespace(command) => command.tag(),
            Self::Load(_) => "LOAD",
            Self::SecurityLabel(_) => "SECURITY LABEL",
        }
    }

    pub(crate) fn requires_standalone(&self) -> bool {
        matches!(self, Self::Tablespace(command) if command.requires_standalone())
    }

    pub(crate) fn execute(
        &self,
        prepared: &Prepared,
        state: &Relational,
        actor: Option<&crate::roles::CatalogRole>,
        roles: &crate::roles::RoleCatalog,
    ) -> Result<SqlResult, crate::CoreError> {
        let superuser = actor.is_none_or(|actor| actor.superuser);
        match self {
            Self::Comment(target) => {
                comments::execute_admin(target, prepared, state, actor, roles)?
            }
            Self::Load(filename) => {
                load::execute(filename, superuser).map_err(crate::CoreError::Sql)?
            }
            Self::Tablespace(command) => command.execute(actor, roles)?,
            Self::SecurityLabel(provider) => {
                return Err(crate::CoreError::Sql(security_label::refuse(provider.as_deref())));
            }
        }
        Ok(SqlResult::default())
    }
}

impl Prepared {
    pub(crate) fn admin_utility(&self) -> Option<&Statement> {
        match self.statement.get() {
            super::Statement::AdminUtility(utility) => Some(utility),
            _ => None,
        }
    }

    /// These statements authorize their own catalog objects during execution.
    pub fn is_admin_utility(&self) -> bool {
        self.admin_utility().is_some()
    }

    /// Whether pgwire must leave the command outside an implicit block.
    pub fn requires_standalone_admin_utility(&self) -> bool {
        self.admin_utility()
            .is_some_and(Statement::requires_standalone)
    }
}

pub(crate) fn block_error(command: &str) -> SqlError {
    user_types::exact_sql_error(
        SqlError::ActiveSqlTransaction,
        format!("{command} cannot run inside a transaction block"),
        None,
    )
}

pub(super) fn missing(kind: &str, name: &str) -> SqlError {
    user_types::exact_sql_error(
        SqlError::UndefinedObject,
        format!("{kind} \"{name}\" does not exist"),
        None,
    )
}

pub(super) fn must_own(kind: &str, name: &str) -> SqlError {
    user_types::exact_sql_error(
        SqlError::InsufficientPrivilege,
        format!("must be owner of {kind} {name}"),
        None,
    )
}

pub(crate) fn role_error(error: crate::roles::RoleError) -> crate::CoreError {
    match error {
        crate::roles::RoleError::Core(error) => error,
        _ => crate::CoreError::Sql(SqlError::InternalError),
    }
}
