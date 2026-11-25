//! PostgreSQL transaction modes, and the level each storage engine executes.
use crate::pg_settings::database::{Parser, Token};

type Result<T> = std::result::Result<T, &'static str>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IsolationLevel {
    Serializable,
    RepeatableRead,
    ReadCommitted,
    ReadUncommitted,
}
impl IsolationLevel {
    pub fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "repeatable read" => Ok(Self::RepeatableRead),
            "read committed" => Ok(Self::ReadCommitted),
            "read uncommitted" => Ok(Self::ReadUncommitted),
            _ => Err("22023"),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Serializable => "serializable",
            Self::RepeatableRead => "repeatable read",
            Self::ReadCommitted => "read committed",
            Self::ReadUncommitted => "read uncommitted",
        }
    }
}
/// The level `engine` executes for a `requested` level, which is what
/// `transaction_isolation` and `default_transaction_isolation` report
/// (GD-0075 R5-75). Sessions keep the requested level; only reporting maps it.
/// - Legacy publishes each SQL transaction only if no other commit happened
///   since its snapshot, so every request runs SERIALIZABLE.
/// - v2 runs the requested level (R5-76). READ UNCOMMITTED runs as READ
///   COMMITTED and, as in PostgreSQL, still reports `read uncommitted`.
pub(crate) fn effective_isolation(
    requested: IsolationLevel,
    engine: giggles_core::Engine,
) -> IsolationLevel {
    match engine {
        giggles_core::Engine::Legacy => IsolationLevel::Serializable,
        giggles_core::Engine::V2 => requested,
    }
}
/// The v2 engine level for a requested PostgreSQL level.
pub(crate) fn sql_isolation(requested: IsolationLevel) -> giggles_core::SqlIsolation {
    match requested {
        IsolationLevel::Serializable => giggles_core::SqlIsolation::Serializable,
        IsolationLevel::RepeatableRead => giggles_core::SqlIsolation::RepeatableRead,
        IsolationLevel::ReadCommitted | IsolationLevel::ReadUncommitted => {
            giggles_core::SqlIsolation::ReadCommitted
        }
    }
}
/// PostgreSQL's default requested level; `effective_isolation` gives what runs.
pub(crate) const DEFAULT_ISOLATION: IsolationLevel = IsolationLevel::ReadCommitted;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Modes {
    pub isolation: Option<IsolationLevel>,
    pub read_only: Option<bool>,
    pub deferrable: Option<bool>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    Begin(Modes),
    Start(Modes),
    Commit,
    Rollback,
    Savepoint(String),
    Release(String),
    RollbackTo(String),
    Set(Modes),
    Prepare(String),
    CommitPrepared(String),
    RollbackPrepared(String),
    ImportSnapshot { id: String, local: bool },
    Session(Modes),
}
fn savepoint_name(p: &mut Parser) -> Result<String> {
    let mut name = match p.tokens.get(p.at) {
        Some(Token::Word(s))
            if s.chars()
                .next()
                .is_some_and(|c| c.is_alphabetic() || c == '_') =>
        {
            s.to_ascii_lowercase()
        }
        Some(Token::Ident(s)) if !s.is_empty() => s.clone(),
        _ => return Err("42601"),
    };
    let mut end = name.len().min(63);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name.truncate(end);
    p.at += 1;
    Ok(name)
}
impl Command {
    pub fn legacy(command: &str) -> Self {
        match command {
            "BEGIN" => Self::Begin(Modes::default()),
            "COMMIT" => Self::Commit,
            "ROLLBACK" => Self::Rollback,
            _ => unreachable!(),
        }
    }
}
fn modes(p: &mut Parser, required: bool) -> Result<Modes> {
    let mut modes = Modes::default();
    let start = p.at;
    loop {
        if p.at == p.tokens.len() {
            if required && p.at == start {
                return Err("42601");
            }
            return Ok(modes);
        }
        if p.kw("ISOLATION") {
            p.need("LEVEL")?;
            modes.isolation = Some(if p.kw("SERIALIZABLE") {
                IsolationLevel::Serializable
            } else if p.kw("REPEATABLE") {
                p.need("READ")?;
                IsolationLevel::RepeatableRead
            } else {
                p.need("READ")?;
                if p.kw("COMMITTED") {
                    IsolationLevel::ReadCommitted
                } else {
                    p.need("UNCOMMITTED")?;
                    IsolationLevel::ReadUncommitted
                }
            });
        } else if p.kw("READ") {
            modes.read_only = Some(if p.kw("ONLY") {
                true
            } else {
                p.need("WRITE")?;
                false
            });
        } else if p.kw("NOT") {
            p.need("DEFERRABLE")?;
            modes.deferrable = Some(false);
        } else if p.kw("DEFERRABLE") {
            modes.deferrable = Some(true);
        } else {
            return Err("42601");
        }
        if p.comma() && p.at == p.tokens.len() {
            return Err("42601");
        }
    }
}
fn leading_word(sql: &str) -> (&str, &str) {
    let sql = sql.trim_start();
    let end = sql
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '$')
        .unwrap_or(sql.len());
    sql.split_at(end)
}

pub(crate) fn is_prepare_transaction(sql: &str) -> bool {
    let (first, tail) = leading_word(sql);
    let (second, tail) = leading_word(tail);
    first.eq_ignore_ascii_case("PREPARE")
        && second.eq_ignore_ascii_case("TRANSACTION")
        && starts_string(tail)
}

fn starts_string(sql: &str) -> bool {
    let sql = sql.trim_start();
    sql.starts_with(['\'', '$']) || sql.starts_with("E'") || sql.starts_with("e'")
}

fn string(sql: &str) -> Result<String> {
    let sql = sql.trim_end();
    let sql = sql.strip_suffix(';').unwrap_or(sql);
    giggles_core::relational::parse_string_literal(sql).map_err(|error| {
        giggles_core::sqlstate(&giggles_core::CoreError::Sql(error))
    })
}

pub(crate) fn parse(sql: &str) -> Result<Option<Command>> {
    let (first, tail) = leading_word(sql);
    if ![
        "BEGIN",
        "START",
        "COMMIT",
        "PREPARE",
        "END",
        "ROLLBACK",
        "ABORT",
        "SET",
        "SAVEPOINT",
        "RELEASE",
    ]
    .iter()
    .any(|s| first.eq_ignore_ascii_case(s))
    {
        return Ok(None);
    }
    let (second, rest) = leading_word(tail);
    if first.eq_ignore_ascii_case("PREPARE") {
        return if is_prepare_transaction(sql) {
            string(rest).map(Command::Prepare).map(Some)
        } else {
            Ok(None)
        };
    }
    if second.eq_ignore_ascii_case("PREPARED") {
        if first.eq_ignore_ascii_case("COMMIT") {
            return string(rest).map(Command::CommitPrepared).map(Some);
        }
        if first.eq_ignore_ascii_case("ROLLBACK") {
            return string(rest).map(Command::RollbackPrepared).map(Some);
        }
    }
    if first.eq_ignore_ascii_case("SET") {
        let local = second.eq_ignore_ascii_case("LOCAL");
        let (transaction, rest) = if local || second.eq_ignore_ascii_case("SESSION") {
            leading_word(rest)
        } else {
            (second, rest)
        };
        let (snapshot, id) = leading_word(rest);
        if transaction.eq_ignore_ascii_case("TRANSACTION") && snapshot.eq_ignore_ascii_case("SNAPSHOT") {
            return string(id).map(|id| Some(Command::ImportSnapshot { id, local }));
        }
    }
    let mut p = Parser::new(sql)?;
    let command = if p.kw("BEGIN") {
        if !p.kw("WORK") {
            p.kw("TRANSACTION");
        }
        Command::Begin(modes(&mut p, false)?)
    } else if p.kw("START") {
        p.need("TRANSACTION")?;
        Command::Start(modes(&mut p, false)?)
    } else if p.kw("COMMIT") || p.kw("END") {
        if !p.kw("WORK") {
            p.kw("TRANSACTION");
        }
        Command::Commit
    } else if p.kw("ROLLBACK") || p.kw("ABORT") {
        if !p.kw("WORK") {
            p.kw("TRANSACTION");
        }
        if p.kw("TO") {
            if p.tokens.len() - p.at > 1 {
                p.kw("SAVEPOINT");
            }
            Command::RollbackTo(savepoint_name(&mut p)?)
        } else {
            Command::Rollback
        }
    } else if p.kw("SAVEPOINT") {
        Command::Savepoint(savepoint_name(&mut p)?)
    } else if p.kw("RELEASE") {
        if p.tokens.len() - p.at > 1 {
            p.kw("SAVEPOINT");
        }
        Command::Release(savepoint_name(&mut p)?)
    } else {
        p.need("SET")?;
        if p.kw("TRANSACTION") {
            Command::Set(modes(&mut p, true)?)
        } else if p.kw("SESSION") && p.kw("CHARACTERISTICS") {
            p.need("AS")?;
            p.need("TRANSACTION")?;
            Command::Session(modes(&mut p, true)?)
        } else {
            return Ok(None);
        }
    };
    p.end()?;
    Ok(Some(command))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn read_uncommitted_runs_read_committed() {
        assert_eq!(
            sql_isolation(IsolationLevel::ReadUncommitted),
            giggles_core::SqlIsolation::ReadCommitted
        );
        assert_eq!(
            sql_isolation(IsolationLevel::RepeatableRead),
            giggles_core::SqlIsolation::RepeatableRead
        );
    }
    #[test]
    fn every_requested_isolation_maps_to_the_engine_level() {
        for requested in [
            IsolationLevel::Serializable,
            IsolationLevel::RepeatableRead,
            IsolationLevel::ReadCommitted,
            IsolationLevel::ReadUncommitted,
        ] {
            assert_eq!(
                effective_isolation(requested, giggles_core::Engine::Legacy),
                IsolationLevel::Serializable
            );
            assert_eq!(
                effective_isolation(requested, giggles_core::Engine::V2),
                requested
            );
        }
    }
    #[test]
    fn bounded_modes_reject_suffixes_and_trailing_commas() {
        for sql in [
            "BEGIN READ ONLY,",
            "START",
            "COMMIT AND CHAIN",
            "SET TRANSACTION",
            "BEGIN ISOLATION LEVEL READ UNKNOWN",
            "END WORK extra",
        ] {
            assert_eq!(parse(sql), Err("42601"), "{sql}");
        }
        assert!(matches!(
            parse("begin work isolation level read committed, read only not deferrable"),
            Ok(Some(Command::Begin(_)))
        ));
        assert_eq!(parse("SET SESSION application_name='x'"), Ok(None));
    }
}
