//! PostgreSQL transaction-mode execution; agent dispatch retains its base grammar.
use super::simple::warning;
use super::*;

pub(super) fn parse_control(
    sql: &str,
    session: &Session,
    diagnostic: &mut Diagnostic,
) -> R<Option<crate::pg_transaction::Command>> {
    let original = sql;
    let statements = super::simple::split(sql)?;
    let sql = if statements.len() == 1 {
        statements[0].as_str()
    } else {
        sql
    };
    if matches!(session.role, Role::Database { .. }) {
        diagnostic.settings(crate::pg_transaction::parse(sql))
    } else {
        match crate::pg_transaction::parse(sql) {
            Ok(Some(
                command @ (crate::pg_transaction::Command::Savepoint(_)
                | crate::pg_transaction::Command::Release(_)
                | crate::pg_transaction::Command::RollbackTo(_)),
            )) => Ok(Some(command)),
            _ => Ok(control(original).map(crate::pg_transaction::Command::legacy)),
        }
    }
}
pub(super) fn execute_mode(
    command: crate::pg_transaction::Command,
    session: &mut Session,
    implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings,
    diagnostic: &mut Diagnostic,
    out: &mut Vec<u8>,
) -> R<&'static str> {
    use crate::pg_transaction::Command;
    if !session.role.is_sql() {
        return Err("42501");
    }
    match command {
        Command::Begin(modes) | Command::Start(modes) => {
            let start_tag = matches!(command, Command::Start(_));
            let starting = !session.in_transaction() || *implicit;
            let actual = execute_control("BEGIN", session, implicit, settings, out)?;
            if starting {
                if let Some(isolation) = modes.isolation {
                    // Re-level the transaction BEGIN just opened (GD-0075 R5-76). An
                    // implicit block that already wrote cannot change level.
                    session
                        .sql
                        .set_transaction_isolation(crate::pg_transaction::sql_isolation(isolation))
                        .map_err(|_| "25001")?;
                    settings.current.transaction_isolation = isolation;
                }
                if let Some(read_only) = modes.read_only {
                    settings.current.transaction_read_only = read_only;
                }
            }
            Ok(if start_tag { "START TRANSACTION" } else { actual })
        }
        Command::Commit => execute_control("COMMIT", session, implicit, settings, out),
        Command::Rollback => execute_control("ROLLBACK", session, implicit, settings, out),
        Command::Savepoint(ref name)
        | Command::Release(ref name)
        | Command::RollbackTo(ref name) => {
            let tag = match &command {
                Command::Savepoint(_) => "SAVEPOINT",
                Command::Release(_) => "RELEASE",
                _ => "ROLLBACK",
            };
            if session.is_aborted() && tag != "ROLLBACK" {
                return Err("25P02");
            }
            if !session.in_transaction() || *implicit {
                diagnostic.message = Some(match tag {
                    "SAVEPOINT" => "SAVEPOINT can only be used in transaction blocks",
                    "RELEASE" => "RELEASE SAVEPOINT can only be used in transaction blocks",
                    _ => "ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
                });
                return Err("25P01");
            }
            let sql = format!(
                "{} \"{}\"",
                if tag == "ROLLBACK" {
                    "ROLLBACK TO"
                } else {
                    tag
                },
                name.replace('"', "\"\"")
            );
            let prepared = core(session.prepare(&sql))?;
            let before = settings.current.clone();
            if let Err(error) = session.execute_prepared(&prepared, &[]) {
                let code = crate::service::sqlstate(&error);
                if code == "3B001" {
                    diagnostic.wire_message = Some(format!("savepoint \"{name}\" does not exist"));
                }
                return Err(code);
            }
            match tag {
                "SAVEPOINT" => settings.savepoint(name.clone()),
                "RELEASE" => settings.release(name),
                _ => settings.rollback_to(name),
            }
            settings_status(&before, settings, session, out)?;
            Ok(tag)
        }
        Command::Set(modes) => {
            if session.is_aborted() {
                return Err("25P02");
            }
            if !session.in_transaction() {
                warning(
                    "SET TRANSACTION can only be used in transaction blocks",
                    &settings.current,
                    out,
                )?;
            } else {
                if settings.queried {
                    diagnostic.message =
                        Some("SET TRANSACTION ISOLATION LEVEL must be called before any query");
                    return Err("25001");
                }
                if let Some(isolation) = modes.isolation {
                    if session
                        .sql
                        .set_transaction_isolation(crate::pg_transaction::sql_isolation(isolation))
                        .is_err()
                    {
                        diagnostic.message =
                            Some("SET TRANSACTION ISOLATION LEVEL must be called before any query");
                        return Err("25001");
                    }
                    settings.current.transaction_isolation = isolation;
                }
                if let Some(read_only) = modes.read_only {
                    settings.current.transaction_read_only = read_only;
                }
            }
            Ok("SET")
        }
        Command::Session(modes) => {
            use crate::pg_settings::{Setting, Show};
            if session.is_aborted() {
                return Err("25P02");
            }
            for (name, value) in [
                (
                    Show::DefaultTransactionIsolation,
                    modes.isolation.map(|level| level.name().to_owned()),
                ),
                (
                    Show::DefaultTransactionReadOnly,
                    modes
                        .read_only
                        .map(|on| if on { "on" } else { "off" }.into()),
                ),
            ] {
                if let Some(value) = value {
                    settings.apply(
                        &Setting::Database {
                            name,
                            value: Some(value),
                            local: false,
                            reset: false,
                        },
                        session.in_transaction(),
                    );
                }
            }
            Ok("SET")
        }
    }
}
pub(super) fn admin_command(stmt: &crate::auth::statements::AdminStatement) -> &'static str {
    use crate::auth::statements::AdminStatement;
    match stmt {
        AdminStatement::Create(..) => "CREATE ROLE",
        AdminStatement::Alter(..) => "ALTER ROLE",
        AdminStatement::Drop(..) => "DROP ROLE",
        AdminStatement::DropTable(..) => "DROP TABLE",
        AdminStatement::Grant { grant: true, .. } => "GRANT",
        AdminStatement::Grant { grant: false, .. } => "REVOKE",
        AdminStatement::Error(..) => "SELECT", // Let the original syntax diagnostic win.
    }
}
pub(super) fn read_only_check(
    command: &'static str,
    settings: &crate::pg_settings::SessionSettings,
    session: &Session,
    diagnostic: &mut Diagnostic,
) -> R<()> {
    if !matches!(session.role, Role::Database { .. }) {
        return Ok(());
    }
    let read_only = if session.in_transaction() {
        settings.current.transaction_read_only
    } else {
        settings.current.default_read_only
    };
    if !read_only {
        return Ok(());
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    diagnostic.message = Some(match command {
        "INSERT" => "cannot execute INSERT in a read-only transaction",
        "UPDATE" => "cannot execute UPDATE in a read-only transaction",
        "DELETE" => "cannot execute DELETE in a read-only transaction",
        "TRUNCATE TABLE" => "cannot execute TRUNCATE in a read-only transaction",
        "CREATE TABLE" => "cannot execute CREATE TABLE in a read-only transaction",
        "CREATE VIEW" => "cannot execute CREATE VIEW in a read-only transaction",
        "DROP VIEW" => "cannot execute DROP VIEW in a read-only transaction",
        "COMMENT" => "cannot execute COMMENT in a read-only transaction",
        "SECURITY LABEL" => "cannot execute SECURITY LABEL in a read-only transaction",
        "ALTER TABLE" => "cannot execute ALTER TABLE in a read-only transaction",
        "CREATE TYPE" => "cannot execute CREATE TYPE in a read-only transaction",
        "CREATE DOMAIN" => "cannot execute CREATE DOMAIN in a read-only transaction",
        "DROP TYPE" => "cannot execute DROP TYPE in a read-only transaction",
        "DROP DOMAIN" => "cannot execute DROP DOMAIN in a read-only transaction",
        "CREATE INDEX" => "cannot execute CREATE INDEX in a read-only transaction",
        "DROP TABLE" => "cannot execute DROP TABLE in a read-only transaction",
        "CREATE SEQUENCE" => "cannot execute CREATE SEQUENCE in a read-only transaction",
        "ALTER SEQUENCE" => "cannot execute ALTER SEQUENCE in a read-only transaction",
        "DROP SEQUENCE" => "cannot execute DROP SEQUENCE in a read-only transaction",
        "CREATE ROLE" => "cannot execute CREATE ROLE in a read-only transaction",
        "ALTER ROLE" => "cannot execute ALTER ROLE in a read-only transaction",
        "DROP ROLE" => "cannot execute DROP ROLE in a read-only transaction",
        "GRANT" => "cannot execute GRANT in a read-only transaction",
        "REVOKE" => "cannot execute REVOKE in a read-only transaction",
        _ => return Ok(()),
    });
    Err("25006")
}
