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
    let actual = match command {
        Command::Begin(modes) | Command::Start(modes) => {
            let start_tag = matches!(command, Command::Start(_));
            let starting = !session.in_transaction() || *implicit;
            let actual = execute_control("BEGIN", session, implicit, settings, diagnostic, out)?;
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
                if let Some(deferrable) = modes.deferrable {
                    settings.current.transaction_deferrable = deferrable;
                }
            }
            if start_tag { "START TRANSACTION" } else { actual }
        }
        Command::Prepare(gid) => {
            let in_block = session.in_transaction() || *implicit;
            if !in_block || *implicit {
                warning("there is no transaction in progress", &settings.current, out)?;
            }
            if !in_block || session.is_aborted() {
                execute_control("ROLLBACK", session, implicit, settings, diagnostic, out)?;
                "ROLLBACK"
            } else {
                let exported = session.sql.has_exported_snapshots();
                if !*implicit {
                    let before = settings.current.clone();
                    execute_control("ROLLBACK", session, implicit, settings, diagnostic, &mut Vec::new())?;
                    diagnostic.pre_rollback_settings = Some(Box::new(before));
                }
                diagnostic.wire_exact = true;
                if exported {
                    diagnostic.message = Some("cannot PREPARE a transaction that has exported snapshots");
                    return Err("0A000");
                }
                if gid.len() >= 200 {
                    diagnostic.wire_message = Some(format!("transaction identifier \"{gid}\" is too long"));
                    return Err("22023");
                }
                diagnostic.message = Some("prepared transactions are disabled");
                diagnostic.wire_hint = Some("Set \"max_prepared_transactions\" to a nonzero value.".into());
                return Err("55000");
            }
        }
        Command::CommitPrepared(ref gid) | Command::RollbackPrepared(ref gid) => {
            if session.is_aborted() {
                return Err("25P02");
            }
            diagnostic.wire_exact = true;
            if session.in_transaction() || *implicit {
                diagnostic.message = Some(if matches!(command, Command::CommitPrepared(_)) {
                    "COMMIT PREPARED cannot run inside a transaction block"
                } else {
                    "ROLLBACK PREPARED cannot run inside a transaction block"
                });
                return Err("25001");
            }
            diagnostic.wire_message = Some(format!("prepared transaction with identifier \"{gid}\" does not exist"));
            return Err("42704");
        }
        Command::ImportSnapshot { id, local } => {
            if session.is_aborted() {
                return Err("25P02");
            }
            if local {
                diagnostic.wire_exact = true;
                diagnostic.message = Some("SET LOCAL TRANSACTION SNAPSHOT is not implemented");
                return Err("0A000");
            }
            return import_snapshot(&id, session, *implicit, settings, diagnostic, out);
        }
        Command::Commit => {
            let was = session.in_transaction();
            // execute_control appends the bounded control reply after publication.
            let actual = execute_control("COMMIT", session, implicit, settings, diagnostic, out)?;
            diagnostic.committed |= was && actual == "COMMIT";
            return Ok(actual);
        }
        Command::Rollback => execute_control("ROLLBACK", session, implicit, settings, diagnostic, out)?,
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
            // GD-0061 R3-311: a savepoint command returns no rows; the
            // unreleased result is discarded.
            if let Err(error) = session.execute_prepared_unreleased(&prepared, &[]) {
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
            tag
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
                let changes_isolation = modes.isolation
                    .is_some_and(|isolation| isolation != settings.current.transaction_isolation);
                if settings.queried && (changes_isolation || modes.deferrable.is_none()) {
                    diagnostic.message =
                        Some("SET TRANSACTION ISOLATION LEVEL must be called before any query");
                    return Err("25001");
                }
                if modes.deferrable.is_some() {
                    check_deferrable(settings, diagnostic)?;
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
                if let Some(deferrable) = modes.deferrable {
                    settings.current.transaction_deferrable = deferrable;
                }
            }
            "SET"
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
                (
                    Show::DefaultTransactionDeferrable,
                    modes.deferrable.map(|on| if on { "on" } else { "off" }.into()),
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
            "SET"
        }
    };
    complete(actual, &empty(), out)?;
    Ok(actual)
}
fn import_snapshot(
    id: &str,
    session: &Session,
    implicit: bool,
    settings: &crate::pg_settings::SessionSettings,
    diagnostic: &mut Diagnostic,
    out: &mut Vec<u8>,
) -> R<&'static str> {
    use crate::pg_transaction::IsolationLevel::{RepeatableRead, Serializable};

    diagnostic.wire_exact = true;
    let in_block = session.in_transaction() || implicit;
    if !in_block {
        warning(
            "SET TRANSACTION can only be used in transaction blocks",
            &settings.current,
            out,
        )?;
    }
    if settings.queried || settings.in_subtransaction() {
        diagnostic.message = Some("SET TRANSACTION SNAPSHOT must be called before any query");
        return Err("25001");
    }
    let isolation = if session.in_transaction() {
        settings.current.transaction_isolation
    } else {
        settings.current.default_isolation
    };
    if !matches!(isolation, RepeatableRead | Serializable) {
        diagnostic.message = Some("a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ");
        return Err("0A000");
    }
    if !id.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'A'..=b'F' | b'-')) {
        diagnostic.wire_message = Some(format!("invalid snapshot identifier: \"{id}\""));
        return Err("22023");
    }
    if id.is_empty() {
        diagnostic.message = Some("could not read file \"pg_snapshots/\": Is a directory");
        return Err("XX000");
    }
    diagnostic.wire_message = Some(format!("snapshot \"{id}\" does not exist"));
    Err("42704")
}

pub(super) fn check_deferrable(
    settings: &crate::pg_settings::SessionSettings,
    diagnostic: &mut Diagnostic,
) -> R<()> {
    let message = if settings.in_subtransaction() {
        "SET TRANSACTION [NOT] DEFERRABLE cannot be called within a subtransaction"
    } else if settings.queried {
        "SET TRANSACTION [NOT] DEFERRABLE must be called before any query"
    } else {
        return Ok(());
    };
    diagnostic.message = Some(message);
    Err("25001")
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
        AdminStatement::Membership { grant: true, .. } => "GRANT ROLE",
        AdminStatement::Membership { grant: false, .. } => "REVOKE ROLE",
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
        "SELECT FOR UPDATE" => "cannot execute SELECT FOR UPDATE in a read-only transaction",
        "SELECT FOR NO KEY UPDATE" => "cannot execute SELECT FOR NO KEY UPDATE in a read-only transaction",
        "SELECT FOR SHARE" => "cannot execute SELECT FOR SHARE in a read-only transaction",
        "SELECT FOR KEY SHARE" => "cannot execute SELECT FOR KEY SHARE in a read-only transaction",
        "INSERT" => "cannot execute INSERT in a read-only transaction",
        "UPDATE" => "cannot execute UPDATE in a read-only transaction",
        "DELETE" => "cannot execute DELETE in a read-only transaction",
        "MERGE" => "cannot execute MERGE in a read-only transaction",
        "TRUNCATE TABLE" => "cannot execute TRUNCATE in a read-only transaction",
        "CREATE TABLE" => "cannot execute CREATE TABLE in a read-only transaction",
        "CREATE TABLESPACE" => "cannot execute CREATE TABLESPACE in a read-only transaction",
        "ALTER TABLESPACE" => "cannot execute ALTER TABLESPACE in a read-only transaction",
        "DROP TABLESPACE" => "cannot execute DROP TABLESPACE in a read-only transaction",
        "CREATE VIEW" => "cannot execute CREATE VIEW in a read-only transaction",
        "DROP VIEW" => "cannot execute DROP VIEW in a read-only transaction",
        "COMMENT" => "cannot execute COMMENT in a read-only transaction",
        "SECURITY LABEL" => "cannot execute SECURITY LABEL in a read-only transaction",
        "ALTER TABLE" => "cannot execute ALTER TABLE in a read-only transaction",
        "CREATE TYPE" => "cannot execute CREATE TYPE in a read-only transaction",
        "ALTER TYPE" => "cannot execute ALTER TYPE in a read-only transaction",
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
        "CREATE TASK" => "cannot execute CREATE TASK in a read-only transaction",
        "REVOKE TASK" => "cannot execute REVOKE TASK in a read-only transaction",
        _ => return Ok(()),
    });
    diagnostic.wire_exact |= matches!(
        command,
        "CREATE TABLESPACE" | "ALTER TABLESPACE" | "DROP TABLESPACE" | "SECURITY LABEL"
    );
    Err("25006")
}
