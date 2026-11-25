//! Server-owned advisory calls. Core evaluates the arguments before any wait.
use super::*;
use crate::advisory::{Action, Mode};
use giggles_core::relational::{AdvisoryCall, AdvisoryResultCast, ColumnDef, EnumParameters};

pub(super) struct Plan {
    pub(super) key: Prepared,
    cast: Option<Box<Prepared>>,
    action: Action,
    two_keys: bool,
    returns_boolean: bool,
    cast_to_name: bool,
}

pub(super) fn parse<'a>(
    sql: &'a str,
    session: &Session,
    diagnostic: &mut Diagnostic,
) -> R<Option<AdvisoryCall<'a>>> {
    if !matches!(session.role, Role::Database { .. }) {
        return Ok(None);
    }
    let _budgets = session.sql.statement_budgets();
    match AdvisoryCall::parse(sql) {
        Ok(call) => Ok(call),
        Err(error) => diagnostic.core(Err(CoreError::Sql(error))),
    }
}

pub(super) fn prepare(
    call: &AdvisoryCall<'_>,
    oids: &[u32],
    session: &mut Session,
    diagnostic: &mut Diagnostic,
) -> R<(Plan, Description, EnumParameters)> {
    diagnostic.phase = "advisory arguments";
    let key = diagnostic.core(session.prepare_advisory_key(call))?;
    if oids.len() > key.parameter_count() {
        return Err("08P01");
    }
    let declared = oids
        .iter()
        .map(|&oid| application_declared_type(oid))
        .collect::<Vec<_>>();
    let key = diagnostic.core(key.with_parameter_types(&declared).map_err(CoreError::Sql))?;
    let key = key.with_declared_enum_parameters(
        diagnostic.core(session.sql.declared_enum_parameters(oids))?,
    );
    let (key_description, enum_parameters) =
        diagnostic.core(session.sql.describe_with_enum_parameters(&key))?;

    let returns_boolean = call.returns_boolean();
    let result_cast = call.result_cast();
    let cast_to_name = matches!(result_cast, Some(AdvisoryResultCast::Name));
    let input_type = if returns_boolean {
        DataType::Boolean
    } else {
        DataType::Text
    };
    let (cast, mut description) = if let Some(AdvisoryResultCast::Sql(cast)) = result_cast {
        diagnostic.phase = "advisory result cast";
        let prepared = diagnostic.core(
            session.prepare_with_parameter_types(&format!("SELECT $1{cast}"), &[Some(input_type)]),
        )?;
        let description = diagnostic.core(session.describe(&prepared))?;
        (Some(Box::new(prepared)), description)
    } else {
        let (data_type, oid) = match (cast_to_name, returns_boolean) {
            (true, _) => (DataType::Text, 19),
            (false, true) => (DataType::Boolean, 16),
            (false, false) => (DataType::Text, 2278),
        };
        (
            None,
            Description {
                columns: vec![ColumnDef {
                    id: 0,
                    name: String::new(),
                    data_type,
                    not_null: false,
                }],
                parameters: vec![],
                type_oids: vec![oid],
                typmods: vec![-1],
            },
        )
    };
    description.columns[0].name = call.column().into();
    description.parameters = key_description.parameters;

    let name = call.name();
    let mode = if name.ends_with("_shared") {
        Mode::Shared
    } else {
        Mode::Exclusive
    };
    let action = match name {
        "pg_advisory_unlock_all" => Action::UnlockAll,
        "pg_advisory_unlock" | "pg_advisory_unlock_shared" => Action::Unlock(mode),
        _ => Action::Acquire {
            mode,
            transaction: name.contains("_xact_"),
            try_only: name.starts_with("pg_try_"),
        },
    };
    Ok((
        Plan {
            key,
            cast,
            action,
            two_keys: call.two_keys(),
            returns_boolean,
            cast_to_name,
        },
        description,
        enum_parameters,
    ))
}

fn scalar(
    statement: &Prepared,
    params: &[Value],
    session: &mut Session,
    diagnostic: &mut Diagnostic,
) -> R<Value> {
    let result = diagnostic.core(session.execute_prepared_unreleased(statement, params))?;
    let mut result = diagnostic.core(result.untasked())?;
    let mut row = result
        .rows
        .pop()
        .filter(|_| result.rows.is_empty())
        .ok_or("XX000")?;
    row.values
        .pop()
        .filter(|_| row.values.is_empty())
        .ok_or("XX000")
}

fn evaluate(
    plan: &Plan,
    params: &[Value],
    description: &Description,
    settings: &crate::pg_settings::Settings,
    session: &mut Session,
    diagnostic: &mut Diagnostic,
    out: &mut Vec<u8>,
) -> R<Released<SqlResult>> {
    if !matches!(session.role, Role::Database { .. }) || session.task().is_some() {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    diagnostic.phase = "advisory arguments";
    let key = scalar(&plan.key, params, session, diagnostic)?;
    // Argument evaluation and its reply have been dropped. Waiting consults
    // only the server's lock table and the connection's cancellation state.
    let mut value = match key {
        Value::Null => Value::Null,
        Value::Int(key) => {
            diagnostic.phase = "advisory lock";
            let success =
                session
                    .service
                    .advisory
                    .call(session, plan.action, (plan.two_keys, key))?;
            if let Action::Unlock(mode) = plan.action
                && !success
                && settings.client_min_messages != "error"
            {
                let mut body = Vec::new();
                for (field, value) in [(b'S', "WARNING"), (b'V', "WARNING"), (b'C', "01000")] {
                    body.push(field);
                    z(value, &mut body);
                }
                body.push(b'M');
                z(
                    &format!("you don't own a lock of type {}", mode.name()),
                    &mut body,
                );
                body.push(0);
                frame(b'N', &body, out)?;
            }
            if plan.returns_boolean && plan.cast_to_name {
                Value::Text(if success { "t" } else { "f" }.into())
            } else if plan.returns_boolean {
                Value::Bool(success)
            } else {
                Value::Text(String::new())
            }
        }
        _ => return Err("XX000"),
    };
    if let Some(cast) = &plan.cast {
        diagnostic.phase = "advisory result cast";
        value = scalar(cast, &[value], session, diagnostic)?;
    }
    Ok(Released::synthetic(SqlResult {
        columns: vec![description.columns[0].name.clone()],
        rows: vec![giggles_core::relational::SqlRow {
            version: 0,
            values: vec![value],
        }],
        affected: 0,
    }))
}

impl State {
    pub(super) fn execute_advisory(
        &mut self,
        portal: &mut Portal,
        session: &mut Session,
        implicit: &mut bool,
        retained: usize,
        out: &mut Vec<u8>,
    ) -> R<()> {
        let super::Plan::Advisory(plan) = portal.s.p.as_ref() else {
            unreachable!()
        };
        if portal.result.is_none() {
            // A standalone key is read without opening an engine transaction.
            // The extended protocol still owns a transaction hold through Sync.
            if !session.in_transaction()
                && matches!(
                    plan.action,
                    Action::Acquire {
                        transaction: true,
                        ..
                    }
                )
            {
                *implicit = true;
            }
            self.settings.queried = true;
            let result = evaluate(
                plan,
                &portal.params,
                &portal.s.d,
                &self.settings.current,
                session,
                &mut self.diagnostic,
                out,
            )?;
            let bytes = serde_json::to_vec(result.get()).map_err(|_| "54000")?.len();
            if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
                return Err("54000");
            }
            portal.result = Some(result);
        }
        let result = portal.result.as_ref().unwrap();
        rows(
            &self.settings.current,
            session.zone().unwrap_or(SessionZone::Fixed(0)),
            result,
            &portal.s.d,
            &portal.formats,
            portal.at,
            1,
            out,
        )?;
        portal.at = 1;
        complete("SELECT", result, out)
    }

    pub(super) fn simple_advisory(
        &mut self,
        call: &AdvisoryCall<'_>,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        self.settings.queried = true;
        let (plan, description, _) = prepare(call, &[], session, &mut self.diagnostic)?;
        let result = evaluate(
            &plan,
            &[],
            &description,
            &self.settings.current,
            session,
            &mut self.diagnostic,
            out,
        )?;
        row_description(&description, &[], out)?;
        rows(
            &self.settings.current,
            session.zone().unwrap_or(SessionZone::Fixed(0)),
            &result,
            &description,
            &[],
            0,
            1,
            out,
        )?;
        complete("SELECT", &result, out)
    }
}
