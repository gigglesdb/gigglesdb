//! Wire integration for standalone configuration SELECTs; no catalog/core changes.
use super::simple::settings_status;
use super::*;

pub(super) fn parse(
    sql: &str,
    session: &Session,
    diagnostic: &mut Diagnostic,
) -> R<Option<crate::pg_settings::Function>> {
    if !matches!(session.role, Role::Database { .. }) {
        return Ok(None);
    }
    diagnostic.settings(crate::pg_settings::parse_function(sql))
}
pub(super) fn description(function: &crate::pg_settings::Function) -> Description {
    if let Some(columns) = function.current_columns() {
        return Description {
            type_oids: vec![], typmods: vec![-1; columns.len()], parameters: vec![],
            columns: columns.iter().map(|call| giggles_core::relational::ColumnDef {
                id: 0, name: call.column().into(), data_type: DataType::Text, not_null: false,
            }).collect(),
        };
    }
    if let crate::pg_settings::Function::Inventory { fields, .. } = function {
        return Description {
            type_oids: vec![],
            typmods: vec![-1; fields.len()],
            parameters: vec![],
            columns: fields.iter().map(|(name, data_type)| giggles_core::relational::ColumnDef {
                id: 0, name: name.clone(), not_null: false, data_type: data_type.clone(),
            }).collect(),
        };
    }
    let boolean = matches!(function, crate::pg_settings::Function::Advisory { name, .. } if name.starts_with("pg_try_") || name == "pg_advisory_unlock");
    Description {
        type_oids: if boolean { vec![16] } else if matches!(function, crate::pg_settings::Function::Advisory { .. }) { vec![2278] } else { vec![] },
        typmods: vec![-1],
        parameters: vec![],
        columns: vec![giggles_core::relational::ColumnDef {
            id: 0,
            name: function.column().into(),
            data_type: if boolean { DataType::Boolean } else { DataType::Text },
            not_null: false,
        }],
    }
}
pub(super) fn evaluate(
    function: &crate::pg_settings::Function,
    settings: &mut crate::pg_settings::SessionSettings,
    session: &Session,
    diagnostic: &mut Diagnostic,
    out: &mut Vec<u8>,
) -> R<SqlResult> {
    if !matches!(session.role, Role::Database { .. }) {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    use crate::pg_settings::{Function, FunctionError};
    if let Function::Inventory { fields, .. } = function {
        let values = crate::pg_settings::inventory_rows(function, settings, settings_read_allowed(session)?)?;
        return Ok(SqlResult {
            columns: fields.iter().map(|(name, _)| name.clone()).collect(), affected: 0,
            rows: values.into_iter().map(|values| giggles_core::relational::SqlRow { version: 0, values }).collect(),
        });
    }
    let before = settings.current.clone();
    if let Some(columns) = function.current_columns() {
        let privileged = settings_read_allowed(session)?;
        let superuser = replication_role_allowed(session)?;
        let rows = if function.has_row(privileged) {
            let mut values = Vec::with_capacity(columns.len());
            for call in &columns {
                if let Function::Set { name, value, .. } = call {
                    use crate::pg_settings::database;
                    if let Ok(name) = database::lookup_native(name) {
                        if name == crate::pg_settings::Show::TransactionDeferrable {
                            super::transaction::check_deferrable(settings, diagnostic)?;
                        }
                        diagnostic.presentation_setting(name, value, &settings.current)?;
                    }
                    if name.eq_ignore_ascii_case("session_replication_role") {
                        diagnostic.replication_role_authorize(session)?;
                        diagnostic.replication_role_value(value)?;
                    }
                    if name.eq_ignore_ascii_case("gigglesdb.max_value_bytes")
                        && !value.parse::<usize>().is_ok_and(|n| (65536..=1073741819).contains(&n)) {
                        diagnostic.wire_message = Some(format!("{value} is outside the valid range for parameter \"gigglesdb.max_value_bytes\" (65536 .. 1073741819)"));
                        return Err("22023");
                    }
                    if name.eq_ignore_ascii_case("gigglesdb.max_statement_tokens")
                        && !value.parse::<i64>().is_ok_and(|n| (0..=i64::from(i32::MAX)).contains(&n)) {
                        diagnostic.wire_message = Some(format!("{value} is outside the valid range for parameter \"gigglesdb.max_statement_tokens\" (0 .. 2147483647)"));
                        return Err("22023");
                    }
                }
                let value = call
                    .evaluate_with_permissions(settings, superuser, privileged, session.database.as_deref())
                    .map_err(|error| match error {
                        FunctionError::Native(code) => diagnostic.settings::<()>(Err(code)).unwrap_err(),
                        FunctionError::Registry(error) => diagnostic.registry_error(error),
                    })?;
                values.push(value.map(Value::Text).unwrap_or(Value::Null));
            }
            vec![giggles_core::relational::SqlRow { values, version: 0 }]
        } else {
            Vec::new()
        };
        settings_status(&before, settings, session, out)?;
        return Ok(SqlResult {
            columns: columns.iter().map(|call| call.column().into()).collect(),
            rows, affected: 0,
        });
    }
    let rows = {
        let value = if let crate::pg_settings::Function::Advisory { name, key, .. } = function {
            let value = session.service.advisory.call(session, name, *key)?;
            if name == "pg_advisory_unlock" && !value && settings.current.client_min_messages != "error" {
                let mut body = Vec::new();
                for (field, value) in [(b'S', "WARNING"), (b'V', "WARNING"), (b'C', "01000"), (b'M', "you don't own a lock of type ExclusiveLock")] { body.push(field); z(value, &mut body); }
                body.push(0); frame(b'N', &body, out)?;
            }
            if name.starts_with("pg_try_") || name == "pg_advisory_unlock" { Value::Bool(value) } else { Value::Text(String::new()) }
        } else { unreachable!("standalone advisory function") };
        vec![giggles_core::relational::SqlRow {
            values: vec![value],
            version: 0,
        }]
    };
    settings_status(&before, settings, session, out)?;
    Ok(SqlResult {
        columns: vec![function.column().into()],
        rows,
        affected: 0,
    })
}
pub(super) fn execute(
    portal: &mut Portal,
    session: &mut Session,
    implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings,
    diagnostic: &mut Diagnostic,
    retained: usize,
    out: &mut Vec<u8>,
) -> R<()> {
    let Plan::SettingsFunction(function) = &*portal.s.p else {
        unreachable!()
    };
    if !matches!(session.role, Role::Database { .. }) {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    if portal.result.is_none() {
        if let crate::pg_settings::Function::Advisory { name, .. } = function {
            // A table-free lock needs no engine snapshot. Its extended implicit
            // transaction still owns xact locks through Sync.
            if name.contains("xact") && !session.in_transaction() { *implicit = true; }
        } else { begin_implicit(session, implicit, settings)?; }
        settings.queried = true;
        let result = evaluate(function, settings, session, diagnostic, out)?;
        let bytes = serde_json::to_vec(&result).map_err(|_| "54000")?.len();
        if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
            return Err("54000");
        }
        // A settings function's value, built here: never core rows.
        portal.result = Some(Released::synthetic(result));
    }
    let result = portal.result.as_ref().unwrap();
    let total = result.get().rows.len();
    rows(
        &settings.current,
        session.zone().unwrap_or(SessionZone::Fixed(0)),
        result,
        &portal.s.d,
        &portal.formats,
        portal.at,
        total,
        out,
    )?;
    portal.at = total;
    complete("SELECT", result, out)
}
impl State {
    pub(super) fn simple_settings_function(
        &mut self,
        function: &crate::pg_settings::Function,
        session: &mut Session,
        implicit: &mut bool,
        out: &mut Vec<u8>,
    ) -> R<()> {
        let own_transaction = !session.in_transaction();
        if !matches!(function, crate::pg_settings::Function::Advisory { .. }) { begin_implicit(session, implicit, &mut self.settings)?; }
        self.settings.queried = true;
        let result = (|| {
            // A settings function's value, built here: never core rows.
            let result = Released::synthetic(evaluate(
                function,
                &mut self.settings,
                session,
                &mut self.diagnostic,
                out,
            )?);
            let description = description(function);
            row_description(&description, &[], out)?;
            rows(
                &self.settings.current,
                session.zone().unwrap_or(SessionZone::Fixed(0)),
                &result,
                &description,
                &[],
                0,
                result.get().rows.len(),
                out,
            )?;
            complete("SELECT", &result, out)
        })();
        if own_transaction {
            let ending = if result.is_ok() {
                execute_implicit_commit(session, implicit, &mut self.settings, &mut self.diagnostic, out)
            } else {
                execute_control("ROLLBACK", session, implicit, &mut self.settings, &mut self.diagnostic, out)
            };
            if result.is_ok() {
                ending?;
            }
        }
        result
    }
}
