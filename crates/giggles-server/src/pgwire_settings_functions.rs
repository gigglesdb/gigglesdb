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
    if let crate::pg_settings::Function::Inventory(result) = function {
        return Description {
            type_oids: result.fields.iter().map(|f| f.data_type.oid()).collect(),
            typmods: vec![-1; result.fields.len()], parameters: vec![],
            columns: result.fields.iter().map(|f| giggles_core::relational::ColumnDef {
                id: 0, name: f.name.clone(), not_null: false,
                data_type: match f.data_type { giggles_pg_catalog::PgType::Numeric => DataType::Numeric, giggles_pg_catalog::PgType::Bool => DataType::Boolean, _ => DataType::Text },
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
    if let crate::pg_settings::Function::Inventory(result) = function {
        return Ok(SqlResult {
            columns: result.fields.iter().map(|f| f.name.clone()).collect(), affected: 0,
            rows: result.rows.iter().map(|r| giggles_core::relational::SqlRow { version: 0, values: r.iter().map(|v| match v {
                giggles_pg_catalog::Value::Text(s) => Value::Text(s.clone()), giggles_pg_catalog::Value::Numeric(n) => Value::Numeric(n.clone()),
                giggles_pg_catalog::Value::Bool(b) => Value::Bool(*b), _ => Value::Null,
            }).collect() }).collect(),
        });
    }
    if function.has_row(settings)
        && let crate::pg_settings::Function::Set { name, value, .. } = function
        && name.eq_ignore_ascii_case("session_replication_role") {
        diagnostic.replication_role_authorize(session)?;
        diagnostic.replication_role_value(value)?;
    }
    if function.has_row(settings)
        && let crate::pg_settings::Function::Set { name, value, .. } = function
        && name.eq_ignore_ascii_case("gigglesdb.max_value_bytes")
        && !value.parse::<usize>().is_ok_and(|n| (65536..=1073741819).contains(&n)) {
        diagnostic.wire_message = Some(format!("{value} is outside the valid range for parameter \"gigglesdb.max_value_bytes\" (65536 .. 1073741819)"));
        return Err("22023");
    }
    if function.has_row(settings)
        && let crate::pg_settings::Function::Set { name, value, .. } = function
        && name.eq_ignore_ascii_case("gigglesdb.max_statement_tokens")
        && !value.parse::<i64>().is_ok_and(|n| (0..=i64::from(i32::MAX)).contains(&n)) {
        diagnostic.wire_message = Some(format!("{value} is outside the valid range for parameter \"gigglesdb.max_statement_tokens\" (0 .. 2147483647)"));
        return Err("22023");
    }
    if let Some(columns) = function.current_columns() {
        let values = columns.iter().map(|call| diagnostic.settings(call.evaluate(settings)).map(|v| v.map(Value::Text).unwrap_or(Value::Null))).collect::<R<Vec<_>>>()?;
        return Ok(SqlResult {
            columns: columns.iter().map(|c| c.column().into()).collect(),
            rows: if function.has_row(settings) { vec![giggles_core::relational::SqlRow { values, version: 0 }] } else { vec![] }, affected: 0,
        });
    }
    let before = settings.current.clone();
    let rows = if function.has_row(settings) {
        let value = if let crate::pg_settings::Function::Advisory { name, key, .. } = function {
            let value = session.service.advisory.call(session, name, *key)?;
            if name == "pg_advisory_unlock" && !value && settings.current.client_min_messages != "error" {
                let mut body = Vec::new();
                for (field, value) in [(b'S', "WARNING"), (b'V', "WARNING"), (b'C', "01000"), (b'M', "you don't own a lock of type ExclusiveLock")] { body.push(field); z(value, &mut body); }
                body.push(0); frame(b'N', &body, out)?;
            }
            if name.starts_with("pg_try_") || name == "pg_advisory_unlock" { Value::Bool(value) } else { Value::Text(String::new()) }
        } else { diagnostic.settings(function.evaluate(settings))?.map(Value::Text).unwrap_or(Value::Null) };
        vec![giggles_core::relational::SqlRow {
            values: vec![value],
            version: 0,
        }]
    } else {
        vec![]
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
    let Plan::SettingsFunction(function) = &portal.s.p else {
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
                execute_implicit_commit(session, implicit, &mut self.settings, out)
            } else {
                execute_control("ROLLBACK", session, implicit, &mut self.settings, out)
            };
            if result.is_ok() {
                ending?;
            }
        }
        result
    }
}
