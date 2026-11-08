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
    Description {
        typmods: vec![-1],
        parameters: vec![],
        columns: vec![giggles_core::relational::ColumnDef {
            id: 0,
            name: function.column().into(),
            data_type: DataType::Text,
            not_null: false,
        }],
    }
}
fn evaluate(
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
    let before = settings.current.clone();
    let value = diagnostic.settings(function.evaluate(settings))?;
    settings_status(&before, settings, session, out)?;
    Ok(SqlResult {
        columns: vec![function.column().into()],
        rows: vec![giggles_core::relational::SqlRow {
            values: vec![value.map(Value::Text).unwrap_or(Value::Null)],
            version: 0,
        }],
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
        begin_implicit(session, implicit, settings)?;
        settings.queried = true;
        let result = evaluate(function, settings, session, diagnostic, out)?;
        let bytes = serde_json::to_vec(&result).map_err(|_| "54000")?.len();
        if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
            return Err("54000");
        }
        portal.result = Some(result);
    }
    let result = portal.result.as_ref().unwrap();
    rows(
        settings.current.extra_float_digits,
        session.zone().unwrap_or(SessionZone::Fixed(0)),
        result,
        &portal.s.d,
        &portal.formats,
        portal.at,
        result.rows.len(),
        out,
    )?;
    portal.at = result.rows.len();
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
        begin_implicit(session, implicit, &mut self.settings)?;
        self.settings.queried = true;
        let result = (|| {
            let result = evaluate(
                function,
                &mut self.settings,
                session,
                &mut self.diagnostic,
                out,
            )?;
            let description = description(function);
            row_description(&description, &[], out)?;
            rows(
                self.settings.current.extra_float_digits,
                session.zone().unwrap_or(SessionZone::Fixed(0)),
                &result,
                &description,
                &[],
                0,
                result.rows.len(),
                out,
            )?;
            complete("SELECT", &result, out)
        })();
        if own_transaction {
            let ending = execute_control(
                if result.is_ok() { "COMMIT" } else { "ROLLBACK" },
                session,
                implicit,
                &mut self.settings,
                out,
            );
            if result.is_ok() {
                ending?;
            }
        }
        result
    }
}
