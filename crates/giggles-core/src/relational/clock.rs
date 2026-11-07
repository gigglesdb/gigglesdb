//! Caller-supplied time; this module never reads a host clock.
use super::{SqlError, Value};
use crate::CoreError;
use giggles_pg_types::temporal::{Date, SessionZone, Time, TimeTz, Timestamp, TimestampTz};
use std::sync::{Arc, Mutex};

/// Explicit time inputs for one SQL execution.
///
/// All instants use UTC microseconds since PostgreSQL's epoch,
/// 2000-01-01 00:00:00 UTC. Absolute timestamp functions need no zone;
/// local timestamp, date and time functions return Unsupported without one.
#[derive(Clone)]
pub struct StatementClock {
    /// Failure returned only when a SQL time function is evaluated.
    /// transaction_us and statement_us are ignored while this is Some.
    pub unavailable: Option<CoreError>,
    /// Transaction start instant, pinned across an explicit transaction.
    pub transaction_us: i64,
    /// Statement start instant; must be at least transaction_us.
    pub statement_us: i64,
    /// Explicit session zone for local calendar and time-of-day values.
    pub zone: Option<SessionZone>,
    /// Called once for each evaluated clock_timestamp() occurrence and row.
    /// Missing callbacks return Unsupported; callback errors propagate unchanged.
    pub wall: Option<Arc<dyn Fn() -> Result<i64, CoreError> + Send + Sync>>,
}
impl std::fmt::Debug for StatementClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementClock")
            .field("unavailable", &self.unavailable)
            .field("transaction_us", &self.transaction_us)
            .field("statement_us", &self.statement_us)
            .field("zone", &self.zone)
            .field("wall", &self.wall.as_ref().map(|_| "<callback>"))
            .finish()
    }
}

/// Shared only by bound expressions from one execution. Prepared syntax never
/// contains this context. The error slot bridges the SQL-only scalar evaluator
/// without translating the caller's CoreError.
#[derive(Clone, Debug)]
pub(crate) struct ExecutionClock {
    clock: StatementClock,
    error: Arc<Mutex<Option<CoreError>>>,
}
impl ExecutionClock {
    pub(crate) fn new(clock: &StatementClock) -> Self {
        Self {
            clock: clock.clone(),
            error: Arc::new(Mutex::new(None)),
        }
    }
    pub(crate) fn take_error(&self) -> Option<CoreError> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
    pub(crate) fn eval(&self, name: &str, precision: u32) -> Result<Value, SqlError> {
        let clock = &self.clock;
        if let Some(error) = &clock.unavailable {
            *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error.clone());
            return Err(SqlError::Unsupported);
        }
        let instant = match name {
            "statement_timestamp" => clock.statement_us,
            "clock_timestamp" => match (clock.wall.as_ref().ok_or(SqlError::Unsupported)?)() {
                Ok(value) => value,
                Err(error) => {
                    *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                    return Err(SqlError::Unsupported);
                }
            },
            _ => clock.transaction_us,
        };
        let pg = |_: giggles_pg_types::Error| SqlError::DatetimeFieldOverflow;
        if matches!(
            name,
            "now"
                | "transaction_timestamp"
                | "statement_timestamp"
                | "clock_timestamp"
                | "current_timestamp"
        ) {
            return TimestampTz::from_microseconds(round(instant, precision)?)
                .map(Value::TimestampTz)
                .map_err(pg);
        }
        let instant = TimestampTz::from_microseconds(instant).map_err(pg)?;
        let local = clock
            .zone
            .ok_or(SqlError::Unsupported)?
            .to_local(instant)
            .map_err(pg)?;
        let micros = local.microseconds();
        match name {
            "localtimestamp" => Timestamp::from_microseconds(round(micros, precision)?)
                .map(Value::Timestamp)
                .map_err(pg),
            "current_date" => Date::from_days(
                i32::try_from(micros.div_euclid(86_400_000_000))
                    .map_err(|_| SqlError::DatetimeFieldOverflow)?,
            )
            .map(Value::Date)
            .map_err(pg),
            "localtime" | "current_time" => {
                let time =
                    Time::from_microseconds(round(micros.rem_euclid(86_400_000_000), precision)?)
                        .map_err(pg)?;
                if name == "localtime" {
                    Ok(Value::Time(time))
                } else {
                    let offset =
                        (i128::from(micros) - i128::from(instant.microseconds())) / 1_000_000;
                    TimeTz::new(
                        time,
                        i32::try_from(offset).map_err(|_| SqlError::DatetimeFieldOverflow)?,
                    )
                    .map(Value::TimeTz)
                    .map_err(pg)
                }
            }
            _ => Err(SqlError::Unsupported),
        }
    }
}
fn round(value: i64, precision: u32) -> Result<i64, SqlError> {
    let scale = 10i128.pow(6 - precision.min(6));
    let value = i128::from(value);
    let rounded = value.signum() * ((value.abs() + scale / 2) / scale) * scale;
    i64::try_from(rounded).map_err(|_| SqlError::DatetimeFieldOverflow)
}
