//! Caller-supplied time; this module never reads a host clock.
use super::{SqlError, Value};
use crate::CoreError;
use giggles_pg_types::temporal::{Date, SessionZone, Time, TimeTz, Timestamp, TimestampTz};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

/// Observation bits (D7.4): which statement-time sources an execution read.
/// Transaction and statement instants: now, transaction_timestamp,
/// statement_timestamp, current_timestamp.
pub(crate) const OBS_INSTANT: u8 = 1 << 0;
/// The wall clock: clock_timestamp().
pub(crate) const OBS_WALL: u8 = 1 << 1;
/// The session zone: current_date, current_time, localtime, localtimestamp
/// and time comparisons.
pub(crate) const OBS_ZONE: u8 = 1 << 2;
/// Sequence functions: nextval, setval, currval.
pub(crate) const OBS_SEQUENCE: u8 = 1 << 3;
/// Transaction-ID functions.
pub(crate) const OBS_XACT: u8 = 1 << 4;

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
pub(crate) type SequenceCallback =
    Arc<dyn Fn(&str, &str, Option<i64>, bool) -> Result<Value, CoreError> + Send + Sync>;
pub(crate) type XactCallback =
    Arc<dyn Fn(&str, Option<i64>) -> Result<Value, CoreError> + Send + Sync>;
struct PrivilegeExecution {
    callback: crate::PrivilegeCallback,
    provider: crate::storage::PrivilegeProvider,
    metadata: Mutex<(Option<crate::PrivilegeMetadata>, Option<Vec<String>>)>,
}
#[derive(Clone)]
pub(crate) struct ExecutionClock {
    clock: StatementClock,
    error: Arc<Mutex<Option<CoreError>>>,
    sequence: Option<SequenceCallback>,
    xact: Option<XactCallback>,
    privilege: Option<Arc<PrivilegeExecution>>,
    extra_float_digits: i32,
    /// OBS_* bits, shared by every clone made from one execution.
    observed: Arc<AtomicU8>,
}
impl ExecutionClock {
    pub(crate) fn new(clock: &StatementClock) -> Self {
        Self {
            clock: clock.clone(),
            sequence: None,
            xact: None,
            privilege: None,
            extra_float_digits: 1,
            error: Arc::new(Mutex::new(None)),
            observed: Arc::new(AtomicU8::new(0)),
        }
    }
    /// A clock whose transaction and statement instants are both `us`, with
    /// no wall-clock callback, for re-running a materialized preview.
    // Callers arrive with the R3 preview/apply items.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn pinned(us: i64, zone: Option<SessionZone>) -> StatementClock {
        StatementClock {
            unavailable: None,
            transaction_us: us,
            statement_us: us,
            zone,
            wall: None,
        }
    }
    /// The OBS_* bits recorded so far by this execution and its clones.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn observed(&self) -> u8 {
        self.observed.load(Ordering::SeqCst)
    }
    fn observe(&self, bits: u8) {
        self.observed.fetch_or(bits, Ordering::SeqCst);
    }
    pub(crate) fn with_extra_float_digits(&self, value: i32) -> Self {
        let mut context = self.clone();
        context.extra_float_digits = value;
        context
    }
    pub(crate) fn extra_float_digits(&self) -> i32 {
        self.extra_float_digits
    }
    pub(crate) fn with_sequences(&self, callback: SequenceCallback) -> Self {
        let mut context = self.clone();
        context.sequence = Some(callback);
        context
    }
    pub(crate) fn sequence(
        &self,
        name: &str,
        sequence: &str,
        value: Option<i64>,
        called: bool,
    ) -> Result<Value, SqlError> {
        self.observe(OBS_SEQUENCE);
        match (self.sequence.as_ref().ok_or(SqlError::Unsupported)?)(name, sequence, value, called)
        {
            Ok(value) => Ok(value),
            Err(error) => {
                *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                Err(SqlError::Unsupported)
            }
        }
    }
    pub(crate) fn with_xact(&self, callback: XactCallback) -> Self {
        let mut context = self.clone();
        context.xact = Some(callback);
        context
    }
    pub(crate) fn xact(&self, name: &str, arg: Option<i64>) -> Result<Value, SqlError> {
        self.observe(OBS_XACT);
        match (self.xact.as_ref().ok_or(SqlError::Unsupported)?)(name, arg) {
            Ok(value) => Ok(value),
            Err(error) => {
                *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                Err(SqlError::Unsupported)
            }
        }
    }
    pub(crate) fn with_privileges(&self, callback: crate::PrivilegeCallback,
        metadata: crate::storage::PrivilegeProvider) -> Self {
        let mut context = self.clone();
        context.privilege = Some(Arc::new(PrivilegeExecution {callback, provider: metadata, metadata: Mutex::new((None, None))}));
        context
    }
    pub(crate) fn set_privilege_search_path(&self, path: &[String]) {
        if let Some(execution) = &self.privilege {
            execution.metadata.lock().unwrap_or_else(|e| e.into_inner()).1 = Some(path.to_vec());
        }
    }
    pub(crate) fn privilege(&self, name: &str, args: &[Value]) -> Result<Value, SqlError> {
        let execution = self.privilege.as_deref().ok_or(SqlError::Unsupported)?;
        let mut state = execution.metadata.lock().unwrap_or_else(|e| e.into_inner());
        let path = state.1.take();
        let metadata = state.0.get_or_insert_with(|| (execution.provider)());
        if let Some(path) = path { metadata.set_search_path(path); }
        match (execution.callback)(name, args, metadata) {
            Ok(value) => Ok(value.map_or(Value::Null, Value::Bool)),
            Err(error) => {
                *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                Err(SqlError::Unsupported)
            }
        }
    }
    pub(crate) fn zone(&self) -> Option<SessionZone> {
        self.clock.zone
    }
    pub(crate) fn comparison_context(&self) -> Result<(i64, SessionZone), SqlError> {
        self.observe(OBS_ZONE);
        if let Some(error) = &self.clock.unavailable {
            *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error.clone());
            return Err(SqlError::Unsupported);
        }
        Ok((
            self.clock.transaction_us,
            self.clock.zone.ok_or(SqlError::Unsupported)?,
        ))
    }
    pub(crate) fn take_error(&self) -> Option<CoreError> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
    pub(crate) fn eval(&self, name: &str, precision: u32) -> Result<Value, SqlError> {
        let clock = &self.clock;
        self.observe(match name {
            "clock_timestamp" => OBS_WALL,
            "current_date" | "current_time" | "localtime" | "localtimestamp" => OBS_ZONE,
            _ => OBS_INSTANT,
        });
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

impl std::fmt::Debug for ExecutionClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionClock")
            .field("clock", &self.clock)
            .field("sequence", &self.sequence.as_ref().map(|_| "<callback>"))
            .field("xact", &self.xact.as_ref().map(|_| "<callback>"))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock(zone: Option<SessionZone>) -> StatementClock {
        StatementClock {
            unavailable: None,
            transaction_us: 1_000_000,
            statement_us: 2_000_000,
            zone,
            wall: Some(Arc::new(|| Ok(3_000_000))),
        }
    }

    #[test]
    fn now_sets_instant_only() {
        let context = ExecutionClock::new(&clock(None));
        assert_eq!(context.observed(), 0);
        context.eval("now", 6).unwrap();
        assert_eq!(context.observed(), OBS_INSTANT);
        context.eval("statement_timestamp", 6).unwrap();
        assert_eq!(context.observed(), OBS_INSTANT);
    }

    #[test]
    fn clock_timestamp_sets_wall() {
        let context = ExecutionClock::new(&clock(None));
        assert_eq!(
            context.eval("clock_timestamp", 6).unwrap(),
            Value::TimestampTz(TimestampTz::from_microseconds(3_000_000).unwrap())
        );
        assert_eq!(context.observed(), OBS_WALL);
    }

    #[test]
    fn current_date_sets_zone() {
        let context = ExecutionClock::new(&clock(Some(SessionZone::fixed(0).unwrap())));
        context.eval("current_date", 6).unwrap();
        assert_eq!(context.observed(), OBS_ZONE);
        let other = ExecutionClock::new(&clock(Some(SessionZone::fixed(0).unwrap())));
        other.comparison_context().unwrap();
        assert_eq!(other.observed(), OBS_ZONE);
    }

    #[test]
    fn sequence_sets_sequence() {
        let base = ExecutionClock::new(&clock(None));
        let context = base.with_sequences(Arc::new(|_, _, _, _| Ok(Value::Int(7))));
        assert_eq!(
            context.sequence("nextval", "s", None, false).unwrap(),
            Value::Int(7)
        );
        // Clones made from one execution share the bits.
        assert_eq!(base.observed(), OBS_SEQUENCE);
        let xact = base.with_xact(Arc::new(|_, _| Ok(Value::Int(1))));
        xact.xact("txid_current", None).unwrap();
        assert_eq!(base.observed(), OBS_SEQUENCE | OBS_XACT);
    }

    #[test]
    fn pinned_clock_returns_the_pin_for_now_and_statement_timestamp() {
        let pin = 123_456_789;
        let pinned = ExecutionClock::pinned(pin, None);
        assert!(pinned.wall.is_none());
        assert!(pinned.unavailable.is_none());
        let context = ExecutionClock::new(&pinned);
        let expected = Value::TimestampTz(TimestampTz::from_microseconds(pin).unwrap());
        assert_eq!(context.eval("now", 6).unwrap(), expected);
        assert_eq!(context.eval("statement_timestamp", 6).unwrap(), expected);
        assert!(context.eval("clock_timestamp", 6).is_err());
    }
}
