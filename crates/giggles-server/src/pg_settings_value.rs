//! PostgreSQL settings that do not have a native GigglesDB implementation.
#[path = "pg_settings_registry.rs"]
mod table;
pub(super) use table::ENTRIES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Bool,
    Integer,
    Real,
    String,
    Enum,
}
impl Kind {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Integer => "integer",
            Self::Real => "real",
            Self::String => "string",
            Self::Enum => "enum",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Context {
    User,
    Superuser,
    Sighup,
    Postmaster,
    Internal,
    Backend,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub name: &'static str,
    pub(super) kind: Kind,
    pub unit: Option<&'static str>,
    pub context: Context,
    pub context_name: &'static str,
    pub min: Option<&'static str>,
    pub max: Option<&'static str>,
    pub enum_values: &'static [&'static str],
    pub category: &'static str,
    pub short_desc: &'static str,
    pub extra_desc: Option<&'static str>,
    pub boot: Option<&'static str>,
    pub default: &'static str,
    pub restricted: bool,
}

pub(super) fn find(lower: &str) -> Option<&'static Entry> {
    ENTRIES
        .binary_search_by(|entry| {
            entry
                .name
                .bytes()
                .map(|b| b.to_ascii_lowercase())
                .cmp(lower.bytes())
        })
        .ok()
        .map(|index| &ENTRIES[index])
}

#[derive(Debug)]
pub(crate) struct Error {
    pub code: &'static str,
    pub message: String,
    pub hint: Option<String>,
    pub detail: Option<&'static str>,
}
impl Error {
    pub(super) fn new(code: &'static str, message: String) -> Self {
        Self {
            code,
            message,
            hint: None,
            detail: None,
        }
    }

    pub(super) fn unknown(name: &str) -> Self {
        Self::new(
            "42704",
            format!("unrecognized configuration parameter \"{name}\""),
        )
    }

    fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

impl Entry {
    pub(crate) fn check_visible(&self, privileged: bool) -> Result<(), Error> {
        if !self.restricted || privileged {
            return Ok(());
        }
        let mut error = Error::new(
            "42501",
            format!("permission denied to examine \"{}\"", self.name),
        );
        error.detail = Some(
            "Only roles with privileges of the \"pg_read_all_settings\" role may examine this parameter.",
        );
        Err(error)
    }

    pub(crate) fn check_set(&self, superuser: bool) -> Result<(), Error> {
        let reason = match self.context {
            Context::User => return Ok(()),
            Context::Superuser if superuser => return Ok(()),
            Context::Superuser => {
                return Err(Error::new(
                    "42501",
                    format!("permission denied to set parameter \"{}\"", self.name),
                ));
            }
            Context::Sighup => "cannot be changed now",
            Context::Postmaster => "cannot be changed without restarting the server",
            Context::Internal => "cannot be changed",
            Context::Backend => "cannot be set after connection start",
        };
        Err(Error::new(
            "55P02",
            format!("parameter \"{}\" {reason}", self.name),
        ))
    }

    fn invalid(&self, input: &str) -> Error {
        Error::new(
            "22023",
            format!("invalid value for parameter \"{}\": \"{input}\"", self.name),
        )
    }

    pub(crate) fn parse(&self, input: &str) -> Result<String, Error> {
        self.parse_in_database(input, None)
    }

    pub(super) fn parse_in_database(&self, input: &str, database: Option<&str>) -> Result<String, Error> {
        match self.kind {
            Kind::Bool => parse_bool(input)
                .map(|on| bool_text(on).into())
                .ok_or_else(|| {
                    Error::new(
                        "22023",
                        format!("parameter \"{}\" requires a Boolean value", self.name),
                    )
                }),
            Kind::Enum => {
                let lower = input.to_ascii_lowercase();
                let canonical = match (self.name, lower.as_str()) {
                    ("wal_compression", "on" | "true" | "yes" | "1") => "pglz",
                    (
                        "wal_compression"
                        | "backslash_quote"
                        | "compute_query_id"
                        | "constraint_exclusion"
                        | "debug_parallel_query"
                        | "synchronous_commit",
                        "false" | "no" | "0",
                    ) => "off",
                    (
                        "backslash_quote"
                        | "compute_query_id"
                        | "constraint_exclusion"
                        | "debug_parallel_query"
                        | "synchronous_commit",
                        "true" | "yes" | "1",
                    ) => "on",
                    _ => lower.as_str(),
                };
                self.enum_values
                    .iter()
                    .find(|v| v.eq_ignore_ascii_case(canonical))
                    .map(|value| (*value).into())
                    .ok_or_else(|| {
                        self.invalid(input).hint(format!(
                            "Available values: {}.",
                            self.enum_values.join(", ")
                        ))
                    })
            }
            Kind::Integer | Kind::Real => self.parse_number(input),
            Kind::String if self.name == "temp_tablespaces" => {
                super::database::temp_tablespaces(input)
            }
            Kind::String if self.name == "default_text_search_config" => {
                self.text_search_config(input, database)
            }
            Kind::String => Ok(input.into()),
        }
    }

    fn text_search_config(
        &self,
        input: &str,
        database: Option<&str>,
    ) -> Result<String, Error> {
        let parts = text_search_name(input).ok_or_else(|| self.invalid(input))?;
        let (schema, name) = match parts.as_slice() {
            [name] => ("pg_catalog", name),
            [schema, name] => (schema.as_str(), name),
            [catalog, schema, name] if database == Some(catalog.as_str()) => (schema.as_str(), name),
            [_, _, _] => {
                return Err(Error::new(
                    "0A000",
                    format!("cross-database references are not implemented: {}", parts.join(".")),
                ));
            }
            _ => {
                return Err(Error::new(
                    "42601",
                    format!("improper qualified name (too many dotted names): {}", parts.join(".")),
                ));
            }
        };
        if schema != "pg_catalog" || !giggles_core::relational::text_search_types::is_builtin_config(name) {
            return Err(self.invalid(input));
        }
        Ok(format!("pg_catalog.{name}"))
    }

    fn parse_number(&self, input: &str) -> Result<String, Error> {
        let integer = self.kind == Kind::Integer;
        let (mut value, suffix) = number(input, integer).ok_or_else(|| self.invalid(input))?;
        if !suffix.is_empty() {
            let units = self.units().ok_or_else(|| self.invalid(input))?;
            let index = units
                .table
                .iter()
                .position(|(unit, _)| *unit == suffix)
                .ok_or_else(|| self.invalid(input).hint(units.hint))?;
            value *= units.factor(index);
            // Input with a unit is rounded first to the next smaller unit.
            if index + 1 < units.table.len() {
                let step = units.factor(index + 1);
                value = (value / step).round_ties_even() * step;
            }
        }
        if integer {
            value = value.round_ties_even();
            if !(f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&value) {
                return Err(self.invalid(input).hint("Value exceeds integer range."));
            }
        }
        let min = self.min.expect("numeric GUC minimum");
        let max = self.max.expect("numeric GUC maximum");
        let bound = |text: &str| match text {
            "1.79769e+308" => f64::MAX,
            "-1.79769e+308" => f64::MIN,
            _ => text.parse().expect("numeric GUC bound"),
        };
        if !(bound(min)..=bound(max)).contains(&value) {
            let unit = self.unit.map_or(String::new(), |unit| format!(" {unit}"));
            let value = if integer {
                (value as i32).to_string()
            } else {
                significant(value)
            };
            return Err(Error::new(
                "22023",
                format!(
                    "{value}{unit} is outside the valid range for parameter \"{}\" ({min}{unit} .. {max}{unit})",
                    self.name
                ),
            ));
        }
        Ok(if integer {
            (value as i32).to_string()
        } else {
            value.to_string()
        })
    }

    pub(crate) fn display(&self, base: &str, with_units: bool) -> String {
        match self.name {
            "archive_command" if base.is_empty() => return "(disabled)".into(),
            "data_directory_mode" | "log_file_mode" | "unix_socket_permissions" => {
                return format!("{:04o}", base.parse::<i32>().expect("validated mode"));
            }
            _ => {}
        }
        if matches!(self.kind, Kind::Integer | Kind::Real) {
            let mut value: f64 = base.parse().expect("validated numeric GUC");
            let mut suffix = "";
            if with_units
                && value > 0.0
                && let Some(units) = self.units()
            {
                for (index, (unit, _)) in units.table.iter().enumerate() {
                    let converted = value / units.factor(index);
                    let exact = if self.kind == Kind::Integer {
                        converted.fract() == 0.0
                    } else {
                        (converted.round_ties_even() / converted - 1.0).abs() <= 1e-8
                    };
                    if exact || index + 1 == units.table.len() {
                        value = converted;
                        suffix = unit;
                        break;
                    }
                }
            }
            let value = if self.kind == Kind::Integer {
                (value as i64).to_string()
            } else {
                significant(value)
            };
            return value + suffix;
        }
        base.into()
    }

    fn units(&self) -> Option<Units> {
        let (table, scale, hint) = match self.unit? {
            unit @ ("B" | "kB" | "8kB" | "MB") => (
                MEMORY,
                match unit {
                    "B" => 1.0,
                    "kB" => 1024.0,
                    "8kB" => 8192.0,
                    _ => 1048576.0,
                },
                "Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".",
            ),
            unit @ ("ms" | "s" | "min") => (
                TIME,
                match unit {
                    "ms" => 1000.0,
                    "s" => 1_000_000.0,
                    _ => 60_000_000.0,
                },
                "Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", and \"d\".",
            ),
            unit => unreachable!("unrecognized registry unit {unit}"),
        };
        Some(Units { table, scale, hint })
    }
}

fn text_search_name(input: &str) -> Option<Vec<String>> {
    let mut rest = input.trim();
    let mut parts = Vec::new();
    loop {
        let name = if let Some(quoted) = rest.strip_prefix('"') {
            rest = quoted;
            let mut name = String::new();
            loop {
                let (text, after_quote) = rest.split_once('"')?;
                name.push_str(text);
                if let Some(after_escape) = after_quote.strip_prefix('"') {
                    name.push('"');
                    rest = after_escape;
                } else {
                    rest = after_quote;
                    break;
                }
            }
            name
        } else {
            let end = rest.find(|c: char| c == '.' || c.is_whitespace()).unwrap_or(rest.len());
            let (name, after) = rest.split_at(end);
            rest = after;
            name.to_ascii_lowercase()
        };
        if name.is_empty() {
            return None;
        }
        parts.push(name);
        rest = rest.trim_start();
        if rest.is_empty() {
            return Some(parts);
        }
        rest = rest.strip_prefix('.')?.trim_start();
    }
}

pub(super) fn parse_bool(input: &str) -> Option<bool> {
    let input = input.to_ascii_lowercase();
    if input.is_empty() || input == "o" {
        return None;
    }
    if ["on", "true", "yes", "1"]
        .iter()
        .any(|v| v.starts_with(&input))
    {
        Some(true)
    } else if ["off", "false", "no", "0"]
        .iter()
        .any(|v| v.starts_with(&input))
    {
        Some(false)
    } else {
        None
    }
}

pub(super) fn bool_text(on: bool) -> &'static str {
    if on { "on" } else { "off" }
}

type UnitTable = &'static [(&'static str, f64)];
const MEMORY: UnitTable = &[
    ("TB", 1099511627776.0),
    ("GB", 1073741824.0),
    ("MB", 1048576.0),
    ("kB", 1024.0),
    ("B", 1.0),
];
const TIME: UnitTable = &[
    ("d", 86400000000.0),
    ("h", 3600000000.0),
    ("min", 60000000.0),
    ("s", 1000000.0),
    ("ms", 1000.0),
    ("us", 1.0),
];
struct Units {
    table: UnitTable,
    scale: f64,
    hint: &'static str,
}
impl Units {
    fn factor(&self, index: usize) -> f64 {
        self.table[index].1 / self.scale
    }
}

fn number(input: &str, integer: bool) -> Option<(f64, &str)> {
    let input = input.trim_ascii();
    let sign = usize::from(input.starts_with(['-', '+']));
    let unsigned = &input[sign..];
    if integer {
        let (radix, prefix) = if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
            (16, 2)
        } else if unsigned.starts_with('0') {
            (8, 0)
        } else {
            (10, 0)
        };
        let digits = unsigned[prefix..]
            .chars()
            .take_while(|c| c.is_digit(radix))
            .count();
        let end = sign + prefix + digits;
        if digits > 0
            && !input[end..].starts_with(['.', 'e', 'E'])
            && let Ok(value) = i64::from_str_radix(&unsigned[prefix..prefix + digits], radix)
        {
            let value = if input.starts_with('-') {
                -value
            } else {
                value
            };
            return Some((value as f64, input[end..].trim_ascii_start()));
        }
    } else {
        let lower = unsigned.to_ascii_lowercase();
        if let Some(inf) = ["infinity", "inf"]
            .into_iter()
            .find(|prefix| lower.starts_with(prefix))
        {
            let value = if input.starts_with('-') {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
            return Some((value, unsigned[inf.len()..].trim_ascii_start()));
        }
    }
    let mut end = sign + unsigned.bytes().take_while(u8::is_ascii_digit).count();
    if input[end..].starts_with('.') {
        end += 1;
        end += input[end..].bytes().take_while(u8::is_ascii_digit).count();
    }
    if input[end..].starts_with(['e', 'E']) {
        let exponent = &input[end + 1..];
        let sign = usize::from(exponent.starts_with(['+', '-']));
        let digits = exponent[sign..]
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        if digits > 0 {
            end += 1 + sign + digits;
        }
    }
    let value: f64 = input[..end].parse().ok()?;
    if !value.is_finite() {
        return None;
    }
    Some((value, input[end..].trim_ascii_start()))
}

// PostgreSQL formats real settings and range boundaries with six significant digits.
fn significant(value: f64) -> String {
    if value.is_infinite() {
        return if value.is_sign_negative() { "-Infinity" } else { "Infinity" }.into();
    }
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    if !(-4..6).contains(&exponent) {
        return format!(
            "{}e{exponent:+03}",
            mantissa.trim_end_matches('0').trim_end_matches('.')
        );
    }
    let fixed = format!("{value:.p$}", p = (5 - exponent) as usize);
    if fixed.contains('.') {
        fixed.trim_end_matches('0').trim_end_matches('.').into()
    } else {
        fixed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_values_and_units() {
        for (name, input, base, show) in [
            ("work_mem", "2048", "2048", "2MB"),
            ("work_mem", "1.5MB", "1536", "1536kB"),
            ("work_mem", "16 MB", "16384", "16MB"),
            ("work_mem", "1e3", "1000", "1000kB"),
            ("work_mem", "100.6", "101", "101kB"),
            ("default_statistics_target", "100.5", "100", "100"),
            ("temp_buffers", "1024", "1024", "8MB"),
            ("effective_cache_size", "100", "100", "800kB"),
            ("idle_session_timeout", "0.5ms", "0", "0"),
            ("idle_session_timeout", "61min", "3660000", "61min"),
            ("idle_session_timeout", "90s", "90000", "90s"),
            ("idle_session_timeout", "1.5s", "1500", "1500ms"),
            ("random_page_cost", "1.1", "1.1", "1.1"),
            ("seq_page_cost", "1e-3", "0.001", "0.001"),
            ("vacuum_cost_delay", "1.5ms", "1.5", "1500us"),
            ("vacuum_cost_delay", "1500us", "1.5", "1500us"),
            ("vacuum_cost_delay", "3", "3", "3ms"),
            ("wal_compression", "ON", "pglz", "pglz"),
            ("synchronous_commit", "TRUE", "on", "on"),
            ("constraint_exclusion", "ON", "on", "on"),
            ("enable_seqscan", "Tr", "on", "on"),
            ("enable_seqscan", "of", "off", "off"),
        ] {
            let entry = find(name).unwrap();
            let parsed = entry.parse(input).unwrap();
            assert_eq!(parsed, base, "{name}={input}");
            assert_eq!(entry.display(&parsed, true), show, "{name}={input}");
        }
    }

    #[test]
    fn oracle_diagnostics() {
        let work_mem = find("work_mem").unwrap();
        let error = work_mem.parse("32").unwrap_err();
        assert_eq!(
            error.message,
            "32 kB is outside the valid range for parameter \"work_mem\" (64 kB .. 2147483647 kB)"
        );
        assert!(work_mem.parse("abc").unwrap_err().hint.is_none());
        assert_eq!(
            work_mem.parse("3000000000").unwrap_err().hint.as_deref(),
            Some("Value exceeds integer range.")
        );
        assert_eq!(
            work_mem.parse("16mb").unwrap_err().hint.as_deref(),
            Some("Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".")
        );
        let error = find("random_page_cost").unwrap().parse("-1").unwrap_err();
        assert_eq!(
            error.message,
            "-1 is outside the valid range for parameter \"random_page_cost\" (0 .. 1.79769e+308)"
        );
        let error = find("geqo_selection_bias").unwrap().parse("3").unwrap_err();
        assert_eq!(
            error.message,
            "3 is outside the valid range for parameter \"geqo_selection_bias\" (1.5 .. 2)"
        );
        let error = find("constraint_exclusion")
            .unwrap()
            .parse("bogus")
            .unwrap_err();
        assert_eq!(
            error.hint.as_deref(),
            Some("Available values: partition, on, off.")
        );
        let bool_entry = find("enable_seqscan").unwrap();
        for input in ["o", "2", "maybe", ""] {
            assert_eq!(
                bool_entry.parse(input).unwrap_err().message,
                "parameter \"enable_seqscan\" requires a Boolean value"
            );
        }
    }
}
