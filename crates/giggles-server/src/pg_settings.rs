//! Session presentation settings. `parse`/`show` retain the locked agent grammar.
#[path = "pg_settings_database.rs"]
pub(crate) mod database;
pub(crate) use database::{parse_database, show_database};
#[path = "pg_settings_functions.rs"]
mod functions;
pub(crate) use functions::{Function, parse_function};
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Setting {
    ApplicationName(String),
    ExtraFloatDigits(i32),
    Database {
        name: Show,
        value: Option<String>,
        local: bool,
        reset: bool,
    },
    ResetAll,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Settings {
    pub application_name: String,
    pub extra_float_digits: i32,
    pub timezone: String,
    pub datestyle: String,
    /// Only public contains engine tables; non-public schemas resolve nothing.
    /// Unqualified names still resolve to public even for an empty path, until
    /// schemas exist. This is an intentional restore-compatibility divergence.
    pub search_path: String,
    /// No NOTICE/DEBUG messages are emitted today. WARNING is suppressed at error.
    pub client_min_messages: String,
    /// Presentation only: there are no user-defined functions to validate.
    pub check_function_bodies: String,
    /// Presentation only: the engine does not synchronize sequential scans.
    pub synchronize_seqscans: String,
    /// Presentation only: views and foreign tables are not supported.
    pub restrict_nonsystem_relation_kind: String,
    /// Presentation only: the engine has no XML type.
    pub xmloption: String,
    pub default_read_only: bool,
    pub transaction_read_only: bool,
    pub default_isolation: crate::pg_transaction::IsolationLevel,
    pub transaction_isolation: crate::pg_transaction::IsolationLevel,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            application_name: String::new(),
            extra_float_digits: 1,
            timezone: "UTC".into(),
            datestyle: "ISO".into(),
            search_path: r#""$user", public"#.into(),
            client_min_messages: "notice".into(),
            check_function_bodies: "on".into(),
            synchronize_seqscans: "on".into(),
            restrict_nonsystem_relation_kind: String::new(),
            xmloption: "content".into(),
            default_read_only: false,
            transaction_read_only: false,
            default_isolation: crate::pg_transaction::DEFAULT_ISOLATION,
            transaction_isolation: crate::pg_transaction::DEFAULT_ISOLATION,
        }
    }
}
impl Settings {
    pub fn apply(&mut self, setting: &Setting) {
        match setting {
            Setting::ApplicationName(value) => self.application_name.clone_from(value),
            Setting::ExtraFloatDigits(value) => self.extra_float_digits = *value,
            Setting::Database { .. } | Setting::ResetAll => unreachable!("session assignment"),
        }
    }
}
struct SavepointSettings {
    name: String,
    current: Settings,
    local: std::collections::BTreeMap<Show, String>,
    queried: bool,
}
#[derive(Default)]
pub(crate) struct SessionSettings {
    pub current: Settings,
    saved: Option<Settings>,
    pub defaults: Settings,
    pub queried: bool,
    local: std::collections::BTreeMap<Show, String>,
    savepoints: Vec<SavepointSettings>,
}
impl SessionSettings {
    pub fn savepoint(&mut self, name: String) {
        self.savepoints.push(SavepointSettings {
            name,
            current: self.current.clone(),
            local: self.local.clone(),
            queried: self.queried,
        });
    }
    pub fn release(&mut self, name: &str) {
        if let Some(at) = self.savepoints.iter().rposition(|s| s.name == name) {
            self.savepoints.truncate(at);
        }
    }
    pub fn rollback_to(&mut self, name: &str) {
        if let Some(at) = self.savepoints.iter().rposition(|s| s.name == name) {
            let saved = &self.savepoints[at];
            self.current = saved.current.clone();
            self.local = saved.local.clone();
            self.queried = saved.queried;
            self.savepoints.truncate(at + 1);
        }
    }
    pub fn transaction(&mut self, was: bool, now: bool, command: &str) {
        if !was && now {
            self.saved = Some(self.current.clone());
            self.queried = false;
            self.current.transaction_read_only = self.current.default_read_only;
            self.current.transaction_isolation =
                crate::pg_transaction::effective_isolation(self.current.default_isolation);
            self.local.clear();
        }
        if was && !now {
            self.savepoints.clear();
            if command == "ROLLBACK" {
                if let Some(saved) = self.saved.take() {
                    self.current = saved;
                }
            } else {
                self.saved = None;
                for (name, value) in std::mem::take(&mut self.local) {
                    self.current.assign(name, value);
                }
            }
            self.local.clear();
            self.queried = false;
            self.current.transaction_read_only = self.current.default_read_only;
            self.current.transaction_isolation =
                crate::pg_transaction::effective_isolation(self.current.default_isolation);
        }
    }
    pub fn apply(&mut self, setting: &Setting, in_transaction: bool) -> bool {
        match setting {
            Setting::ResetAll => {
                for name in RESETTABLE {
                    self.local.remove(name);
                    self.current.assign(*name, name.value(&self.defaults).1);
                }
            }
            Setting::Database {
                name, value, local, ..
            } => {
                if *local && !in_transaction {
                    return true;
                }
                if *local {
                    self.local
                        .entry(*name)
                        .or_insert_with(|| name.value(&self.current).1);
                } else {
                    self.local.remove(name);
                }
                let value = value
                    .clone()
                    .unwrap_or_else(|| name.value(&self.defaults).1);
                self.current.assign(*name, value);
            }
            _ => self.current.apply(setting),
        }
        if !in_transaction {
            self.current.transaction_read_only = self.current.default_read_only;
            self.current.transaction_isolation =
                crate::pg_transaction::effective_isolation(self.current.default_isolation);
        }
        false
    }
}
impl Setting {
    pub fn tag(&self) -> &'static str {
        if matches!(self, Self::ResetAll | Self::Database { reset: true, .. }) {
            "RESET"
        } else {
            "SET"
        }
    }
}
impl Settings {
    fn assign(&mut self, name: Show, value: String) {
        match name {
            Show::ApplicationName => self.application_name = value,
            Show::ExtraFloatDigits => {
                self.extra_float_digits = value.parse().expect("validated GUC")
            }
            Show::TimeZone => self.timezone = value,
            Show::DateStyle => self.datestyle = value,
            Show::SearchPath => self.search_path = value,
            Show::ClientMinMessages => self.client_min_messages = value,
            Show::CheckFunctionBodies => self.check_function_bodies = value,
            Show::SynchronizeSeqscans => self.synchronize_seqscans = value,
            Show::RestrictNonsystemRelationKind => self.restrict_nonsystem_relation_kind = value,
            Show::XmlOption => self.xmloption = value,
            Show::DefaultTransactionReadOnly => self.default_read_only = value == "on",
            Show::DefaultTransactionIsolation => {
                self.default_isolation =
                    crate::pg_transaction::IsolationLevel::parse(&value).expect("validated GUC")
            }
            _ => {} // Fixed-value GUCs were validated by the parser.
        }
    }
}
pub(crate) const RESETTABLE: &[Show] = &[
    Show::ApplicationName,
    Show::ExtraFloatDigits,
    Show::TimeZone,
    Show::DateStyle,
    Show::SearchPath,
    Show::ClientEncoding,
    Show::StandardConformingStrings,
    Show::IntervalStyle,
    Show::ByteaOutput,
    Show::StatementTimeout,
    Show::LockTimeout,
    Show::IdleInTransactionSessionTimeout,
    Show::DefaultTransactionReadOnly,
    Show::DefaultTransactionIsolation,
    Show::TransactionTimeout,
    Show::ClientMinMessages,
    Show::RowSecurity,
    Show::CheckFunctionBodies,
    Show::SynchronizeSeqscans,
    Show::RestrictNonsystemRelationKind,
    Show::XmlOption,
    Show::DefaultTablespace,
    Show::DefaultTableAccessMethod,
];
pub(crate) const REPORTABLE: &[Show] = &[
    Show::ApplicationName,
    Show::TimeZone,
    Show::DateStyle,
    Show::ClientEncoding,
    Show::StandardConformingStrings,
    Show::IntervalStyle,
    Show::SearchPath,
];
type Result<T> = std::result::Result<T, &'static str>;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Show {
    ApplicationName,
    ExtraFloatDigits,
    TransactionIsolation,
    DefaultTransactionIsolation,
    TransactionReadOnly,
    DefaultTransactionReadOnly,
    SearchPath,
    ClientEncoding,
    ServerVersion,
    ServerVersionNum,
    StandardConformingStrings,
    DateStyle,
    TimeZone,
    IntervalStyle,
    ByteaOutput,
    StatementTimeout,
    LockTimeout,
    IdleInTransactionSessionTimeout,
    TransactionTimeout,
    ClientMinMessages,
    /// No RLS exists: off is the only behavior; requests for on are rejected.
    RowSecurity,
    CheckFunctionBodies,
    SynchronizeSeqscans,
    RestrictNonsystemRelationKind,
    XmlOption,
    DefaultTablespace,
    DefaultTableAccessMethod,
}
impl Show {
    pub fn value(self, settings: &Settings) -> (&'static str, String) {
        let (name, value) = match self {
            Self::ApplicationName => ("application_name", settings.application_name.clone()),
            Self::ExtraFloatDigits => (
                "extra_float_digits",
                settings.extra_float_digits.to_string(),
            ),
            Self::TransactionIsolation => (
                "transaction_isolation",
                settings.transaction_isolation.name().into(),
            ),
            Self::DefaultTransactionIsolation => (
                "default_transaction_isolation",
                crate::pg_transaction::effective_isolation(settings.default_isolation)
                    .name()
                    .into(),
            ),
            Self::TransactionReadOnly => (
                "transaction_read_only",
                if settings.transaction_read_only {
                    "on"
                } else {
                    "off"
                }
                .into(),
            ),
            Self::DefaultTransactionReadOnly => (
                "default_transaction_read_only",
                if settings.default_read_only {
                    "on"
                } else {
                    "off"
                }
                .into(),
            ),
            Self::SearchPath => ("search_path", settings.search_path.clone()),
            Self::ClientEncoding => ("client_encoding", "UTF8".into()),
            Self::ServerVersion => ("server_version", "18.0".into()),
            Self::ServerVersionNum => ("server_version_num", "180000".into()),
            Self::DateStyle => ("DateStyle", settings.datestyle.clone()),
            Self::TimeZone => ("TimeZone", settings.timezone.clone()),
            Self::IntervalStyle => ("IntervalStyle", "postgres".into()),
            Self::ByteaOutput => ("bytea_output", "hex".into()),
            Self::TransactionTimeout => ("transaction_timeout", "0".into()),
            Self::ClientMinMessages => {
                ("client_min_messages", settings.client_min_messages.clone())
            }
            Self::RowSecurity => ("row_security", "off".into()),
            Self::CheckFunctionBodies => (
                "check_function_bodies",
                settings.check_function_bodies.clone(),
            ),
            Self::SynchronizeSeqscans => (
                "synchronize_seqscans",
                settings.synchronize_seqscans.clone(),
            ),
            Self::RestrictNonsystemRelationKind => (
                "restrict_nonsystem_relation_kind",
                settings.restrict_nonsystem_relation_kind.clone(),
            ),
            Self::XmlOption => ("xmloption", settings.xmloption.clone()),
            Self::DefaultTablespace => ("default_tablespace", "".into()),
            Self::DefaultTableAccessMethod => ("default_table_access_method", "heap".into()),
            Self::StatementTimeout => ("statement_timeout", "0".into()),
            Self::LockTimeout => ("lock_timeout", "0".into()),
            Self::IdleInTransactionSessionTimeout => {
                ("idle_in_transaction_session_timeout", "0".into())
            }
            Self::StandardConformingStrings => ("standard_conforming_strings", "on".into()),
        };
        (name, value)
    }
}
pub(crate) fn show(sql: &str) -> Result<Option<Show>> {
    let mut p = Parser { source: sql, at: 0 };
    if !p.word().eq_ignore_ascii_case("SHOW") {
        return Ok(None);
    }
    let name = p.word().to_ascii_lowercase();
    let show = match name.as_str() {
        "application_name" => Show::ApplicationName,
        "extra_float_digits" => Show::ExtraFloatDigits,
        "transaction_isolation" => Show::TransactionIsolation,
        "default_transaction_isolation" => Show::DefaultTransactionIsolation,
        "transaction" => {
            if !p.word().eq_ignore_ascii_case("ISOLATION")
                || !p.word().eq_ignore_ascii_case("LEVEL")
            {
                return Err("42601");
            }
            Show::TransactionIsolation
        }
        "datestyle" => Show::DateStyle,
        "timezone" => Show::TimeZone,
        "search_path" => Show::SearchPath,
        "client_encoding" => Show::ClientEncoding,
        "server_version" => Show::ServerVersion,
        "server_version_num" => Show::ServerVersionNum,
        "standard_conforming_strings" => Show::StandardConformingStrings,
        _ => return Err("0A000"),
    };
    p.space();
    if p.source.as_bytes().get(p.at) == Some(&b';') {
        p.at += 1;
        p.space();
    }
    if p.at != sql.len() {
        return Err("42601");
    }
    Ok(Some(show))
}
pub(crate) fn empty_sql(sql: &str) -> Result<bool> {
    let bytes = sql.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at].is_ascii_whitespace() || bytes[at] == b';' {
            at += 1;
        } else if bytes[at..].starts_with(b"--") {
            while at < bytes.len() && !matches!(bytes[at], b'\n' | b'\r') {
                at += 1;
            }
        } else if bytes[at..].starts_with(b"/*") {
            let mut depth = 1;
            at += 2;
            while depth != 0 {
                if at == bytes.len() {
                    return Err("42601");
                }
                if bytes[at..].starts_with(b"/*") {
                    depth += 1;
                    at += 2;
                    if depth > 16 {
                        return Err("54000");
                    }
                } else if bytes[at..].starts_with(b"*/") {
                    depth -= 1;
                    at += 2;
                } else {
                    at += 1;
                }
            }
        } else {
            return Ok(false);
        }
    }
    Ok(true)
}
struct Parser<'a> {
    source: &'a str,
    at: usize,
}
impl Parser<'_> {
    fn space(&mut self) {
        while self
            .source
            .as_bytes()
            .get(self.at)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.at += 1;
        }
    }
    fn word(&mut self) -> &str {
        self.space();
        let start = self.at;
        while self
            .source
            .as_bytes()
            .get(self.at)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
        {
            self.at += 1;
        }
        &self.source[start..self.at]
    }
    fn value(&mut self) -> Result<String> {
        self.space();
        let mut value = String::new();
        if self.source.as_bytes().get(self.at) == Some(&b'\'') {
            self.at += 1;
            loop {
                let c = self.source[self.at..].chars().next().ok_or("42601")?;
                self.at += c.len_utf8();
                if c == '\'' {
                    if self.source.as_bytes().get(self.at) != Some(&b'\'') {
                        break;
                    }
                    self.at += 1;
                }
                value.push(c);
                if value.len() > 128 {
                    return Err("54000");
                }
            }
        } else {
            let start = self.at;
            while self
                .source
                .as_bytes()
                .get(self.at)
                .is_some_and(|b| !b.is_ascii_whitespace() && *b != b';')
            {
                self.at += 1;
            }
            if self.at - start > 128 {
                return Err("54000");
            }
            if self.at == start {
                return Err("42601");
            }
            value.push_str(&self.source[start..self.at]);
        }
        Ok(value)
    }
}
pub(crate) fn parse(sql: &str) -> Result<Option<Vec<Setting>>> {
    let mut p = Parser { source: sql, at: 0 };
    if !p.word().eq_ignore_ascii_case("SET") {
        return Ok(None);
    }
    let mut result = Vec::new();
    loop {
        let name = p.word().to_ascii_lowercase();
        if !matches!(name.as_str(), "application_name" | "extra_float_digits") {
            return Err("0A000");
        }
        p.space();
        if p.source.as_bytes().get(p.at) == Some(&b'=') {
            p.at += 1;
        } else if !p.word().eq_ignore_ascii_case("TO") {
            return Err("42601");
        }
        let value = p.value()?;
        let setting = if name == "application_name" {
            Setting::ApplicationName(value)
        } else {
            Setting::ExtraFloatDigits(
                value
                    .parse::<i32>()
                    .ok()
                    .filter(|n| (-15..=3).contains(n))
                    .ok_or("22023")?,
            )
        };
        result.push(setting);
        if result.len() > 16 {
            return Err("54000");
        }
        p.space();
        if p.at == sql.len() {
            break;
        }
        if p.source.as_bytes()[p.at] != b';' {
            return Err("42601");
        }
        p.at += 1;
        p.space();
        if p.at == sql.len() {
            break;
        }
        if !p.word().eq_ignore_ascii_case("SET") {
            return Err("0A000");
        }
    }
    Ok(Some(result))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inspection_parser_rejects_suffixes_and_unknown_settings() {
        for query in [
            "SHOW TRANSACTION ISOLATION LEVEL",
            " show transaction_isolation; ",
        ] {
            assert_eq!(show(query), Ok(Some(Show::TransactionIsolation)));
        }
        for query in [
            "SHOW application_name; SET application_name='x'",
            "SHOW transaction isolation",
            "SHOW application_name FROM app",
        ] {
            assert_eq!(show(query), Err("42601"));
        }
        assert_eq!(show("SHOW secret"), Err("0A000"));
        assert_eq!(show("SELECT id FROM app"), Ok(None));
        assert_eq!(
            Show::TransactionIsolation.value(&Settings::default()).1,
            "serializable"
        );
    }
    #[test]
    fn presentation_inspection_matches_negotiated_values_and_rejects_suffixes() {
        assert_eq!(show("show DateStyle"), Ok(Some(Show::DateStyle)));
        assert_eq!(show("SHOW timezone;"), Ok(Some(Show::TimeZone)));
        let mut settings = Settings::default();
        assert_eq!(
            Show::SearchPath.value(&settings),
            ("search_path", r#""$user", public"#.into())
        );
        assert_eq!(
            Show::DateStyle.value(&settings),
            ("DateStyle", "ISO".into())
        );
        assert_eq!(Show::TimeZone.value(&settings).1, "UTC");
        settings.timezone = "America/Chicago".into();
        assert_eq!(Show::TimeZone.value(&settings).1, "America/Chicago");
        for sql in [
            "SHOW DateStyle; DELETE FROM app",
            "SHOW TimeZone FROM secret",
        ] {
            assert_eq!(show(sql), Err("42601"));
        }
        assert_eq!(parse("SET DateStyle='SQL'"), Err("0A000"));
    }
    #[test]
    fn empty_sql_scanner_never_hides_effectful_suffixes() {
        for sql in [
            "",
            ";;",
            "-- empty\n;",
            "/* outer /* inner */ */; --done",
            "--quoted ';'",
            "/* 🦀 */",
        ] {
            assert_eq!(empty_sql(sql), Ok(true));
        }
        for sql in [
            "; INSERT INTO app VALUES (1)",
            "/* comment */ SELECT id FROM app",
            "'--'",
            "-- comment\nDELETE FROM app",
        ] {
            assert_eq!(empty_sql(sql), Ok(false));
        }
        assert_eq!(empty_sql("/* unfinished"), Err("42601"));
        assert_eq!(empty_sql(&"/*".repeat(17)), Err("54000"));
    }
    #[test]
    fn actual_driver_batch_and_escaped_names() {
        assert_eq!(
            parse("SET extra_float_digits = 3; SET application_name = 'PostgreSQL JDBC Driver'")
                .unwrap(),
            Some(vec![
                Setting::ExtraFloatDigits(3),
                Setting::ApplicationName("PostgreSQL JDBC Driver".into())
            ])
        );
        assert_eq!(
            parse("set application_name to 'a''b; SET role=admin';").unwrap(),
            Some(vec![Setting::ApplicationName("a'b; SET role=admin".into())])
        );
        assert_eq!(parse("SELECT id FROM app WHERE id=1").unwrap(), None);
    }
    #[test]
    fn unsupported_settings_and_batch_escape_fail_without_partial_changes() {
        for sql in [
            "SET role=admin",
            "SET search_path=private",
            "SET LOCAL application_name='a'",
            "SET application_name='x'; DELETE FROM app",
            "SET application_name='x' trailing",
            "SET application_name='unterminated",
            "SET extra_float_digits=4",
            "SET extra_float_digits=-16",
            "SET extra_float_digits=NaN",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
        assert!(parse(&format!("SET application_name='{}'", "x".repeat(129))).is_err());
        assert!(parse(&"SET application_name='a';".repeat(17)).is_err());
    }
    #[test]
    fn presentation_settings_follow_commit_and_rollback() {
        let mut state = SessionSettings::default();
        state
            .current
            .apply(&Setting::ApplicationName("before".into()));
        state.transaction(false, true, "BEGIN");
        state
            .current
            .apply(&Setting::ApplicationName("rolled back".into()));
        state.transaction(true, false, "ROLLBACK");
        assert_eq!(state.current.application_name, "before");
        state.transaction(false, true, "BEGIN");
        state
            .current
            .apply(&Setting::ApplicationName("committed".into()));
        state.transaction(true, false, "COMMIT");
        assert_eq!(state.current.application_name, "committed");
        state.transaction(false, true, "BEGIN");
        state.current.apply(&Setting::ExtraFloatDigits(3));
        state.transaction(true, false, "ROLLBACK");
        assert_eq!(state.current.extra_float_digits, 1);
    }
}
