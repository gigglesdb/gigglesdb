//! Bounded presentation-only JDBC initialization commands, not a general SET engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Setting {
    ApplicationName(String),
    ExtraFloatDigits(i32),
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Settings {
    pub application_name: String,
    pub extra_float_digits: i32,
}
impl Settings {
    pub fn apply(&mut self, setting: &Setting) {
        match setting {
            Setting::ApplicationName(value) => self.application_name.clone_from(value),
            Setting::ExtraFloatDigits(value) => self.extra_float_digits = *value,
        }
    }
}
#[derive(Default)]
pub(crate) struct SessionSettings {
    pub current: Settings,
    saved: Option<Settings>,
}
impl SessionSettings {
    pub fn transaction(&mut self, was: bool, now: bool, command: &str) {
        if !was && now {
            self.saved = Some(self.current.clone());
        }
        if was && !now {
            if command == "ROLLBACK" {
                if let Some(saved) = self.saved.take() {
                    self.current = saved;
                }
            } else {
                self.saved = None;
            }
        }
    }
}
type Result<T> = std::result::Result<T, &'static str>;
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
        assert_eq!(state.current.extra_float_digits, 0);
    }
}
