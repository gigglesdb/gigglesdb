//! Lexical admission for overflow handling and simple application reads.
use super::*;

pub(super) fn decline_or_limit(sql: &str) -> R<Option<Plan>> {
    if may_own(sql) {
        Err(Error::Limit)
    } else {
        Ok(None)
    }
}

fn owned_identifier(name: &str) -> bool {
    unqualified_catalog(name)
        || matches!(
            name,
            "pg_catalog"
                | "pg_toast"
                | "information_schema"
                | "user"
                | "current_role"
                | "current_catalog"
                | "current_schema"
                | "current_user"
                | "session_user"
                | "pg_options_to_table"
                | "pg_get_sequence_data"
        )
}

fn owned_routine(name: &str) -> bool {
    matches!(
        name,
        "version"
            | "pg_postmaster_start_time"
            | "pg_backend_pid"
            | "pg_is_in_recovery"
            | "txid_current"
            | "current_database"
            | "current_schemas"
            | "pg_get_indexdef"
            | "pg_get_constraintdef"
            | "pg_get_expr"
            | "pg_get_userbyid"
            | "format_type"
            | "obj_description"
            | "pg_collation_actual_version"
            | "col_description"
            | "shobj_description"
            | "pg_get_viewdef"
            | "has_table_privilege"
            | "has_column_privilege"
    )
}

fn owned(name: &str) -> bool {
    owned_identifier(name) || owned_routine(name)
}

// Scan without constructing sqlparser tokens or an AST. Recognize every explicit
// ownership marker used by Scan, including its closed introspection route.
// Ambiguous/invalid lexical forms retain the catalog guard. False positives
// (e.g. a catalog name used as an alias) also retain it; never route a possible
// catalog query to core merely because its catalog reference occurs late.
pub(super) fn may_own(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut routine = false;
    while i < bytes.len() {
        // A routine name can also name an ordinary application column. The
        // function forms above require a following opening parenthesis.
        let call = routine && bytes[i] == b'(';
        match bytes[i] {
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < bytes.len() && !matches!(bytes[i], b'\r' | b'\n') {
                    i += 1;
                }
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut depth = 1usize;
                while i < bytes.len() && depth > 0 {
                    if bytes.get(i..i + 2) == Some(b"/*") {
                        depth += 1;
                        i += 2;
                    } else if bytes.get(i..i + 2) == Some(b"*/") {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                if depth != 0 {
                    return true;
                }
                continue;
            }
            b' ' | b'\t' | b'\r' | b'\n' | 0x0b | 0x0c => {
                i += 1;
                continue;
            }
            _ if call => return true,
            quote @ (b'\'' | b'"') => {
                i += 1;
                let start = i;
                let mut escaped = false;
                loop {
                    let Some(&b) = bytes.get(i) else { return true };
                    // Backslash interpretation depends on literal prefix and GUCs.
                    if b == b'\\' {
                        return true;
                    }
                    if b == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            escaped = true;
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                if quote == b'"' {
                    if escaped {
                        return true;
                    }
                    if owned_identifier(&sql[start..i]) {
                        return true;
                    }
                    routine = owned_routine(&sql[start..i]);
                } else {
                    routine = false;
                }
                i += 1;
            }
            b'$' => {
                routine = false;
                let start = i;
                i += 1;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                {
                    i += 1;
                }
                if bytes.get(i) == Some(&b'$') {
                    i += 1;
                    let delimiter = &sql[start..i];
                    let Some(end) = sql[i..].find(delimiter) else {
                        return true;
                    };
                    i += end + delimiter.len();
                }
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                i += 1;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
                {
                    i += 1;
                }
                    routine = false;
                    if ascii_fold(&sql[start..i], u8::make_ascii_lowercase, |name| {
                        routine = owned_routine(name);
                        owned_identifier(name)
                    }) {
                    return true;
                }
            }
            b if !b.is_ascii() => return true,
            _ => {
                routine = false;
                i += 1;
            }
        }
    }
    false
}

fn ascii_fold(word: &str, fold: fn(&mut u8), matches: impl FnOnce(&str) -> bool) -> bool {
    let mut buffer = [0; 128];
    let Some(bytes) = buffer.get_mut(..word.len()) else {
        return false;
    };
    bytes.copy_from_slice(word.as_bytes());
    bytes.iter_mut().for_each(fold);
    matches(std::str::from_utf8(bytes).expect("ASCII case folding preserves UTF-8"))
}

/// Lexical, allocation-free routing hint for simple application SELECTs.
///
/// If this returns true, [`plan_gateway`](super::plan_gateway) returns `Ok(None)` or
/// `Err(Error::Syntax)` for the same input. The caller still parses and
/// authorizes the original SQL before executing it.
pub fn application_read_hint(sql: &str) -> bool {
    if sql.len() > 4096 || !sql.is_ascii() {
        return false;
    }
    let sql = sql.trim_start_matches(SQL_SPACE);
    let Some(prefix) = sql.get(..6) else {
        return false;
    };
    let rest = &sql[6..];
    if !prefix.eq_ignore_ascii_case("select")
        || rest.as_bytes().first().is_some_and(|&b| word_byte(b))
    {
        return false;
    }

    // Recognize a simple projection before accepting FROM as a clause.
    // FROM can also occur as an alias, a name component or part of an
    // expression (e.g. IS DISTINCT FROM) in a table-free SELECT.
    let mut tokens = ReadTokens(rest);
    loop {
        let first = tokens.next();
        let mut next = tokens.next();
        if first.is_some_and(ReadToken::projection_name) {
            while next == Some(ReadToken::Dot) {
                let part = tokens.next();
                next = tokens.next();
                if part == Some(ReadToken::Star) {
                    break;
                }
                if !part.is_some_and(ReadToken::projection_name) {
                    return false;
                }
            }
        } else if !matches!(first, Some(ReadToken::Star | ReadToken::Literal)) {
            return false;
        }

        if matches!(next, Some(ReadToken::Word(w)) if w.eq_ignore_ascii_case("as")) {
            if !tokens.next().is_some_and(ReadToken::projection_name) {
                return false;
            }
            next = tokens.next();
        } else if next.is_some_and(ReadToken::projection_name) {
            next = tokens.next();
        }

        match next {
            Some(ReadToken::Comma) => {}
            Some(ReadToken::Word(w)) if w.eq_ignore_ascii_case("from") => break,
            _ => return false,
        }
    }

    matches!(
        tokens.next(),
        Some(ReadToken::Word(_) | ReadToken::QuotedName)
    ) && !tokens.any(|token| token == ReadToken::Invalid)
        && !may_own(sql)
}

const SQL_SPACE: [char; 4] = [' ', '\t', '\r', '\n'];

fn word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn pg_prefixed(word: &str) -> bool {
    word.get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("pg_"))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadToken<'a> {
    Word(&'a str),
    QuotedName,
    Literal,
    Star,
    Dot,
    Comma,
    Other,
    Invalid,
}

impl ReadToken<'_> {
    fn projection_name(self) -> bool {
        match self {
            Self::QuotedName => true,
            // Unquoted keywords can start clauses or special expressions.
            // These common column names act as ordinary identifiers here.
            Self::Word(word) => !ascii_fold(word, u8::make_ascii_uppercase, |upper| {
                !matches!(upper, "ID" | "KEY" | "NAME" | "STATUS" | "VALUE")
                    && sqlparser::keywords::ALL_KEYWORDS
                        .binary_search(&upper)
                        .is_ok()
            }),
            _ => false,
        }
    }
}

struct ReadTokens<'a>(&'a str);

impl<'a> Iterator for ReadTokens<'a> {
    type Item = ReadToken<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut separated = self.0.starts_with(SQL_SPACE);
        self.0 = self.0.trim_start_matches(SQL_SPACE);
        loop {
            // sqlparser can consume adjacent comment openers as part of a
            // custom operator. Only skip whitespace-separated comments.
            if let Some(comment) = self.0.strip_prefix("--") {
                if !separated {
                    return Some(self.invalid());
                }
                let end = comment.find(['\r', '\n']).unwrap_or(comment.len());
                self.0 = &comment[end..];
                if self.0.starts_with('\r') && !self.0.starts_with("\r\n") {
                    return Some(self.invalid());
                }
            } else if let Some(comment) = self.0.strip_prefix("/*") {
                if !separated {
                    return Some(self.invalid());
                }
                let Some((body, tail)) = comment.split_once("*/") else {
                    return Some(self.invalid());
                };
                // Decline nested and overlapping comment delimiters.
                if body.contains("/*") || body.ends_with('/') {
                    return Some(self.invalid());
                }
                self.0 = tail;
            } else {
                break;
            }
            self.0 = self.0.trim_start_matches(SQL_SPACE);
            separated = true;
        }

        let bytes = self.0.as_bytes();
        let first = *bytes.first()?;
        let (len, token) = match first {
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let end = bytes
                    .iter()
                    .position(|&b| !word_byte(b))
                    .unwrap_or(bytes.len());
                let word = &self.0[..end];
                if pg_prefixed(word)
                    || ascii_fold(word, u8::make_ascii_lowercase, |lower| {
                        owned_routine(lower) || matches!(
                            lower,
                            "union"
                                | "intersect"
                                | "except"
                                | "with"
                                | "values"
                                | "select"
                                | "table"
                                | "into"
                                | "lateral"
                                | "collate"
                                | "operator"
                                | "overlaps"
                                // These forms can nest without parentheses.
                                | "not"
                                | "case"
                                | "interval"
                        ) || (matches!(lower, "b" | "e" | "n" | "x")
                            && matches!(bytes.get(end), Some(b'\'' | b'"')))
                            || (lower == "u" && bytes[end..].starts_with(b"&"))
                    })
                {
                    return Some(self.invalid());
                }
                (end, ReadToken::Word(word))
            }
            b'0' if matches!(bytes.get(1), Some(b'x' | b'X' | b'o' | b'O' | b'b' | b'B')) => {
                // Only conversion of nondecimal literals can fail with Limit.
                return Some(self.invalid());
            }
            b'0'..=b'9' => (
                bytes
                    .iter()
                    .position(|b| !b.is_ascii_digit())
                    .unwrap_or(bytes.len()),
                ReadToken::Literal,
            ),
            quote @ (b'\'' | b'"') => {
                let mut end = 1;
                loop {
                    match bytes.get(end) {
                        Some(b'\\' | 0) | None => return Some(self.invalid()),
                        Some(&b) if b == quote => {
                            if bytes.get(end + 1) != Some(&quote) {
                                break;
                            }
                            if quote == b'"' {
                                return Some(self.invalid());
                            }
                            end += 2;
                        }
                        Some(_) => end += 1,
                    }
                }
                let value = &self.0[1..end];
                let token = if quote == b'"' {
                    if value.is_empty()
                        || pg_prefixed(value)
                        || owned_routine(value)
                        || !value.bytes().all(|b| word_byte(b) || b == b' ')
                    {
                        return Some(self.invalid());
                    }
                    ReadToken::QuotedName
                } else {
                    // sqlparser accepts single-quoted identifiers in object names.
                    if owned(value) || pg_prefixed(value) {
                        return Some(self.invalid());
                    }
                    ReadToken::Literal
                };
                (end + 1, token)
            }
            b'*' => (1, ReadToken::Star),
            b'.' => (1, ReadToken::Dot),
            b',' => (1, ReadToken::Comma),
            b'+' | b'-' | b'/' | b'=' | b'<' | b'>' | b'!' | b'%' | b'^' | b'|' | b'&' | b'~' => {
                (1, ReadToken::Other)
            }
            _ => return Some(self.invalid()),
        };
        self.0 = &self.0[len..];
        Some(token)
    }
}

impl ReadTokens<'_> {
    fn invalid(&mut self) -> ReadToken<'static> {
        self.0 = "";
        ReadToken::Invalid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_columns_named_like_catalog_functions_do_not_claim_overflow() {
        let predicates = " AND (1 = 1)".repeat(65);
        for name in ["version", "\"version\"", "format_type"] {
            let query = format!("WITH t AS (SELECT 1 AS {name}) SELECT t.{name} FROM t WHERE true{predicates}");
            assert!(matches!(plan_gateway(&query), Ok(None)), "{query}");
        }
        for expr in ["version()", "\"version\" /* comment */ ()", "current_schema"] {
            let query = format!("WITH t AS (SELECT {expr}) SELECT * FROM t WHERE true{predicates}");
            assert!(plan_gateway(&query).is_err(), "{query}");
        }
    }

    fn assert_route(sql: &str, expected_hint: bool) {
        let hinted = application_read_hint(sql);
        assert_eq!(hinted, expected_hint, "{sql}");
        if hinted {
            let plan = plan_gateway(sql);
            assert!(
                matches!(plan, Ok(None) | Err(Error::Syntax)),
                "{sql}: {plan:?}"
            );
        }
    }

    #[test]
    fn application_read_hint_keeps_plain_selects_in_core() {
        for sql in [
            r#"SELECT * FROM "s".users WHERE id=123"#,
            "select id, name from users where id = 5 order by id limit 1",
            " \t\r\nsElEcT u.id AS wanted, name label FROM public.users u",
            "SELECT u.* FROM users AS u LEFT JOIN orders o ON u.id = o.id",
            r#"SELECT "record"."id", "status" FROM "my schema"."users" AS "record""#,
            "SELECT 'mentions pg_class()' FROM users",
            "SELECT id FROM users WHERE name='it''s (pg_class); [$1]'",
            "SELECT id /* ignore pg_class ( */ FROM users -- pg_catalog\r\nWHERE id = 5",
            "SELECT id FROM /* users */ users -- pg_class",
            "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 1 FOR UPDATE",
            "SELECT id FROM users WHERE price >= 1.25e+4 AND quantity > 1_000",
            "SELECT id FROM users WHERE id = 14e+",
        ] {
            assert_route(sql, true);
        }

        for schema in ["app", r#""app""#, r#""App Store""#, "PUBLIC"] {
            for projection in [
                "*",
                "u.*",
                "u.id AS wanted",
                "id, name",
                "'mentions pg_type'",
            ] {
                for tail in [
                    "",
                    " WHERE u.id=123",
                    " ORDER BY u.id DESC LIMIT 2 OFFSET 1",
                    " FOR UPDATE",
                    " JOIN orders o ON u.id=o.user_id",
                    " WHERE u.name = 'uses information_schema (views)'",
                    " /* pg_class */ WHERE u.id = 123 -- current_user",
                ] {
                    assert_route(
                        &format!("SELECT {projection} FROM {schema}.users u{tail}"),
                        true,
                    );
                }
            }
        }

        for repetitions in [60, 160] {
            let sql = format!(
                "SELECT id FROM users WHERE {}id=1",
                "id=1 OR ".repeat(repetitions)
            );
            assert_route(&sql, true);
        }
        assert_route(
            &format!("SELECT id FROM users WHERE {}id=1", "NOT ".repeat(90)),
            false,
        );

        let prefix = "SELECT * FROM users";
        assert_route(
            &format!("{prefix}{}", " ".repeat(4096 - prefix.len())),
            true,
        );
        assert_route(
            &format!("{prefix}{}", " ".repeat(4097 - prefix.len())),
            false,
        );
    }

    #[test]
    fn application_read_hint_declines_ambiguous_and_catalog_selects() {
        for sql in [
            "",
            "SELECTED * FROM t",
            "SELECT 1",
            "SELECT now()",
            "SELECT 'FROM users'",
            "SELECT 1 AS from ORDER BY 1",
            "SELECT 1 ORDER BY 1 AS from desc",
            "SELECT 1 IS DISTINCT FROM true",
            "SELECT 1 FOR UPDATE OF FROM NOWAIT",
            "SELECT t.from FROM t",
            "SELECT 1 /* FROM users */",
            "SELECT * FROM pg_class",
            "SELECT * FROM pg_catalog.pg_type",
            "SELECT * FROM information_schema.columns",
            r#"SELECT * FROM "pg_class""#,
            "SELECT * FROM pg_stat_activity",
            r#"SELECT * FROM "Pg_Settings""#,
            "SELECT * FROM pg_settings",
            "SELECT * FROM t JOIN 'pg_class' ON true",
            "SELECT * FROM t JOIN 'information_schema'.columns ON true",
            "SELECT version() FROM t",
            "SELECT current_user FROM t",
            "SELECT user FROM t",
            "SELECT current_role FROM t",
            "SELECT current_catalog FROM t",
            "SELECT session_user FROM t",
            "SELECT current_schema FROM t",
            "SELECT has_table_privilege('t', 'SELECT') FROM t",
            "SELECT pg_has_role('owner', 'USAGE') FROM t",
            "SELECT format_type(23, NULL) FROM t",
            "SELECT * FROM t UNION SELECT 1",
            "SELECT * FROM t INTERSECT SELECT 1",
            "SELECT * FROM t EXCEPT SELECT 1",
            "WITH q AS (SELECT 1) SELECT * FROM q",
            "SELECT * FROM t LATERAL VIEW a v",
            "SELECT * FROM (SELECT id FROM t) q",
            "SELECT * FROM t INTO x",
            "SELECT id FROM t WHERE id::text = '1'",
            "SELECT id FROM t WHERE id = $1",
            "SELECT id FROM t WHERE id[1] = 1",
            "SELECT id FROM t; SELECT version()",
            "SELECT id FROM t;",
            "SELECT id FROM caf\u{e9}",
            "SELECT id FROM t WHERE id = 'caf\u{e9}'",
            r"SELECT id FROM t WHERE id = 'back\slash'",
            "SELECT id FROM t WHERE id = E'escaped'",
            "SELECT id FROM t WHERE id = U&'escaped'",
            "SELECT id FROM t WHERE id = $tag$value$tag$",
            "SELECT id FROM t WHERE id = 'unterminated",
            r#"SELECT id FROM "unterminated"#,
            "SELECT id FROM t /* unterminated",
            "/* leading */ SELECT * FROM t",
            "-- leading\nSELECT * FROM t",
            "SELECT * FROM t -- ambiguous\r JOIN pg_class ON true",
            "SELECT * FROM t WHERE id =/* comment */ 1",
            "SELECT * FROM t /* /* nested */ comment */",
            "SELECT * FROM t /* /*/ JOIN pg_class ON true */",
            "SELECT * FROM t WHERE id = 0xFF",
        ] {
            assert_route(sql, false);
        }

        let nondecimal = format!("SELECT * FROM t WHERE id = 0x{}", "F".repeat(14000));
        assert_route(&nondecimal, false);
        assert!(matches!(plan_gateway(&nondecimal), Err(Error::Limit)));
    }

    #[test]
    fn application_read_hint_checks_ownership_markers_in_every_name_position() {
        let names = catalog::known_names()
            .iter()
            .copied()
            .filter(|name| owned(name));
        let extra_names = [
            "pg_settings",
            "pg_stat_activity",
            "pg_publication",
            "pg_publication_namespace",
            "pg_publication_rel",
            "pg_statistic_ext",
            "pg_catalog",
            "information_schema",
            "pg_options_to_table",
            "pg_get_sequence_data",
            "user",
            "current_role",
            "current_catalog",
            "version",
            "pg_postmaster_start_time",
            "pg_backend_pid",
            "pg_is_in_recovery",
            "txid_current",
            "current_database",
            "current_schema",
            "current_user",
            "session_user",
            "current_schemas",
            "pg_get_indexdef",
            "pg_get_constraintdef",
            "pg_get_expr",
            "pg_get_userbyid",
            "format_type",
            "obj_description",
            "pg_collation_actual_version",
            "col_description",
            "shobj_description",
            "pg_get_viewdef",
            "pg_has_role",
            "has_table_privilege",
            "has_column_privilege",
        ];
        for name in names.chain(extra_names) {
            for name in [
                name.to_owned(),
                name.to_ascii_uppercase(),
                format!("\"{name}\""),
            ] {
                for sql in [
                    format!("SELECT id FROM {name}"),
                    format!("SELECT {name} FROM data"),
                    format!("SELECT id AS {name} FROM data"),
                    format!("SELECT id FROM data AS {name}"),
                    format!("SELECT id FROM {name}.data"),
                    format!("SELECT id FROM data JOIN {name} ON true"),
                ] {
                    assert_route(&sql, false);
                }
            }
        }
    }
}
