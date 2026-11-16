//! Conservative QUERY_EVENT classification. Maintenance and account statements
//! do not change selected schemas. Unrecognized selected-name mentions fail closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DdlImpact {
    Selected {
        tables: Vec<(String, String)>,
        statement: String,
    },
    Unselected,
    Unknown {
        reason: String,
    },
}
#[derive(Debug)]
struct Token {
    text: String,
    identifier: bool,
    quoted: bool,
}
fn tokens(sql: &str) -> Option<Vec<Token>> {
    let mut chars = sql.chars().peekable();
    let mut out = Vec::new();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        if c == '#'
            || (c == '-'
                && chars.clone().next() == Some('-')
                && chars.clone().nth(1).is_some_and(char::is_whitespace))
        {
            for c in chars.by_ref() {
                if c == '\n' {
                    break;
                }
            }
            continue;
        }
        if c == '/' && chars.peek() == Some(&'*') {
            chars.next();
            // Executable version comments must not conceal an operation.
            if chars.peek() == Some(&'!') {
                return None;
            }
            loop {
                let c = chars.next()?;
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    break;
                }
            }
            continue;
        }
        if matches!(c, '`' | '\'' | '"') {
            let mut text = String::new();
            loop {
                let n = chars.next()?;
                if n == c {
                    if chars.peek() != Some(&c) {
                        break;
                    }
                    chars.next();
                } else if n == '\\' && c != '`' {
                    text.push(chars.next()?);
                    continue;
                }
                text.push(n);
            }
            out.push(Token {
                text: if c == '`' { text } else { String::new() },
                identifier: c == '`',
                quoted: true,
            });
        } else if c.is_alphanumeric() || matches!(c, '_' | '$') {
            let mut text = c.to_string();
            while chars
                .peek()
                .is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '$'))
            {
                text.push(chars.next()?);
            }
            out.push(Token {
                text,
                identifier: true,
                quoted: false,
            });
        } else {
            out.push(Token {
                text: c.to_string(),
                identifier: false,
                quoted: false,
            });
        }
    }
    Some(out)
}
struct Parser {
    tokens: Vec<Token>,
    at: usize,
}
impl Parser {
    fn eat(&mut self, word: &str) -> bool {
        if self
            .tokens
            .get(self.at)
            .is_some_and(|t| !t.quoted && t.text.eq_ignore_ascii_case(word))
        {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn name(&mut self, default: &str) -> Option<(String, String)> {
        let first = self.ident()?;
        if self.eat(".") {
            Some((first, self.ident()?))
        } else {
            Some((default.into(), first))
        }
    }
    fn ident(&mut self) -> Option<String> {
        let t = self.tokens.get(self.at)?;
        if !t.identifier {
            return None;
        }
        self.at += 1;
        Some(t.text.clone())
    }
    fn if_exists(&mut self, create: bool) -> Option<()> {
        if self.eat("IF") {
            if create && !self.eat("NOT") {
                return None;
            }
            if !self.eat("EXISTS") {
                return None;
            }
        }
        Some(())
    }
    fn targets(&mut self, default: &str) -> Option<Vec<(String, String)>> {
        let mut names = Vec::new();
        if self.eat("OPTIMIZE")
            || self.eat("ANALYZE")
            || self.eat("REPAIR")
            || self.eat("GRANT")
            || self.eat("REVOKE")
        {
            return Some(names);
        }
        if self.eat("TRUNCATE") {
            self.eat("TABLE");
            names.push(self.name(default)?);
        } else if self.eat("RENAME") {
            if !self.eat("TABLE") {
                return None;
            }
            loop {
                names.push(self.name(default)?);
                if !self.eat("TO") {
                    return None;
                }
                names.push(self.name(default)?);
                if !self.eat(",") {
                    break;
                }
            }
        } else {
            let create = self.eat("CREATE");
            let drop = !create && self.eat("DROP");
            let alter = !create && !drop && self.eat("ALTER");
            if !(create || drop || alter) {
                return None;
            }
            if self.eat("USER") || self.eat("ROLE") {
                return Some(names);
            }
            if self.eat("DATABASE") || self.eat("SCHEMA") {
                self.if_exists(create)?;
                names.push((self.ident()?, String::new()));
            } else {
                self.eat("TEMPORARY");
                if self.eat("TABLE") {
                    self.if_exists(create)?;
                    names.push(self.name(default)?);
                    if drop {
                        while self.eat(",") {
                            names.push(self.name(default)?);
                        }
                    }
                    if create && self.eat("LIKE") {
                        names.push(self.name(default)?);
                    }
                    if alter && self.eat("RENAME") {
                        self.eat("TO");
                        self.eat("AS");
                        names.push(self.name(default)?);
                    }
                } else {
                    self.eat("UNIQUE");
                    self.eat("FULLTEXT");
                    self.eat("SPATIAL");
                    if !(self.eat("INDEX") || self.eat("TRIGGER")) {
                        return None;
                    }
                    while self.at < self.tokens.len() && !self.eat("ON") {
                        self.at += 1;
                    }
                    names.push(self.name(default)?);
                }
            }
        }
        Some(names)
    }
}
/// Mode 0 compares schema/table names exactly; modes 1/2 compare lowercased names.
/// The returned table names always use the original selected descriptor spelling.
pub fn classify(
    default_schema: &str,
    sql: &str,
    selected: &[(String, String)],
    lower_case_table_names: u8,
) -> DdlImpact {
    let normalize = |s: &str| {
        if lower_case_table_names == 0 {
            s.to_owned()
        } else {
            s.to_lowercase()
        }
    };
    let unknown = || {
        let mentions: Vec<_> = selected
            .iter()
            .filter(|(_, t)| normalize(sql).contains(&normalize(t)))
            .collect();
        if mentions.is_empty() {
            DdlImpact::Unselected
        } else {
            DdlImpact::Unknown {
                reason: format!("unparsed statement {sql:?}; selected tables {mentions:?}"),
            }
        }
    };
    let Some(tokens) = tokens(sql) else {
        return unknown();
    };
    let Some(targets) = (Parser { tokens, at: 0 }).targets(default_schema) else {
        return unknown();
    };
    let tables: Vec<_> = selected
        .iter()
        .filter(|(s, t)| {
            targets.iter().any(|(schema, table)| {
                normalize(schema) != normalize("_skedaddle")
                    && normalize(s) == normalize(schema)
                    && (table.is_empty() || normalize(t) == normalize(table))
            })
        })
        .cloned()
        .collect();
    // An unqualified name with no default database cannot be resolved safely.
    if targets.iter().any(|(s, t)| {
        s.is_empty()
            && selected
                .iter()
                .any(|(_, name)| normalize(name) == normalize(t))
    }) {
        return unknown();
    }
    if tables.is_empty() {
        DdlImpact::Unselected
    } else {
        DdlImpact::Selected {
            tables,
            statement: sql.into(),
        }
    }
}
#[cfg(test)]
mod tests;
