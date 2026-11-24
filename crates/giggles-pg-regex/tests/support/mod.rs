//! SQL fixtures and row formatting for PostgreSQL's test_regex extension.

use giggles_pg_regex::engine::*;

#[derive(Default)]
pub struct Flags {
    pub cflags: i32,
    pub eflags: i32,
    pub info: i64,
    pub glob: bool,
    pub indices: bool,
    pub partial: bool,
}

impl Flags {
    pub fn parse(options: &str) -> Self {
        let mut flags = Self {
            cflags: REG_ADVANCED,
            ..Self::default()
        };
        for flag in options.chars() {
            match flag {
                '-' | '*' => {}
                '!' => flags.partial = true,
                '0' => flags.indices = true,
                'g' => flags.glob = true,
                'i' => flags.cflags |= REG_ICASE,
                'n' => flags.cflags |= REG_NEWLINE,
                'p' => flags.cflags = (flags.cflags | REG_NLSTOP) & !REG_NLANCH,
                'w' => flags.cflags = (flags.cflags | REG_NLANCH) & !REG_NLSTOP,
                'x' => flags.cflags |= REG_EXPANDED,
                'a' => flags.cflags |= REG_ADVF,
                'b' => flags.cflags &= !REG_ADVANCED,
                'c' => {
                    flags.cflags |= REG_EXPECT;
                    flags.partial = true;
                    flags.indices = true;
                }
                'e' => flags.cflags = (flags.cflags & !REG_ADVANCED) | REG_EXTENDED,
                'q' => flags.cflags = (flags.cflags & !REG_ADVANCED) | REG_QUOTE,
                'o' => flags.cflags |= REG_NOSUB,
                's' => flags.cflags |= REG_BOSONLY,
                '+' => flags.cflags |= REG_FAKE,
                ',' => flags.cflags |= REG_PROGRESS,
                '.' => flags.cflags |= REG_DUMP,
                ':' => flags.eflags |= REG_MTRACE,
                ';' => flags.eflags |= REG_FTRACE,
                '^' => flags.eflags |= REG_NOTBOL,
                '$' => flags.eflags |= REG_NOTEOL,
                't' => flags.cflags |= REG_EXPECT,
                '%' => flags.eflags |= REG_SMALL,
                _ => {
                    let (_, bit, _) = INFO
                        .iter()
                        .find(|&&(option, _, _)| option == flag)
                        .unwrap_or_else(|| panic!("unknown test_regex flag {flag}"));
                    flags.info |= bit;
                }
            }
        }
        flags
    }
}

const INFO: &[(char, i64, &str)] = &[
    ('R', REG_UBACKREF, "REG_UBACKREF"),
    ('H', REG_ULOOKAROUND, "REG_ULOOKAROUND"),
    ('Q', REG_UBOUNDS, "REG_UBOUNDS"),
    ('B', REG_UBRACES, "REG_UBRACES"),
    ('A', REG_UBSALNUM, "REG_UBSALNUM"),
    ('U', REG_UPBOTCH, "REG_UPBOTCH"),
    ('E', REG_UBBS, "REG_UBBS"),
    ('P', REG_UNONPOSIX, "REG_UNONPOSIX"),
    ('S', REG_UUNSPEC, "REG_UUNSPEC"),
    ('M', REG_UUNPORT, "REG_UUNPORT"),
    ('L', REG_ULOCALE, "REG_ULOCALE"),
    ('N', REG_UEMPTYMATCH, "REG_UEMPTYMATCH"),
    ('I', REG_UIMPOSSIBLE, "REG_UIMPOSSIBLE"),
    ('T', REG_USHORTEST, "REG_USHORTEST"),
];

fn sql_array(fields: impl IntoIterator<Item = Option<String>>) -> String {
    let mut result = String::from("{");
    for (index, field) in fields.into_iter().enumerate() {
        if index != 0 {
            result.push(',');
        }
        let Some(field) = field else {
            result.push_str("NULL");
            continue;
        };
        let quote = field.is_empty()
            || field.eq_ignore_ascii_case("null")
            || field.chars().any(|c| {
                matches!(c, '"' | '\\' | '{' | '}' | ',') || matches!(c, ' ' | '\t'..='\r')
            });
        if quote {
            result.push('"');
        }
        for c in field.chars() {
            if c == '"' || c == '\\' {
                result.push('\\');
            }
            result.push(c);
        }
        if quote {
            result.push('"');
        }
    }
    result.push('}');
    result
}

pub fn info_row(regex: &Regex, flags: &Flags) -> String {
    let info = INFO.iter().filter_map(|&(_, bit, name)| {
        match (regex.info() & bit != 0, flags.info & bit != 0) {
            (true, true) => Some(Some(name.to_owned())),
            (true, false) => Some(Some(format!("unexpected {name}!"))),
            (false, true) => Some(Some(format!("missing {name}!"))),
            (false, false) => None,
        }
    });
    sql_array(std::iter::once(Some(regex.nsub().to_string())).chain(info))
}

pub fn match_rows(
    regex: &Regex,
    string: &[Chr],
    flags: &Flags,
) -> Result<Vec<String>, RegErrorCode> {
    let unset = RegMatch { so: -1, eo: -1 };
    let mut details = RmDetail { extend: unset };
    let mut pmatch = vec![unset; regex.nsub() + 1];
    let mut matches = Vec::new();
    let mut start = 0;
    while start <= string.len() {
        pmatch.fill(unset);
        details.extend = unset;
        if !regex.exec(string, start, Some(&mut details), &mut pmatch, flags.eflags)? {
            break;
        }
        matches.push(pmatch.clone());
        if !flags.glob {
            break;
        }
        let m = pmatch[0];
        start = m.eo as usize + usize::from(m.so == m.eo);
    }
    if matches.is_empty() && flags.partial && flags.indices {
        pmatch[0] = details.extend;
        matches.push(pmatch);
    }
    let index = |m: RegMatch| format!("{} {}", m.so, if m.so < 0 { m.eo } else { m.eo - 1 });
    Ok(matches
        .into_iter()
        .map(|matches| {
            let mut fields: Vec<_> = matches
                .into_iter()
                .map(|m| {
                    if flags.indices {
                        Some(index(m))
                    } else if m.so < 0 || m.eo < 0 {
                        None
                    } else {
                        Some(
                            string[m.so as usize..m.eo as usize]
                                .iter()
                                .map(|&c| char::from_u32(c).unwrap())
                                .collect(),
                        )
                    }
                })
                .collect();
            if flags.indices && flags.cflags & REG_EXPECT != 0 {
                fields.push(Some(index(details.extend)));
            }
            sql_array(fields)
        })
        .collect())
}

// The expected files contain psql's aligned output. The fixture reader
// preserves continuation-line padding and removes the trailing '+' marker.
pub fn aligned_field(row: &str, width: usize) -> String {
    use std::fmt::Write as _;

    let mut displayed = String::new();
    let mut column = 0;
    for c in row.chars() {
        match c {
            '\n' => {
                displayed.extend(std::iter::repeat_n(' ', width.saturating_sub(column)));
                displayed.push('\n');
                column = 0;
            }
            '\t' => {
                let spaces = 8 - column % 8;
                displayed.extend(std::iter::repeat_n(' ', spaces));
                column += spaces;
            }
            '\r' => {
                displayed.push_str("\\r");
                column += 2;
            }
            '\x00'..='\x1f' | '\x7f' => {
                write!(&mut displayed, "\\x{:02X}", c as u32).unwrap();
                column += 4;
            }
            _ => {
                displayed.push(c);
                column += 1;
            }
        }
    }
    displayed
}

pub fn wchar(string: &str) -> Vec<Chr> {
    string.chars().map(Chr::from).collect()
}

pub struct Case {
    pub line: usize,
    pub pattern: Vec<Chr>,
    pub input: Vec<Chr>,
    pub flags: Flags,
    pub expected: Expected,
}

pub enum Expected {
    CompileError(String),
    ExecError(String),
    Rows { width: usize, rows: Vec<String> },
}

pub fn cases(output: &str) -> Vec<Case> {
    const PREFIX: &str = "select * from test_regex(";
    let mut results = Vec::new();
    for (offset, _) in output
        .match_indices(PREFIX)
        .filter(|&(i, _)| i == 0 || output.as_bytes()[i - 1] == b'\n')
    {
        let line = output[..offset].bytes().filter(|&b| b == b'\n').count() + 1;
        let mut sql = &output[offset + PREFIX.len()..];
        let pattern = expression(&mut sql);
        eat(&mut sql, ",");
        let input = expression(&mut sql);
        eat(&mut sql, ",");
        let flags = Flags::parse(&expression(&mut sql));
        eat(&mut sql, ");");
        let mut lines = sql.trim_start_matches(['\r', '\n']).lines();
        let first = lines.next().unwrap().trim();
        let expected = if let Some(error) = first.strip_prefix("ERROR:  ") {
            if error.starts_with("invalid regular expression: ") {
                Expected::CompileError(error.to_owned())
            } else {
                Expected::ExecError(error.to_owned())
            }
        } else {
            assert_eq!(first, "test_regex", "on output line {line}");
            let separator = lines.next().unwrap();
            assert!(!separator.is_empty() && separator.bytes().all(|b| b == b'-'));
            let mut rows = Vec::new();
            let mut row = String::new();
            for line in &mut lines {
                if line.starts_with('(') && (line.ends_with("row)") || line.ends_with("rows)")) {
                    let count = line.split_whitespace().next().unwrap()[1..]
                        .parse::<usize>()
                        .unwrap();
                    assert_eq!(rows.len(), count);
                    assert!(row.is_empty());
                    break;
                }
                let field = line.strip_prefix(' ').unwrap().trim_end_matches(' ');
                if let Some(part) = field.strip_suffix('+') {
                    row.push_str(part);
                    row.push('\n');
                } else {
                    row.push_str(field);
                    rows.push(std::mem::take(&mut row));
                }
            }
            assert!(!rows.is_empty(), "no info row at output line {line}");
            Expected::Rows {
                width: separator.len() - 2,
                rows,
            }
        };
        results.push(Case {
            line,
            pattern: wchar(&pattern),
            input: wchar(&input),
            flags,
            expected,
        });
    }
    results
}

fn eat(sql: &mut &str, syntax: &str) {
    *sql = sql
        .trim_start()
        .strip_prefix(syntax)
        .unwrap_or_else(|| panic!("expected SQL {syntax:?} before {:?}", sql.lines().next()));
}

fn expression(sql: &mut &str) -> String {
    *sql = sql.trim_start();
    if let Some(body) = sql.strip_prefix("$$") {
        let (value, remainder) = body.split_once("$$").unwrap();
        *sql = remainder;
        return value.to_owned();
    }
    if sql.starts_with("repeat(") {
        eat(sql, "repeat(");
        let value = expression(sql);
        eat(sql, ",");
        let end = sql.find(')').unwrap();
        let count = sql[..end].trim().parse().unwrap();
        *sql = &sql[end + 1..];
        return value.repeat(count);
    }
    let escaped = sql.starts_with("E'");
    eat(sql, if escaped { "E'" } else { "'" });
    let mut value = Vec::new();
    loop {
        let c = take_char(sql);
        if c == '\'' && !sql.starts_with('\'') {
            break;
        }
        if c == '\'' {
            *sql = &sql[1..];
        }
        if c == '\\' && escaped {
            match take_char(sql) {
                'b' => value.push(8),
                'f' => value.push(12),
                'n' => value.push(10),
                'r' => value.push(13),
                't' => value.push(9),
                escape @ ('u' | 'U') => {
                    let digits = if escape == 'u' { 4 } else { 8 };
                    let chr = take_radix(sql, 16, digits, 0);
                    push_char(&mut value, char::from_u32(chr).unwrap());
                }
                'x' => value.push(take_radix(sql, 16, 2, 0) as u8),
                digit @ '0'..='7' => {
                    value.push(take_radix(sql, 8, 2, digit.to_digit(8).unwrap()) as u8)
                }
                c => push_char(&mut value, c),
            }
        } else {
            push_char(&mut value, c);
        }
    }
    String::from_utf8(value).unwrap()
}

fn take_radix(sql: &mut &str, radix: u32, digits: usize, mut value: u32) -> u32 {
    for _ in 0..digits {
        let Some(digit) = sql.chars().next().and_then(|c| c.to_digit(radix)) else {
            break;
        };
        *sql = &sql[1..];
        value = value * radix + digit;
    }
    value
}

fn take_char(sql: &mut &str) -> char {
    let c = sql.chars().next().expect("unterminated SQL literal");
    *sql = &sql[c.len_utf8()..];
    c
}

fn push_char(value: &mut Vec<u8>, c: char) {
    value.extend(c.encode_utf8(&mut [0; 4]).as_bytes());
}
