//! Bounded trusted local SQL frontend. Filesystem/Admin access is authority.
use giggles_core::relational::{SqlResult, Value};
use giggles_core::{Admin, CoreError, Limits, SqlSession};
use std::{
    fmt,
    fs::{File, OpenOptions},
    io::{self, BufRead, Read, Write},
    path::PathBuf,
};

/// Maximum UTF-8 script size, including whitespace and terminators.
pub const MAX_SCRIPT: usize = 1024 * 1024;
/// Maximum individual statement size in UTF-8 bytes.
pub const MAX_STATEMENT: usize = 64 * 1024;
/// Maximum SQL statements in a script or interactive run.
pub const MAX_STATEMENTS: usize = 256;
/// Stable frontend diagnostic and process exit status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendError {
    pub status: u8,
    pub message: String,
}
impl fmt::Display for FrontendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for FrontendError {}
type Result<T> = std::result::Result<T, FrontendError>;
fn input(message: impl Into<String>) -> FrontendError {
    FrontendError {
        status: 2,
        message: message.into(),
    }
}
fn sql(error: CoreError) -> FrontendError {
    FrontendError {
        status: 3,
        message: format!("SQL/core error: {error:?}"),
    }
}
fn io_error(error: io::Error) -> FrontendError {
    FrontendError {
        status: 4,
        message: format!("IO error: {error}"),
    }
}

/// Read at most `limit + 1` bytes, rejecting oversized, non-UTF8 or NUL input.
pub fn bounded_read(reader: impl Read, limit: usize) -> Result<String> {
    let mut bytes = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > limit {
        return Err(input(format!("input exceeds {limit} bytes")));
    }
    let text = String::from_utf8(bytes).map_err(|_| input("input is not valid UTF-8"))?;
    if text.contains('\0') {
        return Err(input("NUL input is unsupported"));
    }
    Ok(text)
}

/// Split complete SQL statements without interpreting semicolons in literals.
/// `allow_final` permits a complete un-terminated final statement for --sql only.
///
/// ```
/// let sql = "INSERT INTO app VALUES('it''s;雪'); SELECT * FROM app LIMIT 1;";
/// let statements = giggles_cli::split_script(sql, false).unwrap();
/// assert_eq!(statements.len(), 2);
/// assert!(statements[0].contains("it''s;雪"));
/// assert!(giggles_cli::split_script("SELECT 'incomplete", false).is_err());
/// ```
pub fn split_script(text: &str, allow_final: bool) -> Result<Vec<String>> {
    let (mut statements, tail, quoted) = scan(text)?;
    if quoted {
        return Err(input("EOF inside quoted SQL string"));
    }
    if !tail.trim().is_empty() {
        if !allow_final {
            return Err(input("incomplete statement at EOF: expected semicolon"));
        }
        statements.push(tail.trim().to_owned());
    }
    if statements.len() > MAX_STATEMENTS {
        return Err(input("script exceeds 256 statements"));
    }
    Ok(statements)
}
fn scan(text: &str) -> Result<(Vec<String>, &str, bool)> {
    if text.len() > MAX_SCRIPT {
        return Err(input("script exceeds 1MiB"));
    }
    if text.contains('\0') {
        return Err(input("NUL input is unsupported"));
    }
    let b = text.as_bytes();
    let (mut start, mut i, mut quoted) = (0, 0, false);
    let mut statements = Vec::new();
    while i < b.len() {
        if i - start >= MAX_STATEMENT && b[i] != b';' {
            return Err(input("statement exceeds 64KiB"));
        }
        if b[i] == b'\'' {
            if quoted && b.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            quoted = !quoted;
        } else if !quoted {
            if (b[i] == b'-' && b.get(i + 1) == Some(&b'-'))
                || (b[i] == b'/' && b.get(i + 1) == Some(&b'*'))
            {
                return Err(input("SQL comments are unsupported by the embedded core"));
            }
            if b[i] == b';' {
                let statement = text[start..i].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_owned());
                }
                if statements.len() > MAX_STATEMENTS {
                    return Err(input("script exceeds 256 statements"));
                }
                start = i + 1;
            }
        }
        i += 1;
    }
    if b.len() - start > MAX_STATEMENT {
        return Err(input("statement exceeds 64KiB"));
    }
    Ok((statements, &text[start..], quoted))
}

/// Parse native typed Value JSON, never SQL text substitution.
pub fn parse_params(text: &str) -> Result<Vec<Value>> {
    if text.len() > MAX_STATEMENT {
        return Err(input("parameters exceed 64KiB"));
    }
    let values: Vec<Value> =
        serde_json::from_str(text).map_err(|e| input(format!("invalid typed parameters: {e}")))?;
    if values.len() > 256 {
        return Err(input("parameters exceed 256 values"));
    }
    Ok(values)
}

/// Output serialization, independent of SQL parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Json,
    Table,
}
impl OutputMode {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "json" => Ok(Self::Json),
            "table" => Ok(Self::Table),
            _ => Err(input("format must be json or table")),
        }
    }
}
fn escape(text: &str) -> String {
    text.chars().flat_map(char::escape_default).collect()
}
fn cell(value: &Value) -> String {
    match value {
        Value::Date(v) => format!(
            "Date(pg={})",
            v.to_binary()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
        Value::Time(v) => format!(
            "Time(pg={})",
            v.to_binary()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
        Value::TimeTz(v) => format!(
            "TimeTz(pg={})",
            v.to_binary()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
        Value::Timestamp(v) => format!(
            "Timestamp(pg={})",
            v.to_binary()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
        Value::TimestampTz(v) => format!(
            "TimestampTz(pg={})",
            v.to_binary()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
        Value::Interval(v) => format!(
            "Interval(pg={})",
            v.to_binary()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
        Value::Numeric(v) => format!("NUMERIC({v})"),
        Value::Float4(v) => format!("REAL(bits={:08x})", v.to_bits()),
        Value::Float8(v) => format!("DOUBLE PRECISION(bits={:016x})", v.to_bits()),
        Value::Null => "NULL".into(),
        Value::Bool(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::Text(v) => format!("\"{}\"", escape(v)),
        Value::Bytes(v) => {
            let mut s = String::from("0x");
            for b in v {
                use std::fmt::Write;
                write!(s, "{b:02x}").expect("String write");
            }
            s
        }
    }
}

#[test]
fn numeric_display_preserves_scale_signed_zero_and_nan_payload() {
    use giggles_core::relational::{Float4, Float8, Numeric};
    assert_eq!(
        cell(&Value::Numeric(Numeric::parse("1.00100").unwrap())),
        "NUMERIC(1.00100)"
    );
    assert_eq!(
        cell(&Value::Float4(Float4::from_bits(0x80000000))),
        "REAL(bits=80000000)"
    );
    assert_eq!(
        cell(&Value::Float4(Float4::from_bits(0x7fc01234))),
        "REAL(bits=7fc01234)"
    );
    assert_eq!(
        cell(&Value::Float8(Float8::from_bits(0x8000000000000000))),
        "DOUBLE PRECISION(bits=8000000000000000)"
    );
    assert_eq!(
        cell(&Value::Float8(Float8::from_bits(0x7ff8000000001234))),
        "DOUBLE PRECISION(bits=7ff8000000001234)"
    );
}
/// Render one result; JSON envelopes retain native types and row versions.
pub fn render_result(result: &SqlResult, mode: OutputMode) -> String {
    if mode == OutputMode::Json {
        return format!("{}\n", serde_json::json!({"version":1,"result":result}));
    }
    let mut rows = vec![
        result
            .columns
            .iter()
            .map(|c| escape(c))
            .chain(std::iter::once("@version".into()))
            .collect::<Vec<_>>(),
    ];
    for row in &result.rows {
        rows.push(
            row.values
                .iter()
                .map(cell)
                .chain(std::iter::once(row.version.to_string()))
                .collect(),
        );
    }
    let widths: Vec<usize> = (0..rows[0].len())
        .map(|i| {
            rows.iter()
                .map(|r| r.get(i).map_or(0, |c| c.chars().count()))
                .max()
                .unwrap_or(0)
                .min(80)
        })
        .collect();
    let mut output = String::new();
    for row in rows {
        for (i, c) in row.iter().enumerate() {
            if i != 0 {
                output.push_str(" | ");
            }
            output.push_str(c);
            output.push_str(
                &" ".repeat(
                    widths
                        .get(i)
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(c.chars().count()),
                ),
            );
        }
        output.push('\n');
    }
    output.push_str(&format!(
        "{} row(s), {} affected\n",
        result.rows.len(),
        result.affected
    ));
    output
}

/// Select exactly one bounded input source.
#[derive(Debug, Clone)]
pub enum InputMode {
    Sql(String),
    File(PathBuf),
    Stdin,
    Repl,
}
/// Fully validated command-line options. Local database access uses trusted Admin authority.
#[derive(Debug, Clone)]
pub struct Options {
    pub create: bool,
    pub database: PathBuf,
    pub source: InputMode,
    pub mode: OutputMode,
    pub atomic: bool,
    pub params_file: Option<PathBuf>,
    pub expected_version: Option<u64>,
    pub output: Option<PathBuf>,
}
/// Usage text is also available without database IO.
pub const HELP: &str = "giggles-sql (--create PATH | --open PATH) (--sql SQL | --file PATH | --stdin | --repl) [--atomic] [--format json|table] [--params-file PATH] [--expected-version U64] [--output NEW_PATH]\nMetacommands: .help .schema .quit .mode json|table\nScripts require semicolons. Authority is trusted local Admin, not task credentials.\n";
impl Options {
    /// Parse options, rejecting duplicates and ambiguous modes.
    pub fn parse(args: Vec<String>) -> Result<Self> {
        let mut args = args.into_iter();
        let mut seen = std::collections::BTreeSet::new();
        let (mut database, mut source) = (None, None);
        let (mut mode, mut atomic, mut params_file, mut expected_version, mut output) =
            (OutputMode::Table, false, None, None, None);
        while let Some(arg) = args.next() {
            if !seen.insert(arg.clone()) {
                return Err(input(format!("duplicate option {arg}")));
            }
            let mut value = || {
                args.next()
                    .ok_or_else(|| input(format!("missing value for {arg}")))
            };
            match arg.as_str() {
                "--create" | "--open" => {
                    if database.is_some() {
                        return Err(input("choose exactly one create/open mode"));
                    }
                    database = Some((arg == "--create", PathBuf::from(value()?)));
                }
                "--sql" | "--file" | "--stdin" | "--repl" => {
                    if source.is_some() {
                        return Err(input("choose exactly one input source"));
                    }
                    source = Some(match arg.as_str() {
                        "--sql" => InputMode::Sql(value()?),
                        "--file" => InputMode::File(value()?.into()),
                        "--stdin" => InputMode::Stdin,
                        _ => InputMode::Repl,
                    });
                }
                "--atomic" => atomic = true,
                "--format" => mode = OutputMode::parse(&value()?)?,
                "--params-file" => params_file = Some(value()?.into()),
                "--expected-version" => {
                    expected_version = Some(
                        value()?
                            .parse::<u64>()
                            .map_err(|_| input("expected version must be a positive u64"))?,
                    )
                }
                "--output" => output = Some(value()?.into()),
                _ => return Err(input(format!("unknown option {arg}"))),
            }
        }
        let (create, database) =
            database.ok_or_else(|| input("explicit --create or --open is required"))?;
        let source = source.ok_or_else(|| input("input source is required"))?;
        if (params_file.is_some() || expected_version.is_some())
            && !matches!(source, InputMode::Sql(_))
        {
            return Err(input("parameters/CAS require exactly one --sql statement"));
        }
        if expected_version == Some(0) {
            return Err(input("expected version must be positive"));
        }
        if atomic && matches!(source, InputMode::Repl) {
            return Err(input("--atomic is for batch input"));
        }
        Ok(Self {
            create,
            database,
            source,
            mode,
            atomic,
            params_file,
            expected_version,
            output,
        })
    }
}

/// Owns one persistent SQL session; no implicit authority rebind or retries.
pub struct Runner {
    session: SqlSession,
    pub mode: OutputMode,
}
impl Runner {
    /// Obtain a session from explicitly trusted local Admin authority.
    pub fn new(admin: &Admin, mode: OutputMode) -> Self {
        Self {
            session: admin.sql_session(),
            mode,
        }
    }
    /// Execute one statement, keeping explicit transaction state between calls.
    pub fn execute(
        &mut self,
        statement: &str,
        params: &[Value],
        expected: Option<u64>,
        output: &mut impl Write,
    ) -> Result<()> {
        let result = self
            .session
            .execute(statement, params, expected)
            .map_err(sql)?;
        output
            .write_all(render_result(&result, self.mode).as_bytes())
            .map_err(io_error)
    }
    /// Roll back unfinished work and reject unfinished transactions at EOF/quit.
    pub fn finish(&mut self) -> Result<()> {
        if self.session.in_transaction() {
            self.session.rollback();
            return Err(FrontendError {
                status: 3,
                message: "unfinished transaction rolled back at EOF/quit".into(),
            });
        }
        Ok(())
    }
    /// Run a pre-split bounded batch with optional all-or-nothing transaction.
    pub fn batch(
        &mut self,
        statements: &[String],
        params: &[Value],
        expected: Option<u64>,
        atomic: bool,
        output: &mut impl Write,
    ) -> Result<()> {
        let result = (|| {
            if atomic {
                // Use the core parser's transaction-control classification, not a keyword heuristic.
                for statement in statements {
                    let p = self.session.prepare(statement).map_err(sql)?;
                    if matches!(
                        p.command(),
                        "BEGIN" | "COMMIT" | "ROLLBACK" | "begin" | "commit" | "rollback"
                    ) {
                        return Err(input("--atomic rejects explicit transaction controls"));
                    }
                }
                self.session.execute("BEGIN", &[], None).map_err(sql)?;
            }
            for statement in statements {
                self.execute(statement, params, expected, output)?;
            }
            // Detect buffered-output failures while an atomic transaction can still roll back.
            output.flush().map_err(io_error)?;
            if atomic {
                self.session.execute("COMMIT", &[], None).map_err(sql)?;
            }
            self.finish()
        })();
        if result.is_err() {
            self.session.rollback();
        }
        result
    }
    /// Bounded interactive/piped REPL; errors stop the run and discard staging.
    pub fn repl(
        &mut self,
        admin: &Admin,
        reader: &mut impl BufRead,
        output: &mut impl Write,
        diagnostics: &mut impl Write,
        prompts: bool,
    ) -> Result<()> {
        let result = self.repl_inner(admin, reader, output, diagnostics, prompts);
        if result.is_err() {
            self.session.rollback();
        }
        result
    }
    fn repl_inner(
        &mut self,
        admin: &Admin,
        reader: &mut impl BufRead,
        output: &mut impl Write,
        diagnostics: &mut impl Write,
        prompts: bool,
    ) -> Result<()> {
        let (mut pending, mut bytes, mut count) = (String::new(), 0, 0);
        loop {
            if prompts {
                diagnostics
                    .write_all(if pending.trim().is_empty() {
                        b"giggles> "
                    } else {
                        b"...> "
                    })
                    .map_err(io_error)?;
                diagnostics.flush().map_err(io_error)?;
            }
            let mut line = Vec::new();
            let n = reader
                .take((MAX_STATEMENT + 2) as u64)
                .read_until(b'\n', &mut line)
                .map_err(io_error)?;
            if n == 0 {
                if !pending.trim().is_empty() {
                    return Err(input("incomplete statement at EOF"));
                }
                return self.finish();
            }
            bytes += n;
            if bytes > MAX_SCRIPT {
                return Err(input("interactive run exceeds 1MiB"));
            }
            let line = bounded_read(line.as_slice(), MAX_STATEMENT)?;
            if pending.trim().is_empty() && line.trim_start().starts_with('.') {
                match line.trim() {
                    ".help" => output.write_all(HELP.as_bytes()).map_err(io_error)?,
                    ".quit" => return self.finish(),
                    ".schema" => {
                        let (epoch, tables) = admin.catalog().map_err(sql)?;
                        let tables = tables.into_iter().map(|mut table| {
                            let heap = table.hidden_rowid().is_some();
                            table.columns = table.logical_columns().cloned().collect();
                            if heap {
                                table.primary_key.clear();
                            }
                            table
                        }).collect::<Vec<_>>();
                        writeln!(output, "{}", serde_json::json!({"version":1,"catalog":{"epoch":epoch,"tables":tables}})).map_err(io_error)?;
                    }
                    ".mode json" => self.mode = OutputMode::Json,
                    ".mode table" => self.mode = OutputMode::Table,
                    _ => return Err(input("unknown metacommand; use .help")),
                }
                continue;
            }
            pending.push_str(&line);
            let (statements, tail, _) = scan(&pending)?;
            let tail = tail.to_owned();
            count += statements.len();
            if count > MAX_STATEMENTS {
                return Err(input("interactive run exceeds 256 statements"));
            }
            for statement in statements {
                self.execute(&statement, &[], None, output)?;
            }
            output.flush().map_err(io_error)?;
            pending = tail;
        }
    }
}

/// Execute validated options. Input is preflighted before database creation/open.
pub fn run(
    options: Options,
    reader: &mut impl BufRead,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
    prompts: bool,
) -> Result<()> {
    let mut statements = Vec::new();
    match &options.source {
        InputMode::Sql(text) => {
            statements = split_script(text, true)?;
            if statements.len() != 1 {
                return Err(input("--sql requires exactly one statement"));
            }
        }
        InputMode::File(path) => {
            statements = split_script(
                &bounded_read(File::open(path).map_err(io_error)?, MAX_SCRIPT)?,
                false,
            )?
        }
        InputMode::Stdin => {
            statements = split_script(&bounded_read(&mut *reader, MAX_SCRIPT)?, false)?
        }
        InputMode::Repl => (),
    }
    let params = if let Some(path) = &options.params_file {
        parse_params(&bounded_read(
            File::open(path).map_err(io_error)?,
            MAX_STATEMENT,
        )?)?
    } else {
        Vec::new()
    };
    // Existing aliases/hardlinks cannot be clobbered because output always uses create_new.
    // Also forbid output inside a new database path to avoid reserving the DB itself.
    if let Some(path) = &options.output {
        let out = std::path::absolute(path).map_err(io_error)?;
        let db = std::path::absolute(&options.database).map_err(io_error)?;
        if out == db || out.starts_with(&db) {
            return Err(input("output must be outside the database path"));
        }
    }
    let mut file = options
        .output
        .as_ref()
        .map(|p| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(p)
                .map_err(io_error)
        })
        .transpose()?;
    let output: &mut dyn Write = match &mut file {
        Some(f) => f,
        None => stdout,
    };
    let admin = if options.create {
        Admin::create(&options.database, Limits::default())
    } else {
        // The one-shot CLI leaves automatic fence recovery disabled.
        Admin::open(&options.database, Limits::default())
    }
    .map_err(sql)?;
    let mut runner = Runner::new(&admin, options.mode);
    // The extra reference keeps generic helper APIs usable with trait-object outputs.
    let result = match options.source {
        InputMode::Repl => runner.repl(&admin, reader, &mut { output }, stderr, prompts),
        _ => runner.batch(
            &statements,
            &params,
            options.expected_version,
            options.atomic,
            &mut { output },
        ),
    };
    result?;
    if let Some(file) = file.as_mut() {
        file.flush().map_err(io_error)?;
    } else {
        stdout.flush().map_err(io_error)?;
    }
    Ok(())
}
