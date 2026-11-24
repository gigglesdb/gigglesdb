//! User routine definitions and V2-only SQL DDL.
use super::*;
use crate::plpgsql::{self, FunctionArgument, FunctionKind, FunctionSignature};
use std::ops::Range;
use std::sync::OnceLock;

pub(crate) const TRIGGER_TYPE: u32 = 2279;
pub(crate) const DEPENDENCY_HINT: &str = "Use DROP ... CASCADE to drop the dependent objects too.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineDiagnostic {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
    /// One-based character position in the SQL statement.
    pub position: Option<usize>,
}
impl RoutineDiagnostic {
    pub(crate) fn new(sqlstate: &'static str, message: impl Into<String>) -> Self {
        Self {
            sqlstate,
            message: message.into(),
            detail: None,
            hint: None,
            context: None,
            position: None,
        }
    }
    pub(crate) fn error(self) -> SqlError {
        DIAGNOSTIC.set(Some(Box::new(self)));
        SqlError::RoutineDdl
    }
}

// Keep SqlError's frozen unit discriminants intact. Move the diagnostic into
// CoreError before an SQL failure leaves the synchronous engine boundary.
thread_local! {
    static DIAGNOSTIC: std::cell::Cell<Option<Box<RoutineDiagnostic>>> = const { std::cell::Cell::new(None) };
}
pub(super) fn clear_diagnostic() {
    DIAGNOSTIC.set(None);
}
pub(super) fn take_diagnostic() -> Option<Box<RoutineDiagnostic>> {
    DIAGNOSTIC.take()
}
pub(super) fn restore_diagnostic(diagnostic: Option<Box<RoutineDiagnostic>>) {
    DIAGNOSTIC.set(diagnostic);
}
pub(crate) fn promote(error: crate::CoreError) -> crate::CoreError {
    match error {
        crate::CoreError::Sql(SqlError::RoutineDdl) => into_core(SqlError::RoutineDdl),
        error => error,
    }
}
pub(crate) fn into_core(error: SqlError) -> crate::CoreError {
    if error != SqlError::RoutineDdl {
        return crate::CoreError::Sql(error);
    }
    take_diagnostic().map_or(crate::CoreError::Sql(SqlError::InternalError), crate::CoreError::RoutineDdl)
}
pub(super) fn error(state: &'static str, message: impl Into<String>) -> SqlError {
    RoutineDiagnostic::new(state, message).error()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineArgument {
    pub name: Option<String>,
    pub data_type: String,
    pub mode: u8,
}

/// The OID derives from the stable catalog ID. Schema names are presentation
/// data: the persisted reference is the schema ID, with zero denoting public.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Routine {
    pub schema: u64,
    pub name: String,
    pub arguments: Vec<RoutineArgument>,
    pub return_type: u32,
    pub language: String,
    pub body: String,
    pub volatility: u8,
    pub strict: bool,
    pub security_definer: bool,
    pub leakproof: bool,
    pub parallel: u8,
    /// Positive finite PostgreSQL float4 cost, stored as IEEE-754 bits.
    pub cost_bits: u32,
    pub owner: u32,
    /// Derived from this definition's body and signature. Each replacement
    /// gets a fresh program, and decoding a catalog record starts cold.
    #[serde(skip)]
    pub(crate) program: RoutineProgram,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RoutineProgram(Arc<OnceLock<plpgsql::CompiledFunction>>);

impl PartialEq for RoutineProgram {
    fn eq(&self, _: &Self) -> bool {
        // Compiling a routine does not change its catalog identity or contents.
        true
    }
}
impl Eq for RoutineProgram {}

impl Routine {
    pub(crate) fn compiled_function(
        &self,
    ) -> Result<plpgsql::CompiledFunction, plpgsql::PlCompileError> {
        if let Some(compiled) = self.program.0.get() {
            return Ok(compiled.clone());
        }
        let compiled = self.compile(&[])?;
        Ok(self.program.0.get_or_init(|| compiled).clone())
    }

    fn compile(
        &self,
        args: &[RoutineArgument],
    ) -> Result<plpgsql::CompiledFunction, plpgsql::PlCompileError> {
        let signature = FunctionSignature {
            name: self.name.clone(),
            kind: FunctionKind::Trigger,
            arguments: args
                .iter()
                .map(|arg| FunctionArgument {
                    name: arg.name.clone(),
                    // The trigger compiler rejects declared arguments before
                    // using their types.
                    data_type: DataType::Text,
                })
                .collect(),
        };
        plpgsql::compile(&self.body, &signature)
    }
}

#[derive(Clone, Debug)]
pub(super) struct Name {
    pub schema: Option<String>,
    pub name: String,
}
impl Name {
    pub(super) fn parse(p: &mut Parser) -> R<Self> {
        let first = p.raw_name()?;
        if p.sym('.') {
            Ok(Self {
                schema: Some(first),
                name: p.name()?,
            })
        } else {
            ident(&first)?;
            Ok(Self {
                schema: None,
                name: first,
            })
        }
    }
    pub(super) fn display(&self) -> String {
        self.schema.as_ref().map_or_else(
            || quote(&self.name),
            |s| format!("{}.{}", quote(s), quote(&self.name)),
        )
    }
}
pub(super) fn quote(name: &str) -> String {
    query::quote_identifier(name)
}

#[derive(Clone, Debug)]
pub(super) enum Ddl {
    Create(Box<Create>),
    Drop {
        names: Vec<(Name, Option<Vec<String>>)>,
        optional: bool,
        cascade: bool,
    },
}
#[derive(Clone, Debug)]
pub(super) struct Create {
    name: Name,
    args: Vec<RoutineArgument>,
    returns: String,
    replace: bool,
    def: Routine,
    body_source: BodySource,
}
impl Ddl {
    pub(super) fn command(&self) -> &'static str {
        match self {
            Self::Create(_) => "CREATE FUNCTION",
            Self::Drop { .. } => "DROP FUNCTION",
        }
    }
}

pub(super) fn starts(tokens: &[Token], words: &[&str]) -> bool {
    tokens.len() >= words.len()
        && tokens
            .iter()
            .zip(words)
            .all(|(token, word)| matches!(token, Token::Word(value) if value == word))
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    [
        ["create", "function"].as_slice(),
        &["create", "or", "replace", "function"],
        &["drop", "function"],
    ]
    .into_iter()
    .any(|words| starts(tokens, words))
}
pub(super) fn spans(source: &str) -> R<Vec<Range<usize>>> {
    let mut spans = Vec::new();
    lex_with_spans(source, &mut Vec::new(), false, Some(&mut spans))?;
    Ok(spans)
}
pub(super) fn finish(p: &mut Parser) -> R<()> {
    p.sym(';');
    if p.at == p.tokens.len() {
        Ok(())
    } else {
        Err(SqlError::Syntax)
    }
}
fn type_name(p: &mut Parser) -> R<String> {
    let first = p.raw_name()?;
    let mut name = if p.sym('.') {
        format!("{first}.{}", p.raw_name()?)
    } else {
        first
    };
    if (name == "double" && p.word("precision"))
        || (matches!(name.as_str(), "character" | "bit") && p.word("varying"))
    {
        let Token::Word(word) = &p.tokens[p.at - 1] else {
            unreachable!()
        };
        name.push(' ');
        name.push_str(word);
    }
    while p.sym('[') {
        p.punct(']')?;
        name.push_str("[]");
    }
    Ok(name)
}
fn arguments(p: &mut Parser, declaration: bool) -> R<Vec<RoutineArgument>> {
    p.punct('(')?;
    let mut args = Vec::new();
    if p.sym(')') {
        return Ok(args);
    }
    loop {
        let mode = if p.word("inout") {
            b'b'
        } else if p.word("out") {
            b'o'
        } else if p.word("variadic") {
            b'v'
        } else {
            p.word("in");
            b'i'
        };
        let first = type_name(p)?;
        let (name, data_type) = if declaration
            && matches!(
                p.tokens.get(p.at),
                Some(Token::Word(_) | Token::QuotedIdent(_))
            ) {
            (Some(first), type_name(p)?)
        } else {
            (None, first)
        };
        args.push(RoutineArgument {
            name,
            data_type,
            mode,
        });
        if !p.sym(',') {
            break;
        }
    }
    p.punct(')')?;
    Ok(args)
}

pub(super) fn parse(p: &mut Parser, source: &str) -> R<Ddl> {
    if p.word("drop") {
        p.need("function")?;
        let optional = p.word("if");
        if optional {
            p.need("exists")?;
        }
        let mut names = Vec::new();
        loop {
            let name = Name::parse(p)?;
            let types = if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
                Some(
                    arguments(p, false)?
                        .into_iter()
                        .map(|arg| arg.data_type)
                        .collect(),
                )
            } else {
                None
            };
            names.push((name, types));
            if !p.sym(',') {
                break;
            }
        }
        let cascade = p.word("cascade");
        if !cascade {
            p.word("restrict");
        }
        finish(p)?;
        return Ok(Ddl::Drop {
            names,
            optional,
            cascade,
        });
    }
    p.need("create")?;
    let replace = p.word("or");
    if replace {
        p.need("replace")?;
    }
    p.need("function")?;
    let name = Name::parse(p)?;
    p.relations.push(schemas::Reference {
        name: name.name.clone(),
        schema: name.schema.clone(),
        create: true,
    });
    let args = arguments(p, true)?;
    p.need("returns")?;
    let returns = if p.word("setof") {
        format!("SETOF {}", type_name(p)?)
    } else {
        type_name(p)?
    };
    let mut def = Routine {
        schema: 0,
        name: name.name.clone(),
        arguments: vec![],
        return_type: TRIGGER_TYPE,
        language: String::new(),
        body: String::new(),
        volatility: b'v',
        strict: false,
        security_definer: false,
        leakproof: false,
        parallel: b'u',
        cost_bits: 100f32.to_bits(),
        owner: 0,
        program: RoutineProgram::default(),
    };
    let mut body_source = None;
    let mut seen = BTreeSet::new();
    while p.at < p.tokens.len() && p.tokens[p.at] != Token::Symbol(';') {
        let option = if p.word("language") {
            def.language = match p.tokens.get(p.at) {
                Some(Token::Word(lang) | Token::QuotedIdent(lang) | Token::String(lang)) => {
                    lang.to_ascii_lowercase()
                }
                _ => return Err(SqlError::Syntax),
            };
            p.at += 1;
            "language"
        } else if p.word("as") {
            let Some(Token::String(body)) = p.tokens.get(p.at) else {
                return Err(SqlError::Syntax);
            };
            def.body = body.clone();
            let locations = spans(source)?;
            let span = locations.get(p.at).ok_or(SqlError::Syntax)?;
            body_source = Some(BodySource::new(source, span));
            p.at += 1;
            "as"
        } else if p.word("volatile") || p.word("stable") || p.word("immutable") {
            let Token::Word(word) = &p.tokens[p.at - 1] else {
                unreachable!()
            };
            def.volatility = word.as_bytes()[0];
            "volatility"
        } else if p.word("not") {
            p.need("leakproof")?;
            def.leakproof = false;
            "leakproof"
        } else if p.word("leakproof") {
            def.leakproof = true;
            "leakproof"
        } else if p.word("strict") {
            def.strict = true;
            "null input"
        } else if p.word("called") {
            p.need("on")?;
            p.need("null")?;
            p.need("input")?;
            def.strict = false;
            "null input"
        } else if p.word("returns") {
            p.need("null")?;
            p.need("on")?;
            p.need("null")?;
            p.need("input")?;
            def.strict = true;
            "null input"
        } else if p.word("security") {
            def.security_definer = p.word("definer");
            if !def.security_definer {
                p.need("invoker")?;
            }
            "security"
        } else if p.word("parallel") {
            def.parallel = if p.word("safe") {
                b's'
            } else if p.word("restricted") {
                b'r'
            } else {
                p.need("unsafe")?;
                b'u'
            };
            "parallel"
        } else if p.word("cost") {
            let Some(Token::Number(cost)) = p.tokens.get(p.at) else {
                return Err(SqlError::Syntax);
            };
            let cost = cost.parse::<f32>().map_err(|_| SqlError::Syntax)?;
            if !cost.is_finite() || cost <= 0.0 {
                return Err(error("22023", "COST must be positive"));
            }
            def.cost_bits = cost.to_bits();
            p.at += 1;
            "cost"
        } else {
            return Err(SqlError::Syntax);
        };
        if !seen.insert(option) {
            return Err(error("42601", "conflicting or redundant options"));
        }
    }
    finish(p)?;
    if !seen.contains("language") || !seen.contains("as") {
        return Err(SqlError::Syntax);
    }
    Ok(Ddl::Create(Box::new(Create {
        name,
        args,
        returns,
        replace,
        def,
        body_source: body_source.ok_or(SqlError::Syntax)?,
    })))
}

/// The compiler reports UTF-8 byte offsets in decoded source. The wire
/// reports one-based character offsets in the statement.
#[derive(Clone, Debug)]
struct BodySource {
    first_character: usize,
    doubled_quotes: bool,
}
impl BodySource {
    fn new(source: &str, span: &Range<usize>) -> Self {
        let literal = &source[span.clone()];
        let start =
            dollar_delimiter(literal).map_or_else(|| literal.find('\'').unwrap_or(0) + 1, str::len);
        Self {
            first_character: source[..span.start + start].chars().count() + 1,
            doubled_quotes: literal.starts_with('\''),
        }
    }

    fn position(&self, body: &str, at: usize) -> Option<usize> {
        let prefix = body.get(..at)?;
        let doubled = if self.doubled_quotes {
            prefix.bytes().filter(|&b| b == b'\'').count()
        } else {
            0
        };
        Some(self.first_character + prefix.chars().count() + doubled)
    }
}

impl Relational {
    pub(crate) fn routine_schema(&self, id: u64) -> Option<&str> {
        if id == 0 {
            Some("public")
        } else {
            self.ext
                .schemas
                .iter()
                .find_map(|(name, schema)| (schema.id == id).then_some(name.as_str()))
        }
    }
    #[allow(dead_code, reason = "consumed by the trigger firing worker")]
    pub(crate) fn routine_by_id(&self, id: u64) -> Option<&Routine> {
        self.ext.routines.get(&id)
    }
    pub(super) fn find_routine(&self, name: &Name, path: &[String]) -> Option<(u64, &Routine)> {
        let find = |schema: &str| {
            self.ext.routines.iter().find_map(|(&id, def)| {
                (def.name == name.name && self.routine_schema(def.schema) == Some(schema))
                    .then_some((id, def))
            })
        };
        if let Some(schema) = &name.schema {
            find(schema)
        } else {
            path.iter().find_map(|s| find(s))
        }
    }
}

pub(super) fn compile_context(name: &str) -> String {
    format!("compilation of PL/pgSQL function \"{name}\" near line 1")
}

pub(super) fn execute(state: &mut Relational, prepared: &Prepared, ddl: &Ddl) -> R<()> {
    match ddl {
        Ddl::Create(create) => {
            let lang = create.def.language.as_str();
            if !matches!(lang, "internal" | "c" | "sql" | "plpgsql") {
                return Err(error(
                    "42704",
                    format!("language \"{lang}\" does not exist"),
                ));
            }
            if lang != "plpgsql" {
                return Err(error("0A000", format!("LANGUAGE {lang} is not supported")));
            }
            if create.def.security_definer {
                return Err(error("0A000", "SECURITY DEFINER functions are not supported"));
            }
            if !matches!(create.returns.as_str(), "trigger" | "pg_catalog.trigger") {
                return Err(error(
                    "0A000",
                    format!("RETURNS {} is not supported", create.returns),
                ));
            }
            let schema = state.routine_creation_schema(prepared)?;
            let existing = state
                .ext
                .routines
                .iter()
                .find(|(_, def)| def.schema == schema && def.name == create.name.name);
            if existing.is_some() && !create.replace && create.args.is_empty() {
                return Err(error(
                    "42723",
                    format!(
                        "function \"{}\" already exists with same argument types",
                        create.name.name
                    ),
                ));
            }
            // Declared arguments are outside the supported routine signature,
            // independently of whether the PL/pgSQL body is validated now.
            if !prepared.skip_routine_body_check || !create.args.is_empty() {
                let compiled = if create.args.is_empty() {
                    create.def.compiled_function()
                } else {
                    create.def.compile(&create.args)
                };
                compiled.map_err(|err| {
                    let mut diagnostic = RoutineDiagnostic::new(err.sqlstate, err.message);
                    diagnostic.hint = err.hint;
                    diagnostic.position = err
                        .position
                        .and_then(|at| create.body_source.position(&create.def.body, at));
                    if err.sqlstate == "42P13" {
                        diagnostic.context = Some(compile_context(&create.name.name));
                    }
                    diagnostic.error()
                })?;
            }
            let mut def = create.def.clone();
            def.schema = schema;
            def.owner = prepared
                .extension_owner
                .load(std::sync::atomic::Ordering::Relaxed);
            let id = if let Some((&id, old)) = existing {
                def.owner = old.owner;
                id
            } else {
                allocate(&mut state.next_id)?
            };
            type_oid(id)?;
            Arc::make_mut(&mut state.ext).routines.insert(id, def);
        }
        Ddl::Drop {
            names,
            optional,
            cascade,
        } => {
            let mut ids = BTreeSet::new();
            let mut notices = Vec::new();
            let mut cascades = Vec::new();
            for (name, args) in names {
                let signature = format!(
                    "{}({})",
                    name.display(),
                    args.as_ref()
                        .map_or_else(String::new, |args| args.join(", "))
                );
                let found = args
                    .as_ref()
                    .is_none_or(Vec::is_empty)
                    .then(|| state.find_routine(name, &prepared.search_path))
                    .flatten();
                let Some((id, _)) = found else {
                    let msg = format!("function {signature} does not exist");
                    if *optional {
                        notices.push(format!("{msg}, skipping"));
                        continue;
                    }
                    if args.is_none() {
                        return Err(error(
                            "42883",
                            format!("could not find a function named \"{}\"", name.display()),
                        ));
                    }
                    return Err(error("42883", msg));
                };
                if !ids.insert(id) {
                    continue;
                }
                let dependents = triggers::routine_dependents(state, id);
                if !*cascade && !dependents.is_empty() {
                    let mut diagnostic = RoutineDiagnostic::new(
                        "2BP01",
                        format!(
                            "cannot drop function {signature} because other objects depend on it"
                        ),
                    );
                    diagnostic.detail = Some(
                        dependents
                            .iter()
                            .map(|d| format!("{d} depends on function {signature}"))
                            .collect::<Vec<_>>()
                            .join("\n"),
                    );
                    diagnostic.hint = Some(DEPENDENCY_HINT.into());
                    return Err(diagnostic.error());
                }
                cascades.extend(
                    dependents
                        .into_iter()
                        .map(|d| format!("drop cascades to {d}")),
                );
            }
            let ext = Arc::make_mut(&mut state.ext);
            ext.routines.retain(|id, _| !ids.contains(id));
            ext.triggers.retain(|_, triggers| {
                triggers.retain(|_, trigger| !ids.contains(&trigger.routine));
                !triggers.is_empty()
            });
            for message in notices {
                user_types::sql_notice(message, "00000");
            }
            cascade_notices(cascades);
        }
    }
    Ok(())
}

impl Relational {
    fn routine_creation_schema(&self, prepared: &Prepared) -> R<u64> {
        let schema = prepared.creation_schema(self)?;
        Ok(self.ext.schemas.get(&schema).map_or(0, |def| def.id))
    }
}

pub(super) fn cascade_notices(mut notices: Vec<String>) {
    if notices.len() == 1 {
        user_types::sql_notice(notices.remove(0), "00000");
    } else if !notices.is_empty() {
        user_types::sql_notice_detail(
            format!("drop cascades to {} other objects", notices.len()),
            notices.join("\n"),
            "00000",
        );
    }
}

impl Prepared {
    /// Existing owners involved in CREATE OR REPLACE or DROP FUNCTION.
    pub fn routine_owners<'a>(
        &'a self,
        catalog: &CatalogPresentation,
        creation_schema: Option<&str>,
    ) -> Vec<(&'a str, u32)> {
        let find = |schema: &str, name: &str| {
            catalog
                .routines
                .values()
                .find_map(|(s, r)| (s == schema && r.name == name).then_some(r.owner))
        };
        match self.statement.get() {
            Statement::Routine(Ddl::Create(c)) if c.replace => creation_schema
                .and_then(|s| find(s, &c.name.name))
                .map(|owner| (c.name.name.as_str(), owner))
                .into_iter()
                .collect(),
            Statement::Routine(Ddl::Drop { names, .. }) => names
                .iter()
                .filter_map(|(name, args)| {
                    if args.as_ref().is_some_and(|args| !args.is_empty()) {
                        return None;
                    }
                    let owner = if let Some(schema) = &name.schema {
                        find(schema, &name.name)
                    } else {
                        self.search_path
                            .iter()
                            .find_map(|schema| find(schema, &name.name))
                    }?;
                    Some((name.name.as_str(), owner))
                })
                .collect(),
            _ => vec![],
        }
    }
    pub fn creates_leakproof_routine(&self) -> bool {
        matches!(self.statement.get(), Statement::Routine(Ddl::Create(c)) if c.def.leakproof)
    }
    pub(super) fn routine_catalog_only(&self) -> bool {
        matches!(
            self.statement.get(),
            Statement::Routine(_) | Statement::Trigger(_)
        ) || matches!(self.statement.get(), Statement::AlterTable(alter) if alter.actions.iter().all(|a| matches!(a, alter_table::Action::TriggerState(..))))
    }
    pub(crate) fn requires_routine_catalog(&self) -> bool {
        matches!(
            self.statement.get(),
            Statement::Routine(_) | Statement::Trigger(_)
        ) || matches!(self.statement.get(), Statement::AlterTable(alter) if alter.actions.iter().any(|a| matches!(a, alter_table::Action::TriggerState(..))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiling_body_preserves_statement_diagnostics() {
        for (body, succeeds) in [
            ("DECLARE t timestamp(7); BEGIN PERFORM 1; RETURN NEW; END", true),
            ("DECLARE t timestamp(7); BEGIN DELETE FRM t; RETURN NEW; END", false),
        ] {
            let sql = format!(
                "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $body${body}$body$"
            );
            let prepared = Prepared::parse(&sql, 0, 0).unwrap();
            let Statement::Routine(Ddl::Create(create)) = prepared.statement.get() else {
                panic!("expected CREATE FUNCTION");
            };
            ddl_type::publish(&["outer warning".into()]);
            user_types::sql_notice("outer notice".into(), "00000");
            user_types::sql_error_hint("outer hint");
            user_types::sql_error_detail("outer detail".into());
            user_types::view_replace_error("outer message".into());
            let outer = RoutineDiagnostic::new("XX000", "outer routine diagnostic");
            let error = outer.clone().error();

            assert_eq!(create.def.compiled_function().is_ok(), succeeds);

            assert_eq!(take_sql_warnings(), ["outer warning"]);
            assert_eq!(take_sql_notices(), [("outer notice".into(), None, Some("00000"))]);
            assert_eq!(user_types::take_sql_error_hint(), Some("outer hint"));
            assert_eq!(user_types::take_sql_error_detail().as_deref(), Some("outer detail"));
            assert_eq!(take_sql_error_message().as_deref(), Some("outer message"));
            let crate::CoreError::RoutineDdl(diagnostic) = into_core(error) else {
                panic!("expected outer routine diagnostic");
            };
            assert_eq!(*diagnostic, outer);
        }
    }

    #[test]
    fn unchecked_bodies_stay_cold_until_the_compiler_is_needed() {
        let mut state = Relational::default();
        for (name, body) in [
            ("valid", "BEGIN RETURN NEW; END"),
            ("invalid", "BEGIN RETURN NEW END"),
        ] {
            let sql = format!(
                "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $body${body}$body$"
            );
            let mut prepared = Prepared::parse(&sql, 0, 0).unwrap();
            prepared.skip_routine_body_check = true;
            let Statement::Routine(ddl) = prepared.statement.get() else {
                panic!("expected routine DDL");
            };
            execute(&mut state, &prepared, ddl).unwrap();
            let (_, routine) = state
                .find_routine(
                    &Name {
                        schema: None,
                        name: name.into(),
                    },
                    &prepared.search_path,
                )
                .unwrap();
            assert!(routine.program.0.get().is_none());
            if name == "valid" {
                routine.compiled_function().unwrap();
                assert!(routine.program.0.get().is_some());
            } else {
                assert_eq!(routine.compiled_function().unwrap_err().sqlstate, "42601");
            }
        }
    }
}
