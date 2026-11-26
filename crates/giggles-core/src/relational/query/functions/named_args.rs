//! PostgreSQL function argument notation. Keep the source order until binding
//! so diagnostics can describe the call as written and all AST walkers see its inputs.
use super::*;

const PREFIX: &str = "\0named-call:";
pub(super) const HINT: &str = "No function matches the given name and argument types. You might need to add explicit type casts.";
const AMBIGUOUS_HINT: &str =
    "Could not choose a best candidate function. You might need to add explicit type casts.";

#[derive(Clone, Debug, Default)]
pub(in crate::relational) struct Names {
    positional: usize,
    names: Vec<String>,
    variadic: bool,
    values: Vec<std::ops::Range<usize>>,
}

fn arrow(token: Option<&Token>) -> bool {
    matches!(token, Some(Token::Op(op)) if op == "=>" || op == ":=")
}

/// Keep named-argument syntax out of newly persisted expression text.
pub(in crate::relational) fn reject_stored(tokens: &[Token]) -> R<()> {
    if tokens.iter().any(|token| arrow(Some(token))) {
        return Err(SqlError::Unsupported);
    }
    Ok(())
}

/// A cheap lookahead for callers with a separate positional grammar.
pub(in crate::relational) fn in_call(tokens: &[Token]) -> bool {
    let mut depth = 0usize;
    for token in tokens {
        match token {
            Token::Symbol('(' | '[') => depth += 1,
            Token::Symbol(')' | ']') if depth == 0 => break,
            Token::Symbol(')' | ']') => depth -= 1,
            token if depth == 0 && arrow(Some(token)) => return true,
            _ => {}
        }
    }
    false
}

impl Names {
    /// Consume only the argument label, leaving its expression to the caller.
    pub(in crate::relational) fn argument(&mut self, p: &mut Parser, syntax_only: bool) -> R<()> {
        self.variadic = p.word("variadic");
        if arrow(p.tokens.get(p.at + 1))
            && matches!(
                p.tokens.get(p.at),
                Some(Token::Word(_) | Token::QuotedIdent(_))
            )
        {
            if syntax_only {
                return Err(super::super::clause_syntax(p.tokens.get(p.at + 1)));
            }
            let name = p.raw_name()?;
            if name.is_empty() {
                return Err(SqlError::Syntax);
            }
            p.at += 1;
            if self.names.contains(&name) {
                user_types::view_replace_error(format!(
                    "argument name \"{name}\" used more than once"
                ));
                return Err(SqlError::Syntax);
            }
            self.names.push(name);
        } else if arrow(p.tokens.get(p.at)) {
            return Err(super::super::clause_syntax(p.tokens.get(p.at)));
        } else {
            if !self.names.is_empty() {
                user_types::view_replace_error(
                    "positional argument cannot follow named argument".into(),
                );
                return Err(SqlError::Syntax);
            }
            self.positional += 1;
        }
        if matches!(p.tokens.get(p.at), None | Some(Token::Symbol(')' | ','))) {
            return Err(super::super::clause_syntax(p.tokens.get(p.at)));
        }
        Ok(())
    }
}

pub(in crate::relational::query) fn parse(
    p: &mut Parser,
    depth: usize,
    aggregate_argument: bool,
    syntax_only: bool,
) -> R<(Names, Vec<Scalar>)> {
    let mut names = Names::default();
    let mut args = Vec::new();
    loop {
        if args.len() == MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        names.argument(p, syntax_only)?;
        let start = p.at;
        let value = match Scalar::parse_expr(p, depth, aggregate_argument) {
            Err(SqlError::Unsupported) if arrow(p.tokens.get(p.at)) => {
                return Err(super::super::clause_syntax(p.tokens.get(p.at)));
            }
            value => value?,
        };
        args.push(value);
        names.values.push(start..p.at);
        if arrow(p.tokens.get(p.at))
            || names.variadic && p.tokens.get(p.at) == Some(&Token::Symbol(','))
        {
            return Err(super::super::clause_syntax(p.tokens.get(p.at)));
        }
        if !p.sym(',') {
            break;
        }
    }
    Ok((names, args))
}

struct Call<'a> {
    name: &'a str,
    positional: usize,
    names: Vec<&'a str>,
    variadic: bool,
}

pub(super) fn scalar(name: String, names: Names, args: Vec<Scalar>) -> Scalar {
    // The length separates the routine name from labels, since schema lookup
    // already uses NUL inside its internal routine names.
    Scalar::Call(
        format!(
            "{PREFIX}{}:{}:{}:{name}{}",
            name.len(),
            names.positional,
            u8::from(names.variadic),
            names.names.join("\0")
        ),
        args,
    )
}

fn decode(s: &str) -> Option<Call<'_>> {
    let (length, rest) = s.strip_prefix(PREFIX)?.split_once(':')?;
    let (positional, rest) = rest.split_once(':')?;
    let (variadic, rest) = rest.split_once(':')?;
    let (name, labels) = rest.split_at_checked(length.parse().ok()?)?;
    let labels = labels.split('\0').collect::<Vec<_>>();
    if name.is_empty() || labels.iter().any(|label| label.is_empty()) {
        return None;
    }
    Some(Call {
        name,
        positional: positional.parse().ok()?,
        names: labels,
        variadic: match variadic {
            "0" => false,
            "1" => true,
            _ => return None,
        },
    })
}

fn decode_arguments(name: &str, count: usize) -> R<Call<'_>> {
    decode(name)
        .filter(|call| call.positional.checked_add(call.names.len()) == Some(count))
        .ok_or(SqlError::InternalError)
}

pub(in crate::relational::query) fn routine(name: &str) -> &str {
    name.strip_prefix(PREFIX)
        .and_then(|rest| {
            let (length, rest) = rest.split_once(':')?;
            let (_, rest) = rest.split_once(':')?;
            let (_, rest) = rest.split_once(':')?;
            rest.get(..length.parse::<usize>().ok()?)
        })
        .unwrap_or(name)
}

pub(super) fn requires_lookup(name: &str) -> bool {
    decode(name).is_some_and(|call| {
        if call.uses_positional_lookup() {
            return deferred_routine_error(call.name).is_some();
        }
        call.layouts()
            .next()
            .is_none_or(|layout| layout.signature.aggregate)
    })
}

pub(in crate::relational::query) fn is_named(name: &str) -> bool {
    name.starts_with(PREFIX)
}

fn parser_type<'a>(labels: impl IntoIterator<Item = &'a str>) -> Option<DataType> {
    labels.into_iter().find_map(|label| match label {
        "parser_name" => Some(DataType::Text),
        "parser_oid" => Some(DataType::Oid),
        _ => None,
    })
}

fn builtin(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

fn display(name: &str) -> String {
    [
        UNDEFINED_ROUTINE_PREFIX,
        PG_TOAST_ROUTINE_PREFIX,
        INVALID_SCHEMA_ROUTINE_PREFIX,
    ]
    .into_iter()
    .find_map(|prefix| name.strip_prefix(prefix))
    .unwrap_or(name)
    .replace('\0', ".")
}

fn diagnostic<'a>(
    name: &str,
    args: impl IntoIterator<Item = &'a Scalar>,
    labels: impl Iterator<Item = Option<&'a str>>,
    error: SqlError,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<SqlError> {
    let types = args
        .into_iter()
        .zip(labels)
        .map(|(arg, label)| {
            let ty = argument_type(arg, resolve, tables, parameters)?;
            let ty = ty.as_ref().map_or("unknown", typeof_expr::name);
            Ok(match label {
                Some(label) => format!("{label} => {ty}"),
                None => ty.into(),
            })
        })
        .collect::<R<Vec<_>>>()?;
    let name = display(name);
    let (message, hint) = if error == SqlError::AmbiguousFunction {
        (
            format!("function {name}({}) is not unique", types.join(", ")),
            AMBIGUOUS_HINT,
        )
    } else {
        (undefined_signature(&name, &types), HINT)
    };
    user_types::view_replace_error(message);
    user_types::sql_error_hint(hint);
    Ok(error)
}

pub(super) fn undefined_signature(name: &str, types: &[impl AsRef<str>]) -> String {
    let types = types
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ");
    format!("function {name}({types}) does not exist")
}

fn argument_type(
    arg: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<Option<DataType>> {
    if arg.aggregate_unknown(parameters) {
        Ok(None)
    } else {
        arg.bind(resolve, tables, parameters)
            .map(|bound| Some(bound.ty))
    }
}

type Input = (&'static str, Option<DataType>);

struct Signature {
    inputs: &'static [Input],
    defaults: &'static [&'static str],
    variadic: bool,
    aggregate: bool,
}

mod postgres_names;
mod signatures;

pub(in crate::relational::query) fn postgres_has_named_inputs(name: &str) -> bool {
    postgres_names::NAMES.binary_search(&name).is_ok()
}

struct Layout<'a> {
    signature: &'a Signature,
    slots: Vec<Option<usize>>,
}

impl Call<'_> {
    fn layout(&self, signature: &'static Signature) -> Option<Layout<'static>> {
        let mut slots = vec![None; signature.inputs.len()];
        if signature.variadic != self.variadic || self.positional > slots.len() {
            return None;
        }
        for (i, slot) in slots.iter_mut().take(self.positional).enumerate() {
            *slot = Some(i);
        }
        for (i, name) in self.names.iter().enumerate() {
            let position = signature
                .inputs
                .iter()
                .position(|(label, _)| label == name)?;
            if slots[position].replace(self.positional + i).is_some() {
                return None;
            }
        }
        let required = slots.len() - signature.defaults.len();
        if slots[..required].iter().any(Option::is_none) {
            return None;
        }
        while slots.last() == Some(&None) {
            slots.pop();
        }
        Some(Layout { signature, slots })
    }

    fn layouts(&self) -> impl Iterator<Item = Layout<'static>> + '_ {
        signatures::lookup(builtin(self.name))
            .iter()
            .filter_map(|signature| self.layout(signature))
    }

    fn uses_positional_lookup(&self) -> bool {
        let name = builtin(self.name);
        deferred_routine_error(self.name) == Some(SqlError::InvalidSchema)
            || signatures::lookup(name).is_empty() && postgres_has_named_inputs(name)
    }

    fn labels(&self) -> impl Iterator<Item = Option<&str>> + '_ {
        std::iter::repeat_n(None, self.positional).chain(self.names.iter().copied().map(Some))
    }

    fn error(
        &self,
        error: SqlError,
        args: &[Scalar],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        parameters: &[Option<DataType>],
    ) -> R<SqlError> {
        diagnostic(
            self.name,
            args,
            self.labels(),
            error,
            resolve,
            tables,
            parameters,
        )
    }
}

impl Layout<'_> {
    fn arguments(&self, args: &[Scalar]) -> R<Vec<Scalar>> {
        let default_start = self.signature.inputs.len() - self.signature.defaults.len();
        self.slots
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                if let Some(index) = *slot {
                    let arg = args[index].clone();
                    // regexp_replace's fourth positional input defaults to the text overload
                    // for unknowns. The label start selects the integer overload instead.
                    if self.signature.inputs[i].0 == "start"
                        && matches!(arg, Scalar::Literal(Value::Text(_) | Value::Null))
                        && self.signature.inputs[2].0 == "replacement"
                    {
                        return Ok(Scalar::Cast(Box::new(arg), DataType::Integer));
                    }
                    return Ok(arg);
                }
                let mut p = Parser {
                    tokens: lex(self.signature.defaults[i - default_start])?,
                    at: 0,
                    params: BTreeSet::new(),
                    relations: Vec::new(),
                    privilege_seen: false,
                    privilege_casts: false,
                };
                Scalar::parse_expr(&mut p, 0, false)
            })
            .collect()
    }

    fn accepts(&self, types: &[Option<DataType>]) -> bool {
        self.slots
            .iter()
            .zip(self.signature.inputs)
            .all(
                |(slot, (_, expected))| match (slot.and_then(|i| types[i].as_ref()), expected) {
                    (Some(actual), Some(expected)) => compatible(actual, expected),
                    _ => true,
                },
            )
    }
}

fn compatible(actual: &DataType, expected: &DataType) -> bool {
    actual == expected
        || match expected {
            DataType::Integer => *actual == DataType::SmallInt,
            DataType::DoublePrecision => number(actual),
            DataType::Text => matches!(actual, DataType::Varchar | DataType::Bpchar),
            _ => false,
        }
}

/// Resolve a named call before passing its arguments to the positional binder.
pub(super) fn bind(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let call = decode_arguments(name, args.len())?;
    if call.uses_positional_lookup() {
        // Keep the positional lookup error for missing schemas and catalog
        // routines whose named signatures are outside this profile.
        return Scalar::bind_call(call.name, args, resolve, tables, parameters)
            .and(Err(SqlError::Unsupported));
    }
    for layout in call.layouts() {
        if layout.signature.aggregate {
            let types = args
                .iter()
                .map(|arg| argument_type(arg, resolve, tables, parameters))
                .collect::<R<Vec<_>>>()?;
            if !layout.accepts(&types) {
                continue;
            }
            user_types::view_replace_error("aggregates cannot use named arguments".into());
            return Err(SqlError::Unsupported);
        }
        let positional = layout.arguments(args)?;
        let bound = if text_search::known(call.name) {
            let first = parser_type(call.names.iter().copied());
            text_search::bind(
                call.name,
                first.as_ref(),
                &positional,
                resolve,
                tables,
                parameters,
            )
        } else {
            let bound = positional
                .iter()
                .map(|arg| arg.bind(resolve, tables, parameters))
                .collect::<R<Vec<_>>>()?;
            Scalar::finish_call(call.name, &positional, bound, parameters)
        };
        match bound {
            // Both four-argument variants exist positionally. Named notation
            // must keep the variant selected by the label.
            Ok(BoundScalar {
                node: Node::Call(_, ref bound),
                ..
            }) if builtin(call.name) == "regexp_replace"
                && let [_, _, _, fourth] = bound.as_slice()
                && layout.signature.inputs[3].1.as_ref() != Some(&fourth.ty) => {}
            Err(SqlError::UndefinedFunction | SqlError::TextSearchUndefinedFunction) => {}
            Err(error @ SqlError::AmbiguousFunction) => {
                return Err(call.error(error, args, resolve, tables, parameters)?);
            }
            result => return result,
        }
    }
    Err(call.error(
        SqlError::UndefinedFunction,
        args,
        resolve,
        tables,
        parameters,
    )?)
}

pub(super) fn infer(
    name: &str,
    args: &[Scalar],
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    let call = decode_arguments(name, args.len())?;
    if call.uses_positional_lookup() {
        return Scalar::Call(call.name.into(), args.to_vec())
            .infer(None, parameters, resolve, tables);
    }
    if let Some(layout) = call.layouts().next() {
        if random_uuid::known(call.name) {
            let positional = layout.arguments(args)?;
            return match random_uuid::infer(call.name, &positional, parameters, resolve, tables) {
                Err(error @ SqlError::AmbiguousFunction) => {
                    Err(call.error(error, args, resolve, tables, parameters)?)
                }
                Err(SqlError::UndefinedFunction | SqlError::TextSearchUndefinedFunction) => {
                    Err(call.error(
                        SqlError::UndefinedFunction,
                        args,
                        resolve,
                        tables,
                        parameters,
                    )?)
                }
                result => result,
            };
        }
        if text_search::known(call.name) {
            let positional = layout.arguments(args)?;
            let first = parser_type(call.names.iter().copied());
            return match text_search::infer(
                call.name,
                first.as_ref(),
                &positional,
                parameters,
                resolve,
                tables,
            ) {
                Err(SqlError::UndefinedFunction | SqlError::TextSearchUndefinedFunction) => {
                    Err(call.error(
                        SqlError::UndefinedFunction,
                        args,
                        resolve,
                        tables,
                        parameters,
                    )?)
                }
                Err(error @ SqlError::AmbiguousFunction) => {
                    Err(call.error(error, args, resolve, tables, parameters)?)
                }
                result => result,
            };
        }
        for (slot, (_, expected)) in layout.slots.iter().zip(layout.signature.inputs) {
            if let Some(i) = *slot {
                let hint = args[i]
                    .aggregate_unknown(parameters)
                    .then_some(expected.as_ref())
                    .flatten();
                args[i].infer(hint, parameters, resolve, tables)?;
            }
        }
        return Ok(());
    }
    for arg in args {
        arg.infer(None, parameters, resolve, tables)?;
    }
    Err(call.error(
        SqlError::UndefinedFunction,
        args,
        resolve,
        tables,
        parameters,
    )?)
}

#[derive(Clone, Debug)]
pub(in crate::relational::query) struct SourceLayout {
    names: Names,
    // Each slot indexes an argument in source order. All supported FROM
    // functions with named inputs have complete signatures.
    slots: Vec<usize>,
}

impl SourceLayout {
    pub(in crate::relational::query) fn new(name: &str, names: Names, args: &[Scalar]) -> R<Self> {
        let layout = names.call(name).layouts().next();
        if let Some(layout) = layout {
            let slots = layout
                .slots
                .into_iter()
                .map(|slot| slot.ok_or(SqlError::InternalError))
                .collect::<R<Vec<_>>>()?;
            return Ok(Self { names, slots });
        }
        Err(names.undefined_source(name, args)?)
    }

    pub(in crate::relational::query) fn parser_type(&self) -> Option<DataType> {
        parser_type(self.names.names.iter().map(String::as_str))
    }

    pub(in crate::relational::query) fn arguments(&self, values: Vec<Scalar>) -> R<Vec<Scalar>> {
        let mut values = values.into_iter().map(Some).collect::<Vec<_>>();
        self.slots
            .iter()
            .map(|&i| {
                values
                    .get_mut(i)
                    .and_then(Option::take)
                    .ok_or(SqlError::InternalError)
            })
            .collect()
    }

    pub(in crate::relational::query) fn tokens(&self, tokens: &[Token]) -> Vec<Token> {
        let mut positional = Vec::new();
        for &slot in &self.slots {
            if !positional.is_empty() {
                positional.push(Token::Symbol(','));
            }
            positional.extend_from_slice(&tokens[self.names.values[slot].clone()]);
        }
        positional
    }

    pub(in crate::relational::query) fn error(
        &self,
        name: &str,
        args: &[Scalar],
        parameters: &[Option<DataType>],
    ) -> R<SqlError> {
        let mut original = vec![None; args.len()];
        for (&source, value) in self.slots.iter().zip(args) {
            original[source] = Some(value);
        }
        let no_columns = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
        diagnostic(
            name,
            original.into_iter().flatten(),
            self.names.call(name).labels(),
            SqlError::UndefinedFunction,
            &no_columns,
            &[],
            parameters,
        )
    }
}

impl Names {
    pub(in crate::relational::query) fn undefined_source(
        &self,
        name: &str,
        args: &[Scalar],
    ) -> R<SqlError> {
        let no_columns = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
        self.call(name)
            .error(SqlError::UndefinedFunction, args, &no_columns, &[], &[])
    }

    fn call<'a>(&'a self, name: &'a str) -> Call<'a> {
        Call {
            name,
            positional: self.positional,
            names: self.names.iter().map(String::as_str).collect(),
            variadic: self.variadic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_call_names_return_internal_errors() {
        let resolve = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
        for invalid in [
            "\0named-call:13:0:0:make_interval",
            "\0named-call:13:0:0:make_intervaldays",
            "\0named-call:13:0:2:make_intervaldays",
            "\0named-call:bad:0:0:make_intervaldays",
        ] {
            assert!(matches!(
                bind(invalid, &[], &resolve, &[], &[]),
                Err(SqlError::InternalError)
            ));
            assert_eq!(
                infer(invalid, &[], &mut [], &resolve, &[]),
                Err(SqlError::InternalError)
            );
        }
        assert!(decode("\0named-call:13:0:0:make_interval").is_none());
    }
}
