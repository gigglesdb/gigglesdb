//! Typed scalar projection binding. No SQL generation or catalog emulation.
use super::*;
mod binary;
mod character;
mod degree_math;
#[cfg(test)]
mod integer_cast_tests;
mod integer_formats;
mod md5;
mod numeric;
mod overlay;
#[cfg(test)]
mod overlay_tests;
mod planning;
mod position;
mod quoting;
mod substring_order;
mod temporal;
mod text;
#[derive(Debug, Clone)]
pub(super) enum Scalar {
    Param(usize),
    Cast(Box<Scalar>, DataType),
    Column(ColumnRef),
    Literal(Value),
    Call(String, Vec<Scalar>),
    SubstringOrder([Box<Scalar>; 3]),
}
pub(super) struct BoundScalar {
    pub(super) ty: DataType,
    node: Node,
}
enum Node {
    Param(usize),
    Cast(Box<BoundScalar>),
    Column(BoundColumn, String),
    Literal(Value),
    Call(String, Vec<BoundScalar>),
}
fn number(t: &DataType) -> bool {
    integer(t)
        || matches!(
            t,
            DataType::Numeric | DataType::Real | DataType::DoublePrecision
        )
}
fn integer(t: &DataType) -> bool {
    matches!(t, DataType::SmallInt | DataType::Integer | DataType::BigInt)
}
impl Scalar {
    pub(super) fn aggregate_unknown(&self, parameters: &[Option<DataType>]) -> bool {
        match self {
            Self::Literal(Value::Null | Value::Text(_)) => true,
            Self::Param(n) => parameters.get(n - 1).is_none_or(Option::is_none),
            _ => false,
        }
    }
    pub(super) fn reject_nested_aggregate(&self) -> R<()> {
        match self {
            Self::Call(name, _) if matches!(name.as_str(), "count" | "sum" | "min" | "max") => {
                Err(SqlError::GroupingError)
            }
            Self::Call(_, args) => args.iter().try_for_each(Self::reject_nested_aggregate),
            Self::SubstringOrder(args) => args.iter().try_for_each(|a| a.reject_nested_aggregate()),
            Self::Cast(arg, _) => arg.reject_nested_aggregate(),
            _ => Ok(()),
        }
    }

    pub(super) fn parse(p: &mut Parser, depth: usize) -> R<Self> {
        Self::parse_context(p, depth, false)
    }
    pub(super) fn parse_aggregate_argument(p: &mut Parser, depth: usize) -> R<Self> {
        Self::parse_context(p, depth, true)
    }
    fn parse_context(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        let mut value = Self::primary_context(p, depth, aggregate_argument)?;
        let mut casts = depth;
        while p.sym(':') {
            p.punct(':')?;
            casts += 1;
            if casts > 32 {
                return Err(SqlError::Limit);
            }
            value = Self::Cast(Box::new(value), cast_type(p)?);
        }
        Ok(value)
    }
    // Routine names alone admit quoted identifiers; object names still use Parser::name.
    fn routine_name(p: &mut Parser) -> R<(String, bool)> {
        let result = match p.tokens.get(p.at) {
            Some(Token::Word(n)) => (n.clone(), false),
            Some(Token::QuotedIdent(n)) => (n.clone(), true),
            Some(
                Token::String(_)
                | Token::Number(_)
                | Token::Param(_)
                | Token::Symbol(_)
                | Token::Op(_),
            )
            | None => return Err(SqlError::Syntax),
        };
        p.at += 1;
        Ok(result)
    }
    fn primary_context(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        if depth > 32 {
            return Err(SqlError::Limit);
        }
        if p.word("cast") {
            p.punct('(')?;
            let value = Self::parse_context(p, depth + 1, aggregate_argument)?;
            p.need("as")?;
            let ty = cast_type(p)?;
            p.punct(')')?;
            return Ok(Self::Cast(Box::new(value), ty));
        }
        if p.sym('(') {
            let value = Self::parse_context(p, depth + 1, aggregate_argument)?;
            p.punct(')')?;
            return Ok(value);
        }
        if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if ["date","time","timetz","timestamp","timestamptz","interval"].contains(&w.as_str()))
            && (matches!(p.tokens.get(p.at + 1), Some(Token::String(_)))
                || matches!(p.tokens.get(p.at + 1), Some(Token::Word(w)) if w == "with" || w == "without"))
        {
            let ty = cast_type(p)?;
            let Expr::Literal(v) = p.expr()? else {
                return Err(SqlError::Syntax);
            };
            return Ok(Self::Cast(Box::new(Self::Literal(v)), ty));
        }
        if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if !["null", "true", "false"].contains(&w.as_str()))
            || matches!(p.tokens.get(p.at), Some(Token::QuotedIdent(_)))
        {
            let qualified_call = p.tokens.get(p.at + 1) == Some(&Token::Symbol('.'))
                && matches!(
                    p.tokens.get(p.at + 2),
                    Some(Token::Word(_) | Token::QuotedIdent(_))
                )
                && p.tokens.get(p.at + 3) == Some(&Token::Symbol('('));
            if p.tokens.get(p.at + 1) == Some(&Token::Symbol('(')) || qualified_call {
                let (name, quoted) = Self::routine_name(p)?;
                let (name, quoted) = if qualified_call {
                    p.punct('.')?;
                    // Fixed builtin namespace only; exact quoted case is preserved.
                    if name != "pg_catalog" {
                        return Err(SqlError::Unsupported);
                    }
                    Self::routine_name(p)?
                } else {
                    (name, quoted)
                };
                // Quoted/qualified COALESCE names denote routines, never SQL syntax.
                if (qualified_call || quoted) && name == "coalesce" {
                    return Err(SqlError::Unsupported);
                }
                p.punct('(')?;
                if name == "position" && !qualified_call && !quoted {
                    if matches!(p.tokens.get(p.at), None | Some(Token::Symbol(')' | ',' | '*'))) {
                        return Err(SqlError::Syntax);
                    }
                    let needle = Self::parse_context(p, depth + 1, aggregate_argument)?;
                    p.need("in")?;
                    if matches!(p.tokens.get(p.at), None | Some(Token::Symbol(')' | ',' | '*'))) {
                        return Err(SqlError::Syntax);
                    }
                    let source = Self::parse_context(p, depth + 1, aggregate_argument)?;
                    p.punct(')')?;
                    return Ok(Self::Call(name, vec![source, needle]));
                }
                if aggregate_argument && p.sym('*') {
                    p.punct(')')?;
                    if name != "count" {
                        return Err(SqlError::UndefinedFunction);
                    }
                    // Binder's existing nested-aggregate classifier rejects this marker.
                    return Ok(Self::Call(name, vec![Self::Literal(Value::Null)]));
                }
                if name == "trim" && !qualified_call && !quoted {
                    return Self::parse_trim_construct(p, depth, aggregate_argument);
                }
                let mut args = vec![];
                if name == "extract" && !qualified_call && !quoted {
                    let field = match p.tokens.get(p.at).cloned() {
                        Some(Token::Word(f) | Token::String(f)) => f,
                        _ => return Err(SqlError::Syntax),
                    };
                    p.at += 1;
                    p.need("from")?;
                    let value = Self::parse_context(p, depth + 1, aggregate_argument)?;
                    p.punct(')')?;
                    return Ok(Self::Call(
                        name,
                        vec![Self::Literal(Value::Text(field)), value],
                    ));
                }

                if !p.sym(')') {
                    loop {
                        if args.len() >= MAX_COLUMNS {
                            return Err(SqlError::Limit);
                        }
                        args.push(Self::parse_context(p, depth + 1, aggregate_argument)?);
                        if !qualified_call
                            && !quoted
                            && name == "overlay"
                            && args.len() == 1
                            && p.word("placing")
                        {
                            args.push(Self::parse_overlay_argument(
                                p,
                                depth + 1,
                                aggregate_argument,
                            )?);
                            p.need("from")?;
                            args.push(Self::parse_overlay_argument(
                                p,
                                depth + 1,
                                aggregate_argument,
                            )?);
                            if p.word("for") {
                                args.push(Self::parse_overlay_argument(
                                    p,
                                    depth + 1,
                                    aggregate_argument,
                                )?);
                            }
                            p.punct(')')?;
                            break;
                        }

                        if !qualified_call
                            && !quoted
                            && name == "substring"
                            && args.len() == 1
                            && (p.word("from") || p.word("for"))
                        {
                            let from = matches!(p.tokens.get(p.at - 1), Some(Token::Word(w)) if w == "from");
                            if !from {
                                args.push(Self::Literal(Value::Int(1)));
                            }
                            args.push(Self::parse_context(p, depth + 1, aggregate_argument)?);
                            if from && p.word("for") {
                                args.push(Self::parse_context(p, depth + 1, aggregate_argument)?);
                            }
                            if !from && p.word("from") {
                                if matches!(
                                    p.tokens.get(p.at),
                                    None | Some(Token::Symbol(')' | ','))
                                ) {
                                    return Err(SqlError::Syntax);
                                }
                                let start = Self::parse_context(p, depth + 1, aggregate_argument)?;
                                p.punct(')')?;
                                let length = args.pop().ok_or(SqlError::Syntax)?;
                                args.pop().ok_or(SqlError::Syntax)?;
                                let source = args.pop().ok_or(SqlError::Syntax)?;
                                return Ok(Self::SubstringOrder([
                                    Box::new(source),
                                    Box::new(start),
                                    Box::new(length),
                                ]));
                            }
                            p.punct(')')?;
                            break;
                        }
                        if !p.sym(',') {
                            p.punct(')')?;
                            break;
                        }
                    }
                }
                Ok(Self::Call(name, args))
            } else {
                Ok(Self::Column(p.column_ref()?))
            }
        } else {
            match p.expr()? {
                Expr::Literal(v) => Ok(Self::Literal(v)),
                Expr::Param(n) => Ok(Self::Param(n)),
            }
        }
    }
    fn parse_overlay_argument(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        if matches!(p.tokens.get(p.at), None | Some(Token::Symbol(')' | ',')))
            || matches!(p.tokens.get(p.at), Some(Token::Word(word)) if matches!(word.as_str(), "from" | "for" | "placing"))
        {
            return Err(SqlError::Syntax);
        }
        Self::parse_context(p, depth, aggregate_argument)
    }
    fn parse_trim_construct(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        let name = if p.word("leading") {
            "ltrim"
        } else if p.word("trailing") {
            "rtrim"
        } else {
            p.word("both");
            "btrim"
        };
        let argument = |p: &mut Parser| {
            if matches!(p.tokens.get(p.at), None | Some(Token::Symbol(')' | ',')))
                || matches!(p.tokens.get(p.at), Some(Token::Word(word)) if matches!(word.as_str(), "from" | "both" | "leading" | "trailing"))
            {
                return Err(SqlError::Syntax);
            }
            Self::parse_context(p, depth + 1, aggregate_argument)
        };
        let mut characters = None;
        let mut args = Vec::new();
        if !p.word("from") {
            let first = argument(p)?;
            if p.word("from") {
                characters = Some(first);
            } else {
                args.push(first);
            }
        }
        if args.is_empty() {
            args.push(argument(p)?);
        }
        while p.sym(',') {
            // The character expression occupies the last normalized argument.
            if args.len() + usize::from(characters.is_some()) >= MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            args.push(argument(p)?);
        }
        p.punct(')')?;
        if let Some(characters) = characters {
            args.push(characters);
        }
        Ok(Self::Call(name.into(), args))
    }

    pub(super) fn bind(
        &self,
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        parameters: &[Option<DataType>],
    ) -> R<BoundScalar> {
        let (ty, node) = match self {
            Self::Param(n) => (
                parameters[*n - 1].clone().ok_or(SqlError::Parameters)?,
                Node::Param(*n),
            ),
            Self::Cast(a, ty) => {
                if let Self::Literal(Value::Text(_) | Value::Null) = a.as_ref() {
                    let Self::Literal(v) = a.as_ref() else {
                        unreachable!()
                    };
                    let v = cast_value(v.clone(), ty, &mut 0)?;
                    return Ok(BoundScalar {
                        ty: ty.clone(),
                        node: Node::Cast(Box::new(BoundScalar {
                            ty: ty.clone(),
                            node: Node::Literal(v),
                        })),
                    });
                }
                let a = a.bind(resolve, tables, parameters)?;
                if !(number(&a.ty) && number(ty)
                    || a.ty == *ty
                    || integer(&a.ty) && (integer(ty) || *ty == DataType::Bytes)
                    || a.ty == DataType::Bytes && integer(ty)
                    || a.ty == DataType::Text
                    || *ty == DataType::Text
                    || a.ty == DataType::Boolean && *ty == DataType::Integer
                    || a.ty == DataType::Integer && *ty == DataType::Boolean)
                {
                    return Err(SqlError::Type);
                }
                (ty.clone(), Node::Cast(Box::new(a)))
            }
            Self::Column(c) => {
                let c = resolve(c, tables.len())?;
                (
                    tables[c.source].columns[c.column].data_type.clone(),
                    Node::Column(c, tables[c.source].columns[c.column].name.clone()),
                )
            }
            Self::Literal(v) => (
                match v {
                    Value::Int(n) if i32::try_from(*n).is_ok() => DataType::Integer,
                    Value::Int(_) => DataType::BigInt,
                    Value::Bool(_) => DataType::Boolean,
                    Value::Bytes(_) => DataType::Bytes,
                    Value::Numeric(_) => DataType::Numeric,
                    Value::Float4(_) => DataType::Real,
                    Value::Float8(_) => DataType::DoublePrecision,
                    Value::Date(_) => DataType::Date,
                    Value::Time(_) => DataType::Time,
                    Value::TimeTz(_) => DataType::TimeTz,
                    Value::Timestamp(_) => DataType::Timestamp,
                    Value::TimestampTz(_) => DataType::TimestampTz,
                    Value::Interval(_) => DataType::Interval,

                    _ => DataType::Text,
                },
                Node::Literal(v.clone()),
            ),
            Self::SubstringOrder(args) => {
                return Self::bind_substring_order(args, resolve, tables, parameters);
            }
            Self::Call(name, args) if name == "overlay" => {
                return Self::bind_overlay(args, resolve, tables, parameters);
            }
            Self::Call(name, args) => {
                if name == "position" {
                    return Self::bind_position(args, resolve, tables, parameters);
                }
                let original = args;
                if integer_formats::known_name(name)
                    && args.len() == 1
                    && args[0].aggregate_unknown(parameters)
                {
                    return Err(SqlError::AmbiguousFunction);
                }
                let mut args = args
                    .iter()
                    .map(|a| a.bind(resolve, tables, parameters))
                    .collect::<R<Vec<_>>>()?;
                if let Some(arity) = degree_math::arity(name)
                    && (args.len() != arity
                        || args.iter().zip(original).any(|(a, source)| {
                            !number(&a.ty)
                                && !matches!(source, Self::Literal(Value::Text(_) | Value::Null))
                        }))
                {
                    return Err(SqlError::UndefinedFunction);
                }
                let null = |a: &BoundScalar| matches!(a.node, Node::Literal(Value::Null));
                let ty = if integer_formats::known_name(name) {
                    if args.len() != 1 {
                        return Err(SqlError::UndefinedFunction);
                    }
                    integer_formats::argument_type(Some(&args[0].ty))?;
                    DataType::Text
                } else if let Some(result) = binary::signature(
                    name,
                    args.len(),
                    args.iter()
                        .enumerate()
                        .find(|(i, a)| {
                            (*i == 0 || matches!(name.as_str(), "btrim" | "ltrim" | "rtrim"))
                                && a.ty == DataType::Bytes
                        })
                        .map(|(_, a)| &a.ty)
                        .or_else(|| args.first().map(|a| &a.ty)),
                )
                .or_else(|| md5::signature(name, args.len(), args.first().map(|a| &a.ty)))
                .or_else(|| character::signature(name, args.len()))
                .or_else(|| quoting::signature(name, args.len(), args.first().map(|a| &a.ty)))
                .or_else(|| text::signature(name, args.len()))
                .or_else(|| temporal::signature(name, &args))
                {
                    let (types, result) = result?;
                    for ((a, original), ty) in args.iter_mut().zip(original).zip(types) {
                        if let Some(ty) = ty {
                            if (name == "octet_length" || name == "bit_length")
                                && a.ty == DataType::Bytes
                            {
                                continue;
                            }
                            if let Self::Literal(Value::Text(s)) = original
                                && ty != DataType::Text
                            {
                                a.node =
                                    Node::Literal(cast_value(Value::Text(s.clone()), &ty, &mut 0)?);
                                a.ty = ty.clone();
                            }
                            if null(a) {
                                a.ty = ty.clone();
                            }
                            if a.ty != ty
                                && !(integer(&a.ty)
                                    && (ty == DataType::BigInt
                                        || ty == DataType::Integer && a.ty == DataType::SmallInt))
                            {
                                return Err(
                                    if temporal::known_name(name)
                                        || character::known_name(name)
                                        || name == "md5"
                                    {
                                        SqlError::UndefinedFunction
                                    } else {
                                        SqlError::Type
                                    },
                                );
                            }
                        }
                    }
                    result
                } else {
                    match name.as_str() {
                        "pi" if args.is_empty() => DataType::DoublePrecision,
                        n if matches!(n, "sqrt" | "exp" | "ln" | "log" | "log10")
                            && args.len() == 1
                            && args[0].ty == DataType::Numeric =>
                        {
                            DataType::Numeric
                        }
                        n if args.len() == 2
                            && (n == "log"
                                || n == "power"
                                    && args.iter().any(|a| a.ty == DataType::Numeric)
                                    && args.iter().all(|a| {
                                        integer(&a.ty)
                                            || matches!(a.ty, DataType::Numeric | DataType::Text)
                                    })) =>
                        {
                            for (i, a) in args.iter_mut().enumerate() {
                                if !(integer(&a.ty)
                                    || a.ty == DataType::Numeric
                                    || matches!(
                                        original[i],
                                        Self::Literal(Value::Text(_) | Value::Null)
                                    ))
                                {
                                    return Err(SqlError::Type);
                                }
                                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[i]
                                {
                                    *a = BoundScalar {
                                        ty: DataType::Numeric,
                                        node: Node::Literal(cast_value(
                                            v.clone(),
                                            &DataType::Numeric,
                                            &mut 0,
                                        )?),
                                    };
                                    continue;
                                }
                                let old = std::mem::replace(
                                    a,
                                    BoundScalar {
                                        ty: DataType::Numeric,
                                        node: Node::Literal(Value::Null),
                                    },
                                );
                                *a = BoundScalar {
                                    ty: DataType::Numeric,
                                    node: Node::Cast(Box::new(old)),
                                };
                            }
                            DataType::Numeric
                        }
                        n if numeric::float_arity(n).is_some_and(|arity| arity == args.len()) => {
                            for (i, a) in args.iter_mut().enumerate() {
                                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[i]
                                {
                                    *a = BoundScalar {
                                        ty: DataType::DoublePrecision,
                                        node: Node::Literal(cast_value(
                                            v.clone(),
                                            &DataType::DoublePrecision,
                                            &mut 0,
                                        )?),
                                    };
                                } else {
                                    if !number(&a.ty) {
                                        return Err(SqlError::Type);
                                    }
                                    let old = std::mem::replace(
                                        a,
                                        BoundScalar {
                                            ty: DataType::DoublePrecision,
                                            node: Node::Literal(Value::Null),
                                        },
                                    );
                                    a.node = Node::Cast(Box::new(old));
                                }
                            }
                            DataType::DoublePrecision
                        }
                        "scale" | "min_scale" | "trim_scale" | "factorial" if args.len() == 1 => {
                            let target = if name == "factorial" {
                                DataType::BigInt
                            } else {
                                DataType::Numeric
                            };
                            if let Self::Literal(v @ (Value::Text(_) | Value::Null)) = &original[0]
                            {
                                args[0] = BoundScalar {
                                    ty: target.clone(),
                                    node: Node::Literal(cast_value(v.clone(), &target, &mut 0)?),
                                };
                            } else {
                                if !(integer(&args[0].ty) || args[0].ty == target) {
                                    return Err(SqlError::Type);
                                }
                                let old = args.remove(0);
                                args.push(BoundScalar {
                                    ty: target.clone(),
                                    node: Node::Cast(Box::new(old)),
                                });
                            }
                            if matches!(name.as_str(), "scale" | "min_scale") {
                                DataType::Integer
                            } else {
                                DataType::Numeric
                            }
                        }
                        "div" | "mod" | "gcd" | "lcm" if args.len() == 2 => {
                            if name != "div"
                                && original.iter().all(|a| {
                                    matches!(
                                        a,
                                        Self::Literal(Value::Text(_) | Value::Null)
                                            | Self::Param(_)
                                    )
                                })
                            {
                                return Err(SqlError::Type);
                            }
                            if matches!(name.as_str(), "gcd" | "lcm")
                                && args.iter().all(|a| {
                                    !matches!(
                                        a.ty,
                                        DataType::Integer | DataType::BigInt | DataType::Numeric
                                    )
                                })
                            {
                                return Err(SqlError::Type);
                            }
                            let target = if name == "div"
                                || args.iter().any(|a| a.ty == DataType::Numeric)
                            {
                                DataType::Numeric
                            } else if name == "mod"
                                && args.iter().all(|a| {
                                    !matches!(
                                        a.ty,
                                        DataType::Integer | DataType::BigInt | DataType::Numeric
                                    )
                                })
                            {
                                DataType::SmallInt
                            } else if args.iter().any(|a| a.ty == DataType::BigInt) {
                                DataType::BigInt
                            } else {
                                DataType::Integer
                            };
                            for (i, a) in args.iter_mut().enumerate() {
                                if !integer(&a.ty)
                                    && a.ty != DataType::Numeric
                                    && !matches!(
                                        original[i],
                                        Self::Literal(Value::Text(_) | Value::Null)
                                    )
                                {
                                    return Err(SqlError::Type);
                                }
                                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[i]
                                {
                                    a.node = Node::Literal(cast_value(v.clone(), &target, &mut 0)?);
                                    a.ty = target.clone();
                                    continue;
                                }
                                let old = std::mem::replace(
                                    a,
                                    BoundScalar {
                                        ty: target.clone(),
                                        node: Node::Literal(Value::Null),
                                    },
                                );
                                a.ty = target.clone();
                                a.node = Node::Cast(Box::new(old));
                            }
                            target
                        }
                        "round" | "ceil" | "ceiling" | "floor" | "trunc" | "sign"
                            if args.len() == 1
                                || (matches!(name.as_str(), "round" | "trunc")
                                    && args.len() == 2) =>
                        {
                            let target = if args.len() == 2 || args[0].ty == DataType::Numeric {
                                DataType::Numeric
                            } else {
                                DataType::DoublePrecision
                            };
                            if args.len() == 2
                                && let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[1]
                            {
                                args[1] = BoundScalar {
                                    ty: DataType::Integer,
                                    node: Node::Literal(cast_value(
                                        v.clone(),
                                        &DataType::Integer,
                                        &mut 0,
                                    )?),
                                };
                            }
                            if args.len() == 2
                                && ((!integer(&args[1].ty) || args[1].ty == DataType::BigInt)
                                    || matches!(
                                        args[0].ty,
                                        DataType::Real | DataType::DoublePrecision
                                    ))
                            {
                                return Err(SqlError::Type);
                            }
                            if args[0].ty != target {
                                if !number(&args[0].ty)
                                    && !matches!(
                                        original[0],
                                        Self::Literal(Value::Text(_) | Value::Null)
                                    )
                                {
                                    return Err(SqlError::Type);
                                }
                                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[0]
                                {
                                    args[0].node =
                                        Node::Literal(cast_value(v.clone(), &target, &mut 0)?);
                                    args[0].ty = target.clone();
                                }
                                let old = std::mem::replace(
                                    &mut args[0],
                                    BoundScalar {
                                        ty: target.clone(),
                                        node: Node::Literal(Value::Null),
                                    },
                                );
                                args[0] = BoundScalar {
                                    ty: target,
                                    node: Node::Cast(Box::new(old)),
                                };
                            }
                            args[0].ty.clone()
                        }
                        "abs" if args.len() == 1 => {
                            if null(&args[0])
                                || matches!(&self, Self::Call(_, a) if matches!(a[0], Self::Literal(Value::Text(_))))
                            {
                                let v = if let Self::Literal(v) = &original[0] {
                                    v.clone()
                                } else {
                                    Value::Null
                                };
                                args[0].node = Node::Literal(cast_value(
                                    v,
                                    &DataType::DoublePrecision,
                                    &mut 0,
                                )?);
                                args[0].ty = DataType::DoublePrecision;
                            }
                            if !number(&args[0].ty) {
                                return Err(SqlError::Type);
                            }
                            args[0].ty.clone()
                        }
                        "length" | "lower" | "upper" if args.len() == 1 => {
                            if !null(&args[0]) && args[0].ty != DataType::Text {
                                return Err(SqlError::Type);
                            }
                            if name == "length" {
                                DataType::Integer
                            } else {
                                DataType::Text
                            }
                        }
                        "substring" | "substr" if (2..=3).contains(&args.len()) => {
                            for (i, a) in args.iter_mut().enumerate() {
                                let ty = if i == 0 {
                                    DataType::Text
                                } else {
                                    DataType::Integer
                                };
                                if let Self::Literal(Value::Text(s)) = &original[i] {
                                    a.node = Node::Literal(cast_value(
                                        Value::Text(s.clone()),
                                        &ty,
                                        &mut 0,
                                    )?);
                                    a.ty = ty.clone();
                                }
                                if null(a) {
                                    a.ty = ty.clone();
                                }
                                if a.ty != ty {
                                    return Err(SqlError::Type);
                                }
                            }
                            DataType::Text
                        }
                        "coalesce" if !args.is_empty() => {
                            let mut ty: Option<DataType> = None;
                            for (a, original) in args.iter().zip(original) {
                                if matches!(original, Self::Literal(Value::Text(_))) {
                                    continue;
                                }
                                if null(a) {
                                    continue;
                                }
                                ty = Some(match ty {
                                    None => a.ty.clone(),
                                    Some(ref t) if t == &a.ty => t.clone(),
                                    Some(ref t) if integer(t) && integer(&a.ty) => {
                                        if *t == DataType::BigInt || a.ty == DataType::BigInt {
                                            DataType::BigInt
                                        } else {
                                            DataType::Integer
                                        }
                                    }
                                    Some(ref t) if number(t) && number(&a.ty) => {
                                        if matches!(t, DataType::DoublePrecision | DataType::Real)
                                            || matches!(
                                                a.ty,
                                                DataType::DoublePrecision | DataType::Real
                                            )
                                        {
                                            DataType::DoublePrecision
                                        } else {
                                            DataType::Numeric
                                        }
                                    }
                                    _ => return Err(SqlError::Type),
                                });
                            }
                            let ty = ty.unwrap_or(DataType::Text);
                            for (a, original) in args.iter_mut().zip(original) {
                                if let Self::Literal(Value::Text(s)) = original {
                                    a.node = Node::Literal(cast_value(
                                        Value::Text(s.clone()),
                                        &ty,
                                        &mut 0,
                                    )?);
                                    a.ty = ty.clone();
                                }
                                if null(a) {
                                    a.ty = ty.clone();
                                }
                            }
                            ty
                        }
                        "abs" | "length" | "lower" | "upper" | "substring" | "substr"
                        | "coalesce" => {
                            return Err(SqlError::Type);
                        }
                        _ => return Err(SqlError::Unsupported),
                    }
                };
                (ty, Node::Call(name.clone(), args))
            }
        };
        Ok(BoundScalar { ty, node })
    }
}
impl BoundScalar {
    fn strong_name(&self) -> Option<&str> {
        match &self.node {
            Node::Column(_, name) | Node::Call(name, _) => Some(name),
            Node::Cast(a) => a.strong_name(),
            _ => None,
        }
    }
    pub(super) fn name(&self) -> &str {
        match &self.node {
            Node::Param(_) => "?column?",
            Node::Cast(a) => {
                if let Some(name) = a.strong_name() {
                    name
                } else {
                    match self.ty {
                        DataType::SmallInt => "int2",
                        DataType::Integer => "int4",
                        DataType::BigInt => "int8",
                        DataType::Text => "text",
                        DataType::Boolean => "bool",
                        DataType::Bytes => "bytea",
                        DataType::Numeric => "numeric",
                        DataType::Real => "float4",
                        DataType::DoublePrecision => "float8",
                        DataType::Date => "date",
                        DataType::Time => "time",
                        DataType::TimeTz => "timetz",
                        DataType::Timestamp => "timestamp",
                        DataType::TimestampTz => "timestamptz",
                        DataType::Interval => "interval",
                    }
                }
            }
            Node::Column(_, name) => name,
            Node::Literal(_) => "?column?",
            Node::Call(n, _) => n,
        }
    }
    pub(super) fn eval(&self, row: &Tuple, params: &[Value], budget: &mut usize) -> R<Value> {
        match &self.node {
            Node::Param(n) => {
                let v = params.get(n - 1).ok_or(SqlError::Parameters)?;
                charge(budget, v)?;
                Ok(v.clone())
            }
            Node::Cast(a) => {
                let v = a.eval(row, params, budget)?;
                if let Value::Int(n) = v
                    && self.ty == DataType::Bytes
                {
                    let bytes = match a.ty {
                        DataType::SmallInt => (n as i16).to_be_bytes().to_vec(),
                        DataType::Integer => (n as i32).to_be_bytes().to_vec(),
                        DataType::BigInt => n.to_be_bytes().to_vec(),
                        _ => return Err(SqlError::Type),
                    };
                    charge(budget, &Value::Bytes(bytes.clone()))?;
                    return Ok(Value::Bytes(bytes));
                }
                cast_value(v, &self.ty, budget)
            }
            Node::Column(c, _) => {
                let v = row.get(*c);
                charge(budget, v)?;
                Ok(v.clone())
            }
            Node::Literal(v) => {
                charge(budget, v)?;
                Ok(v.clone())
            }
            Node::Call(n, args) => {
                if n == "coalesce" {
                    for a in args {
                        let v = a.eval(row, params, budget)?;
                        if v != Value::Null {
                            return cast_value(v, &self.ty, budget);
                        }
                    }
                    return Ok(Value::Null);
                }
                let values = args
                    .iter()
                    .map(|a| a.eval(row, params, budget))
                    .collect::<R<Vec<_>>>()?;
                if n != "concat"
                    && n != "concat_ws"
                    && n != "quote_nullable"
                    && values.contains(&Value::Null)
                {
                    return Ok(Value::Null);
                }
                if let Some(result) = integer_formats::eval(
                    n,
                    &values,
                    args.first().map(|argument| &argument.ty),
                    budget,
                ) {
                    return result;
                }
                if n == "overlay" {
                    return overlay::eval(&values, budget);
                }
                if let Some(result) = position::eval(n, &values, budget)
                    .or_else(|| binary::eval(n, &values, budget))
                    .or_else(|| md5::eval(n, &values, budget))
                    .or_else(|| character::eval(n, &values, budget))
                    .or_else(|| quoting::eval(n, &values, budget))
                    .or_else(|| text::eval(n, &values, budget))
                    .or_else(|| temporal::eval(n, &values, budget))
                {
                    return result;
                }
                if values.contains(&Value::Null) {
                    return Ok(Value::Null);
                }
                if let Some(v) = numeric::eval(n, &values).or_else(|| numeric_function(n, &values))
                {
                    let v = cast_value(v?, &self.ty, budget)?;
                    charge(budget, &v)?;
                    return Ok(v);
                }
                let v = match (n.as_str(), &values[0]) {
                    ("abs", Value::Numeric(n)) => Value::Numeric(
                        Numeric::parse(n.to_string().trim_start_matches('-')).map_err(pg_error)?,
                    ),
                    ("abs", Value::Float4(n)) => {
                        Value::Float4(Float4::from_bits(n.value().abs().to_bits()))
                    }
                    ("abs", Value::Float8(n)) => {
                        Value::Float8(Float8::from_bits(n.value().abs().to_bits()))
                    }
                    ("abs", Value::Int(v)) => {
                        let v = v.checked_abs().ok_or(SqlError::Overflow)?;
                        if (self.ty == DataType::SmallInt && i16::try_from(v).is_err())
                            || (self.ty == DataType::Integer && i32::try_from(v).is_err())
                        {
                            return Err(SqlError::Overflow);
                        }
                        Value::Int(v)
                    }
                    ("length", Value::Text(s)) => Value::Int(
                        i32::try_from(s.chars().count()).map_err(|_| SqlError::Overflow)? as i64,
                    ),
                    ("lower" | "upper", Value::Text(s)) => {
                        // PostgreSQL 18.0 libc C locale only changes ASCII case.
                        reserve(budget, s.len())?;
                        return Ok(Value::Text(if n == "lower" {
                            s.to_ascii_lowercase()
                        } else {
                            s.to_ascii_uppercase()
                        }));
                    }
                    ("substring" | "substr", Value::Text(s)) => {
                        let Value::Int(start) = values[1] else {
                            return Err(SqlError::Type);
                        };
                        let end = if values.len() == 3 {
                            let Value::Int(len) = values[2] else {
                                return Err(SqlError::Type);
                            };
                            if len < 0 {
                                return Err(SqlError::SubstringError);
                            }
                            start + len
                        } else {
                            i64::MAX
                        };
                        let lo = (start.max(1) - 1) as usize;
                        let hi = (end.max(1) - 1) as usize;
                        let start = s.char_indices().nth(lo).map_or(s.len(), |(i, _)| i);
                        let end = s.char_indices().nth(hi).map_or(s.len(), |(i, _)| i);
                        let out = &s[start..end.max(start)];
                        reserve(budget, out.len())?;
                        return Ok(Value::Text(out.into()));
                    }
                    _ => return Err(SqlError::Type),
                };
                charge(budget, &v)?;
                Ok(v)
            }
        }
    }
}
fn reserve(budget: &mut usize, bytes: usize) -> R<()> {
    let total = budget.checked_add(bytes).ok_or(SqlError::Limit)?;
    if bytes > MAX_ROW_BYTES || total > MAX_RESULT_BYTES {
        return Err(SqlError::Limit);
    }
    *budget = total;
    Ok(())
}

fn cast_type(p: &mut Parser) -> R<DataType> {
    Ok(match p.name()?.as_str() {
        "smallint" | "int2" => DataType::SmallInt,
        "integer" | "int" | "int4" => DataType::Integer,
        "bigint" | "int8" => DataType::BigInt,
        "text" => DataType::Text,
        "boolean" | "bool" => DataType::Boolean,
        "bytea" => DataType::Bytes,
        "numeric" | "decimal" => DataType::Numeric,
        "real" | "float4" => DataType::Real,
        "float8" => DataType::DoublePrecision,
        "date" => DataType::Date,
        "time" => {
            if p.word("with") {
                p.need("time")?;
                p.need("zone")?;
                DataType::TimeTz
            } else {
                if p.word("without") {
                    p.need("time")?;
                    p.need("zone")?;
                }
                DataType::Time
            }
        }
        "timetz" => DataType::TimeTz,
        "timestamp" => {
            if p.word("with") {
                p.need("time")?;
                p.need("zone")?;
                DataType::TimestampTz
            } else {
                if p.word("without") {
                    p.need("time")?;
                    p.need("zone")?;
                }
                DataType::Timestamp
            }
        }
        "timestamptz" => DataType::TimestampTz,
        "interval" => DataType::Interval,

        "double" => {
            p.need("precision")?;
            DataType::DoublePrecision
        }
        _ => return Err(SqlError::Unsupported),
    })
}
fn integer_from_float(value: f64, ty: &DataType) -> R<i64> {
    let exclusive_upper = match ty {
        DataType::SmallInt => 32_768.0,
        DataType::Integer => 2_147_483_648.0,
        DataType::BigInt => 9_223_372_036_854_775_808.0,
        _ => return Err(SqlError::Type),
    };
    let rounded = value.round_ties_even();
    // Powers of two are exact in f64. i64::MAX as f64 is not a valid upper
    // bound: it equals 2^63, which must be excluded; -2^63 remains valid.
    if !rounded.is_finite() || rounded < -exclusive_upper || rounded >= exclusive_upper {
        return Err(SqlError::Overflow);
    }
    Ok(rounded as i64)
}

pub(super) fn cast_value(v: Value, ty: &DataType, budget: &mut usize) -> R<Value> {
    if v == Value::Null {
        return Ok(v);
    }
    let out = match (v, ty) {
        (Value::Date(v), DataType::Date) => Value::Date(v),
        (Value::Date(v), DataType::Text) => Value::Text(v.to_string()),
        (Value::Time(v), DataType::Time) => Value::Time(v),
        (Value::Time(v), DataType::Text) => Value::Text(v.to_string()),
        (Value::TimeTz(v), DataType::TimeTz) => Value::TimeTz(v),
        (Value::TimeTz(v), DataType::Text) => Value::Text(v.to_string()),
        (Value::Timestamp(v), DataType::Timestamp) => Value::Timestamp(v),
        (Value::Timestamp(v), DataType::Text) => Value::Text(v.to_string()),
        (Value::TimestampTz(v), DataType::TimestampTz) => Value::TimestampTz(v),
        (Value::TimestampTz(v), DataType::Text) => Value::Text(v.to_string()),
        (Value::Interval(v), DataType::Interval) => Value::Interval(v),
        (Value::Interval(v), DataType::Text) => Value::Text(v.to_string()),
        (Value::Text(s), DataType::Date) => Value::Date(
            Date::parse(&s, &giggles_pg_types::temporal::ParserSettings::default())
                .map_err(|e| temporal::parse_error(e, false))?,
        ),
        (Value::Text(s), DataType::Time) => {
            Value::Time(Time::parse(&s).map_err(|e| temporal::parse_error(e, false))?)
        }
        (Value::Text(s), DataType::TimeTz) => Value::TimeTz(
            TimeTz::parse(&s, &giggles_pg_types::temporal::ParserSettings::default())
                .map_err(|e| temporal::parse_error(e, false))?,
        ),
        (Value::Text(s), DataType::Timestamp) => Value::Timestamp(
            Timestamp::parse(&s, &giggles_pg_types::temporal::ParserSettings::default())
                .map_err(|e| temporal::parse_error(e, false))?,
        ),
        (Value::Text(s), DataType::TimestampTz) => Value::TimestampTz(
            TimestampTz::parse_in_session(
                &s,
                &giggles_pg_types::temporal::SessionSettings {
                    date_order: giggles_pg_types::temporal::DateOrder::default(),
                    zone: giggles_pg_types::temporal::SessionZone::Fixed(0),
                },
            )
            .map_err(|e| temporal::parse_error(e, false))?,
        ),
        (Value::Text(s), DataType::Interval) => {
            Value::Interval(Interval::parse(&s).map_err(|e| temporal::parse_error(e, true))?)
        }

        (Value::Numeric(n), DataType::Numeric) => Value::Numeric(n),
        (Value::Float4(n), DataType::Real) => Value::Float4(n),
        (Value::Float4(n), DataType::DoublePrecision) => {
            Value::Float8(Float8::from_bits((n.value() as f64).to_bits()))
        }
        (Value::Float8(n), DataType::DoublePrecision) => Value::Float8(n),
        (Value::Numeric(n), DataType::Text) => Value::Text(n.to_string()),
        (Value::Float4(n), DataType::Text) => Value::Text(n.to_text()),
        (Value::Float8(n), DataType::Text) => Value::Text(n.to_text()),
        (Value::Text(s), DataType::Numeric) => {
            Value::Numeric(Numeric::parse(&s).map_err(pg_error)?)
        }
        (Value::Text(s), DataType::Real) => Value::Float4(Float4::parse(&s).map_err(pg_error)?),
        (Value::Text(s), DataType::DoublePrecision) => {
            Value::Float8(Float8::parse(&s).map_err(pg_error)?)
        }
        (Value::Int(n), DataType::Numeric) => {
            Value::Numeric(Numeric::parse(&n.to_string()).map_err(pg_error)?)
        }
        (Value::Int(n), DataType::Real) => {
            Value::Float4(Float4::parse(&n.to_string()).map_err(pg_error)?)
        }
        (Value::Int(n), DataType::DoublePrecision) => {
            Value::Float8(Float8::parse(&n.to_string()).map_err(pg_error)?)
        }
        (Value::Numeric(n), t) if integer(t) => {
            if n.scale().is_none() {
                return Err(SqlError::Unsupported);
            }
            return cast_value(
                Value::Text(n.round(0).map_err(pg_error)?.to_string()),
                t,
                budget,
            );
        }
        (Value::Numeric(n), t) if matches!(t, DataType::Real | DataType::DoublePrecision) => {
            return cast_value(Value::Text(n.to_string()), t, budget);
        }
        (Value::Float4(n), DataType::Numeric) => {
            Value::Numeric(numeric::from_float(n.value() as f64, 5)?)
        }
        (Value::Float8(n), DataType::Numeric) => {
            Value::Numeric(numeric::from_float(n.value(), 14)?)
        }
        (Value::Float4(n), t) if integer(t) => {
            Value::Int(integer_from_float(f64::from(n.value()), t)?)
        }
        (Value::Float8(n), t) if integer(t) => Value::Int(integer_from_float(n.value(), t)?),
        (Value::Float4(n), t) if number(t) => {
            return cast_value(
                Value::Text(if integer(t) {
                    n.value().round_ties_even().to_string()
                } else {
                    n.to_text()
                }),
                t,
                budget,
            );
        }
        (Value::Float8(n), t) if number(t) => {
            return cast_value(
                Value::Text(if integer(t) {
                    n.value().round_ties_even().to_string()
                } else {
                    n.to_text()
                }),
                t,
                budget,
            );
        }
        (Value::Bytes(bytes), t) if integer(t) => {
            let width = match t {
                DataType::SmallInt => 2,
                DataType::Integer => 4,
                _ => 8,
            };
            if bytes.len() > width {
                return Err(SqlError::Overflow);
            }
            let mut raw = [0u8; 8];
            if bytes.len() == width && bytes.first().is_some_and(|b| b & 128 != 0) {
                raw.fill(255);
            }
            raw[8 - bytes.len()..].copy_from_slice(&bytes);
            Value::Int(i64::from_be_bytes(raw))
        }
        (Value::Int(n), t) if integer(t) => {
            if (*t == DataType::SmallInt && i16::try_from(n).is_err())
                || (*t == DataType::Integer && i32::try_from(n).is_err())
            {
                return Err(SqlError::Overflow);
            }
            Value::Int(n)
        }
        (Value::Text(s), t) if integer(t) => {
            let parsed = match t {
                DataType::SmallInt => giggles_pg_types::integer::parse_i16(&s).map(i64::from),
                DataType::Integer => giggles_pg_types::integer::parse_i32(&s).map(i64::from),
                DataType::BigInt => giggles_pg_types::integer::parse_i64(&s),
                _ => return Err(SqlError::Type),
            };
            Value::Int(parsed.map_err(|e| match e {
                giggles_pg_types::integer::IntegerError::Syntax => {
                    SqlError::InvalidTextRepresentation
                }
                giggles_pg_types::integer::IntegerError::Range => SqlError::Overflow,
                giggles_pg_types::integer::IntegerError::Limit => SqlError::Limit,
            })?)
        }
        (Value::Text(s), DataType::Text) => Value::Text(s),
        (Value::Bool(b), DataType::Boolean) => Value::Bool(b),
        (Value::Bytes(b), DataType::Bytes) => Value::Bytes(b),
        (Value::Int(n), DataType::Text) => Value::Text(n.to_string()),
        (Value::Bytes(bytes), DataType::Text) => {
            reserve(
                budget,
                bytes
                    .len()
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(2))
                    .ok_or(SqlError::Limit)?,
            )?;
            let mut s = String::from("\\x");
            use std::fmt::Write;
            for b in bytes {
                write!(s, "{b:02x}").map_err(|_| SqlError::Type)?;
            }
            return Ok(Value::Text(s));
        }
        (Value::Text(s), DataType::Bytes) => {
            use giggles_pg_types::bytea::{ByteaInputError as E, prepare_bytea_text};
            let map_error = |error| match error {
                E::HexInvalidDigit | E::HexOddDigits => SqlError::InvalidParameterValue,
                E::EscapeSyntax => SqlError::InvalidTextRepresentation,
                E::NulInput => SqlError::CharacterNotInRepertoire,
                E::OutputLimit | E::Allocation => SqlError::Limit,
            };
            reserve(budget, s.len())?;
            let plan = prepare_bytea_text(&s, s.len()).map_err(map_error)?;
            return Ok(Value::Bytes(plan.decode().map_err(map_error)?));
        }
        (Value::Bool(b), DataType::Text) => Value::Text(b.to_string()),
        (Value::Bool(b), DataType::Integer) => Value::Int(i64::from(b)),
        (Value::Int(n), DataType::Boolean) => Value::Bool(n != 0),
        (Value::Text(s), DataType::Boolean) => {
            let s = s
                .trim_matches(|c: char| c.is_ascii_whitespace())
                .to_ascii_lowercase();
            let yes = ["true", "yes", "on", "1"]
                .iter()
                .any(|v| !s.is_empty() && v.starts_with(&s));
            let no = ["false", "no", "off", "0"]
                .iter()
                .any(|v| !s.is_empty() && v.starts_with(&s));
            if yes == no {
                return Err(SqlError::Type);
            }
            Value::Bool(yes)
        }
        _ => return Err(SqlError::Unsupported),
    };
    charge(budget, &out)?;
    Ok(out)
}
impl Scalar {
    pub(super) fn infer(
        &self,
        expected: Option<&DataType>,
        parameters: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<()> {
        match self {
            Self::Param(n) => {
                let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                if let Some(ty) = expected {
                    if slot.as_ref().is_some_and(|old| old != ty) {
                        return Err(SqlError::Type);
                    }
                    *slot = Some(ty.clone());
                }
            }
            Self::Cast(a, ty) => {
                // An explicit cast converts the already chosen input type; it
                // constrains a parameter only while that parameter is unknown.
                let expected = if let Self::Param(n) = a.as_ref() {
                    if parameters.get(n - 1).is_some_and(Option::is_some) {
                        None
                    } else {
                        Some(ty)
                    }
                } else {
                    Some(ty)
                };
                a.infer(expected, parameters, resolve, tables)?;
            }
            Self::SubstringOrder(args) => {
                Self::infer_substring_order(args, parameters, resolve, tables)?;
            }
            Self::Call(n, args) if n == "overlay" => {
                Self::infer_overlay(args, parameters, resolve, tables)?;
            }
            Self::Call(n, args) if n == "position" => {
                Self::infer_position(args, parameters, resolve, tables)?;
            }
            Self::Call(n, args) if quoting::known_name(n) && args.len() == 1 => {
                args[0].infer(None, parameters, resolve, tables)?;
                let ty = if args[0].aggregate_unknown(parameters) {
                    None
                } else {
                    Some(args[0].bind(resolve, tables, parameters)?.ty)
                };
                let (types, _) = quoting::signature(n, args.len(), ty.as_ref()).unwrap()?;
                args[0].infer(types[0].as_ref(), parameters, resolve, tables)?;
            }
            Self::Call(n, args) if integer_formats::known_name(n) && args.len() == 1 => {
                args[0].infer(None, parameters, resolve, tables)?;
                let ty = if args[0].aggregate_unknown(parameters) {
                    None
                } else {
                    Some(args[0].bind(resolve, tables, parameters)?.ty)
                };
                integer_formats::argument_type(ty.as_ref())?;
            }
            Self::Call(n, args) => {
                let common = if n == "coalesce" {
                    args.iter().find_map(|a| match a {
                        Self::Cast(_, t) => Some(t.clone()),
                        Self::Literal(Value::Int(v)) => Some(if i32::try_from(*v).is_ok() {
                            DataType::Integer
                        } else {
                            DataType::BigInt
                        }),
                        Self::Literal(Value::Bool(_)) => Some(DataType::Boolean),
                        Self::Literal(Value::Null | Value::Text(_)) => None,
                        _ => a.bind(resolve, tables, parameters).ok().map(|b| b.ty),
                    })
                } else {
                    None
                };
                for (i, a) in args.iter().enumerate() {
                    let first_type = args
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| {
                            *i == 0 || matches!(n.as_str(), "btrim" | "ltrim" | "rtrim")
                        })
                        .filter_map(|(_, a)| a.bind(resolve, tables, parameters).ok().map(|a| a.ty))
                        .find(|t| *t == DataType::Bytes);
                    let ty = if let Some(sig) =
                        binary::signature(n, args.len(), first_type.as_ref())
                            .or_else(|| {
                                md5::signature(n, args.len(), first_type.as_ref())
                                    .filter(|signature| signature.is_ok())
                            })
                            .or_else(|| {
                                character::signature(n, args.len())
                                    .filter(|signature| signature.is_ok())
                            })
                            .or_else(|| text::signature(n, args.len()))
                    {
                        sig?.0[i].clone()
                    } else {
                        match n.as_str() {
                            "date_trunc"
                                if args.len() == 3
                                    && i != 1
                                    && (matches!(
                                        &args[1],
                                        Self::Cast(_, DataType::TimestampTz)
                                    ) || args[1]
                                        .bind(resolve, tables, parameters)
                                        .is_ok_and(|a| a.ty == DataType::TimestampTz)) =>
                            {
                                Some(DataType::Text)
                            }
                            "lower" | "upper" | "length" => Some(DataType::Text),
                            "substring" | "substr" => Some(if i == 0 {
                                DataType::Text
                            } else {
                                DataType::Integer
                            }),
                            "log" if args.len() == 2 => Some(DataType::Numeric),
                            "power"
                                if args.iter().any(|a| {
                                    a.bind(resolve, tables, parameters)
                                        .is_ok_and(|a| a.ty == DataType::Numeric)
                                }) && !args.iter().any(|a| {
                                    a.bind(resolve, tables, parameters).is_ok_and(|a| {
                                        matches!(a.ty, DataType::Real | DataType::DoublePrecision)
                                    })
                                }) =>
                            {
                                Some(DataType::Numeric)
                            }
                            n if numeric::float_arity(n).is_some() => {
                                Some(DataType::DoublePrecision)
                            }
                            // PG18 Parse with no parameter OIDs selects the
                            // preferred numeric-category type for abs(unknown).
                            "abs" => Some(
                                a.bind(resolve, tables, parameters)
                                    .map_or(DataType::DoublePrecision, |a| a.ty),
                            ),
                            "scale" | "min_scale" | "trim_scale" => Some(DataType::Numeric),
                            "factorial" => Some(DataType::BigInt),
                            "div" => Some(DataType::Numeric),
                            "mod" | "gcd" | "lcm" => args
                                .iter()
                                .filter_map(|a| {
                                    a.bind(resolve, tables, parameters).ok().map(|a| a.ty)
                                })
                                .find(number),
                            "round" | "trunc" if args.len() == 2 => Some(if i == 1 {
                                DataType::Integer
                            } else {
                                DataType::Numeric
                            }),
                            "round" | "trunc" | "ceil" | "ceiling" | "floor" | "sign" => {
                                Some(DataType::DoublePrecision)
                            }
                            "coalesce" => common.clone(),
                            _ => None,
                        }
                    };
                    a.infer(ty.as_ref(), parameters, resolve, tables)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn pg_error(e: giggles_pg_types::Error) -> SqlError {
    match e {
        giggles_pg_types::Error::Limit => SqlError::Limit,
        giggles_pg_types::Error::Range => SqlError::Overflow,
        _ => SqlError::Type,
    }
}

fn numeric_function(name: &str, v: &[Value]) -> Option<R<Value>> {
    if !matches!(
        name,
        "round" | "ceil" | "ceiling" | "floor" | "trunc" | "sign"
    ) {
        return None;
    }
    Some((|| {
        let scale = if let Some(Value::Int(n)) = v.get(1) {
            i32::try_from(*n).map_err(|_| SqlError::Overflow)?
        } else {
            0
        };
        Ok(match &v[0] {
            Value::Numeric(n) => {
                let zero = Numeric::parse("0").map_err(pg_error)?;
                // PostgreSQL preserves specials before examining the requested scale.
                let out = if n.scale().is_none() && name != "sign" || n.to_string() == "NaN" {
                    n.clone()
                } else if name == "sign" {
                    Numeric::parse(if n > &zero {
                        "1"
                    } else if n < &zero {
                        "-1"
                    } else {
                        "0"
                    })
                    .map_err(pg_error)?
                } else {
                    // REL_18_STABLE numeric_round/numeric_trunc clamp dscale to
                    // NUMERIC_DSCALE_MAX before arithmetic. The frozen Numeric
                    // API has a smaller integer range: positions beyond its
                    // coefficient ceiling can only yield zero or exceed capacity.
                    if scale < -(giggles_pg_types::MAX_SCALE as i32) {
                        let text = n.to_string();
                        let whole = text.trim_start_matches('-').split('.').next().unwrap();
                        if name == "round"
                            && i64::from(whole.len() as u32) == -i64::from(scale)
                            && whole.as_bytes()[0] >= b'5'
                        {
                            return Err(SqlError::Limit);
                        }
                        return Ok(Value::Numeric(zero));
                    }
                    let scale = scale.min(giggles_pg_types::MAX_SCALE as i32);
                    if name == "trunc" {
                        return numeric::trunc(n, scale).map(Value::Numeric);
                    }
                    let mut r = n.round(scale).map_err(pg_error)?;
                    let adjust = match name {
                        "floor" if r > *n => Some("-1"),
                        "ceil" | "ceiling" if r < *n => Some("1"),
                        _ => None,
                    };
                    if let Some(a) = adjust {
                        let unit = Numeric::parse(a).map_err(pg_error)?;
                        r = r.checked_add(&unit).map_err(pg_error)?;
                    }
                    r
                };
                Value::Numeric(out)
            }
            Value::Float4(n) => Value::Float4(Float4::from_bits(
                (float_function(name, n.value() as f64) as f32).to_bits(),
            )),
            Value::Float8(n) => {
                Value::Float8(Float8::from_bits(float_function(name, n.value()).to_bits()))
            }
            Value::Int(n) => Value::Int(if name == "sign" { n.signum() } else { *n }),
            _ => return Err(SqlError::Type),
        })
    })())
}
fn float_function(name: &str, n: f64) -> f64 {
    match name {
        "round" => n.round_ties_even(),
        "floor" => n.floor(),
        "ceil" | "ceiling" => n.ceil(),
        "trunc" => n.trunc(),
        "sign" => {
            if n == 0.0 || n.is_nan() {
                0.0
            } else {
                n.signum()
            }
        }
        _ => n,
    }
}

// PostgreSQL implicit numeric operator promotion. Integer cross-width
// comparisons stay exact; floating operands choose a floating operator.
pub(super) fn comparison_type(a: &DataType, b: &DataType) -> Option<DataType> {
    if !number(a) || !number(b) {
        return None;
    }
    Some(
        if matches!(a, DataType::Real | DataType::DoublePrecision)
            || matches!(b, DataType::Real | DataType::DoublePrecision)
        {
            // Mixed float4 operators resolve via float8, including integer and
            // numeric operands; rounding both sides to float4 is incorrect.
            DataType::DoublePrecision
        } else if a == &DataType::Numeric || b == &DataType::Numeric {
            DataType::Numeric
        } else {
            DataType::BigInt
        },
    )
}

pub(super) fn aggregate_numeric_add(old: Value, input: Value, budget: &mut usize) -> R<Value> {
    let input = match input {
        Value::Numeric(n) => n,
        Value::Int(n) => {
            // BigInt::from(i64): inline on 64-bit; at most a two-u32 Vec on 32-bit.
            reserve(budget, 8)?;
            Numeric::aggregate_from_i64(n)
        }
        _ => return Err(SqlError::Type),
    };
    match old {
        Value::Null => Ok(Value::Numeric(input)),
        Value::Numeric(old) => {
            reserve(budget, old.aggregate_scratch_bound(&input))?;
            old.checked_add_aggregate(&input)
                .map(Value::Numeric)
                .map_err(|_| SqlError::Overflow)
        }
        _ => Err(SqlError::Type),
    }
}

impl BoundScalar {
    pub(in super::super) fn insert_assignment(self, target: &DataType) -> R<Self> {
        if &self.ty == target {
            return Ok(self);
        }
        if !(number(&self.ty) && number(target)
            || *target == DataType::Text && (number(&self.ty) || self.ty == DataType::Boolean))
        {
            return Err(SqlError::Type);
        }
        Ok(Self {
            ty: target.clone(),
            node: Node::Cast(Box::new(self)),
        })
    }
}
