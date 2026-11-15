//! Typed scalar projection binding. No SQL generation or catalog emulation.
use super::*;
mod arithmetic;
pub(in crate::relational) mod assignment;
mod binary;
mod cast;
mod character;
mod comparison;
#[cfg(test)]
mod cross_binding_tests;
mod pg18_types;
pub(super) use comparison::{comparable, compare_cross, cross_compare};
mod datetime_input;
mod degree_math;
mod greatest;
#[cfg(test)]
mod integer_cast_tests;
mod integer_formats;
mod join;
pub(super) use join::{join_equal, join_key, join_key_supported};
mod md5;
mod numeric;
mod overlay;
#[cfg(test)]
mod overlay_tests;
mod pg18_routines;
mod planning;
mod position;
mod quoting;

pub(super) fn keyword_requires_quotes(name: &str) -> bool {
    quoting::keyword_requires_quotes(name)
}
mod format;
mod substring_order;
mod temporal;
mod text;
// Preserve failed routine lookup until argument and source analysis finishes.
pub(super) const CAST_TYPMOD: &str = "\0cast:typmod";
const UNDEFINED_ROUTINE_PREFIX: &str = "public.\0";
const PG_TOAST_ROUTINE_PREFIX: &str = "pg_toast.\0";
const INVALID_SCHEMA_ROUTINE_PREFIX: &str = "schema.\0";
fn deferred_routine_error(name: &str) -> Option<SqlError> {
    if name.starts_with(UNDEFINED_ROUTINE_PREFIX) || name.starts_with(PG_TOAST_ROUTINE_PREFIX) {
        Some(SqlError::UndefinedFunction)
    } else if name.starts_with(INVALID_SCHEMA_ROUTINE_PREFIX) {
        Some(SqlError::InvalidSchema)
    } else {
        None
    }
}
pub(super) fn is_deferred_routine_name(name: &str) -> bool {
    deferred_routine_error(name).is_some()
}
pub(super) fn deferred_routine_binds_for_sql_role(name: &str) -> bool {
    deferred_routine_error(name).is_some() && !name.starts_with(PG_TOAST_ROUTINE_PREFIX)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UnaryOp {
    Neg,
    Plus,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
}
#[derive(Debug, Clone)]
pub(super) enum Scalar {
    Predicate(Box<super::Filter>),
    Case(Box<CaseExpr>),
    Unary(UnaryOp, Box<Scalar>),
    Binary(BinaryOp, Box<Scalar>, Box<Scalar>),
    Param(usize),
    Cast(Box<Scalar>, DataType),
    Domain(Box<Scalar>, String, R<TypeDef>),
    Column(ColumnRef),
    Literal(Value),
    Call(String, Vec<Scalar>),
    SubstringOrder([Box<Scalar>; 3]),
}
#[derive(Debug, Clone)]
pub(super) struct CaseExpr {
    pub(super) arms: Vec<(super::Filter, Scalar)>,
    pub(super) otherwise: Option<Scalar>,
}
pub(in crate::relational) struct BoundScalar {
    pub(super) ty: DataType,
    node: Node,
}
enum Node {
    EnumSort(Box<BoundScalar>, Vec<String>),
    Domain(Box<BoundScalar>, TypeDef),
    Predicate(Box<super::BoundFilter>),
    Case(
        Vec<(super::BoundFilter, BoundScalar)>,
        Option<Box<BoundScalar>>,
    ),
    ZonedOperator(
        String,
        Vec<BoundScalar>,
        Option<crate::relational::clock::ExecutionClock>,
    ),
    CrossCompare(
        String,
        Box<BoundScalar>,
        Box<BoundScalar>,
        Option<crate::relational::clock::ExecutionClock>,
    ),
    Sequence(
        String,
        Vec<BoundScalar>,
        Option<crate::relational::clock::ExecutionClock>,
    ),
    Xact(
        String,
        Vec<BoundScalar>,
        Option<crate::relational::clock::ExecutionClock>,
    ),
    Clock(
        String,
        i32,
        Option<crate::relational::clock::ExecutionClock>,
    ),
    Param(usize),
    Cast(Box<BoundScalar>),
    TypmodCast(Box<BoundScalar>, i32),
    SessionCast(
        Box<BoundScalar>,
        Option<crate::relational::clock::ExecutionClock>,
    ),
    Column(BoundColumn, String),
    Literal(Value),
    Call(String, Vec<BoundScalar>),
}
fn cast_node(value: BoundScalar, target: &DataType) -> Node {
    if session_sensitive(&value.ty, target) {
        Node::SessionCast(Box::new(value), None)
    } else {
        Node::Cast(Box::new(value))
    }
}
/// Shared result selection for COALESCE and CASE, with unknown literals omitted.
fn common_result_type(types: impl IntoIterator<Item = DataType>) -> R<DataType> {
    let mut common = None;
    for ty in types {
        if ty == DataType::Bpchar {
            return Err(SqlError::Unsupported);
        }
        let ty = if ty == DataType::Varchar {
            DataType::Text
        } else {
            ty
        };
        common = Some(match common {
            None => ty,
            Some(ref old) if *old == ty => ty,
            Some(ref old) if integer(old) && integer(&ty) => {
                if *old == DataType::BigInt || ty == DataType::BigInt {
                    DataType::BigInt
                } else {
                    DataType::Integer
                }
            }
            Some(ref old) if number(old) && number(&ty) => {
                if matches!(old, DataType::DoublePrecision | DataType::Real)
                    || matches!(ty, DataType::DoublePrecision | DataType::Real)
                {
                    DataType::DoublePrecision
                } else {
                    DataType::Numeric
                }
            }
            _ => return Err(SqlError::Type),
        });
    }
    Ok(common.unwrap_or(DataType::Text))
}
fn coalesce_hint(
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> Option<DataType> {
    // Retain COALESCE's established first-known-argument parameter inference.
    args.iter()
        .find_map(|a| {
            cast::explicit_target(a).cloned().or_else(|| match a {
                Scalar::Literal(Value::Null | Value::Text(_)) => None,
                _ => a.bind(resolve, tables, parameters).ok().map(|b| b.ty),
            })
        })
        .and_then(|ty| common_result_type([ty]).ok())
}
pub(in crate::relational) fn zone_dependent_literal(value: &Value, target: &DataType) -> bool {
    if !datetime_input::target(target) || !matches!(value, Value::Text(_)) {
        return false;
    }
    matches!(
        literal_cast_node(value.clone(), target),
        Ok(Node::SessionCast(..))
    )
}
pub(in crate::relational) fn coerce_clocked(
    value: Value,
    target: &DataType,
    clock: Option<&crate::relational::clock::ExecutionClock>,
) -> R<Value> {
    if value == Value::Null {
        return Ok(value);
    }
    if matches!(value, Value::Text(_)) && datetime_input::target(target) {
        return session_cast(value, &DataType::Text, target, clock);
    }
    cast_value(value, target, &mut 0)
}
fn session_sensitive(source: &DataType, target: &DataType) -> bool {
    use DataType::*;
    matches!(
        (source, target),
        (TimestampTz | Real | DoublePrecision, Text)
            | (Text | Date | Timestamp, TimestampTz)
            | (TimestampTz, Date | Timestamp | Time | TimeTz)
    )
}
fn literal_cast_node(value: Value, target: &DataType) -> R<Node> {
    if let Value::Text(text) = &value
        && datetime_input::target(target)
        && (datetime_input::clock_word(text)
            || *target == DataType::TimeTz
                && match (
                    datetime_input::timetz(text, 0),
                    datetime_input::timetz(text, 3600),
                ) {
                    (Ok(a), Ok(b)) => a != b,
                    _ => false,
                })
    {
        return Ok(Node::SessionCast(
            Box::new(BoundScalar {
                ty: DataType::Text,
                node: Node::Literal(value),
            }),
            None,
        ));
    }
    if let Value::Text(text) = &value
        && *target == DataType::TimestampTz
    {
        use giggles_pg_types::temporal::{DateOrder, SessionSettings, SessionZone};
        let parse = |offset| {
            TimestampTz::parse_in_session(
                text,
                &SessionSettings {
                    date_order: DateOrder::default(),
                    zone: SessionZone::Fixed(offset),
                },
            )
        };
        if let (Ok(a), Ok(b)) = (parse(0), parse(3600))
            && a == b
        {
            return Ok(Node::Literal(Value::TimestampTz(a)));
        }
        return Ok(Node::SessionCast(
            Box::new(BoundScalar {
                ty: DataType::Text,
                node: Node::Literal(value),
            }),
            None,
        ));
    }
    Ok(Node::Literal(coerce_clocked(value, target, None)?))
}
fn session_cast(
    value: Value,
    source: &DataType,
    target: &DataType,
    clock: Option<&crate::relational::clock::ExecutionClock>,
) -> R<Value> {
    let fallback;
    let clock = match clock {
        Some(clock) => clock,
        None => {
            fallback =
                crate::relational::clock::ExecutionClock::new(&crate::relational::StatementClock {
                    unavailable: None,
                    transaction_us: 0,
                    statement_us: 0,
                    zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
                    wall: None,
                });
            &fallback
        }
    };
    match (value, target) {
        (Value::Float4(n), DataType::Text) => Ok(Value::Text(
            n.to_text_with_extra_float_digits(clock.extra_float_digits())
                .map_err(pg_error)?,
        )),
        (Value::Float8(n), DataType::Text) => Ok(Value::Text(
            n.to_text_with_extra_float_digits(clock.extra_float_digits())
                .map_err(pg_error)?,
        )),
        (Value::Text(text), target) if datetime_input::target(target) => {
            datetime_input::parse(text, target, clock)
        }
        (Value::TimestampTz(t), DataType::Text) => Ok(Value::Text(
            t.format_in_zone(
                clock
                    .zone()
                    .unwrap_or(giggles_pg_types::temporal::SessionZone::Fixed(0)),
            )
            .map_err(pg_error)?,
        )),
        (value, target) => assignment::convert(value, source, target, Some(clock)),
    }
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
pub(super) fn sequence_name(name: &str) -> bool {
    matches!(name, "nextval" | "currval" | "setval")
}
impl Scalar {
    /// Catalog dependencies are distinct from the positive table-free proof.
    pub(in crate::relational) fn sequence_dependencies(&self, out: &mut Vec<Option<String>>) {
        match self {
            Self::Predicate(filter) => filter.sequence_dependencies(out),
            Self::Case(case) => {
                for (condition, result) in &case.arms {
                    condition.sequence_dependencies(out);
                    result.sequence_dependencies(out);
                }
                if let Some(result) = &case.otherwise {
                    result.sequence_dependencies(out);
                }
            }
            Self::Call(name, args) => {
                if sequence_name(name) {
                    fn literal(arg: &Scalar) -> Option<String> {
                        match arg {
                            Scalar::Literal(Value::Text(name)) => Some(name.clone()),
                            Scalar::Cast(arg, DataType::Text) => literal(arg),
                            _ => None,
                        }
                    }
                    out.push(args.first().and_then(literal));
                }
                for arg in args {
                    arg.sequence_dependencies(out);
                }
            }
            Self::Cast(arg, _) | Self::Domain(arg, ..) | Self::Unary(_, arg) => {
                arg.sequence_dependencies(out)
            }
            Self::Binary(_, a, b) => {
                a.sequence_dependencies(out);
                b.sequence_dependencies(out);
            }
            Self::SubstringOrder(args) => {
                for arg in args {
                    arg.sequence_dependencies(out);
                }
            }
            _ => {}
        }
    }
    pub(super) fn has_undefined_routine(&self) -> bool {
        match self {
            Self::Predicate(filter) => filter.has_undefined_routine(),
            Self::Case(case) => {
                case.arms.iter().any(|(condition, result)| {
                    condition.has_undefined_routine() || result.has_undefined_routine()
                }) || case
                    .otherwise
                    .as_ref()
                    .is_some_and(Self::has_undefined_routine)
            }
            Self::Call(name, args) => {
                deferred_routine_error(name).is_some()
                    || args.iter().any(Self::has_undefined_routine)
            }
            Self::Cast(arg, _) | Self::Domain(arg, ..) | Self::Unary(_, arg) => {
                arg.has_undefined_routine()
            }
            Self::Binary(_, left, right) => {
                left.has_undefined_routine() || right.has_undefined_routine()
            }
            Self::SubstringOrder(args) => args.iter().any(|a| a.has_undefined_routine()),
            _ => false,
        }
    }
    pub(super) fn aggregate_unknown(&self, parameters: &[Option<DataType>]) -> bool {
        match self {
            Self::Predicate(_) | Self::Case(_) => false,
            Self::Literal(Value::Null | Value::Text(_)) => true,
            Self::Param(n) => parameters.get(n - 1).is_none_or(Option::is_none),
            _ => false,
        }
    }
    pub(super) fn reject_nested_aggregate(&self) -> R<()> {
        match self {
            Self::Predicate(filter) => filter.reject_nested_aggregate(),
            Self::Case(case) => {
                for (condition, result) in &case.arms {
                    condition.reject_nested_aggregate()?;
                    result.reject_nested_aggregate()?;
                }
                case.otherwise
                    .as_ref()
                    .map_or(Ok(()), Self::reject_nested_aggregate)
            }
            Self::Call(name, _) if deferred_routine_error(name).is_some() => Ok(()),
            Self::Call(name, _) if matches!(name.as_str(), "count" | "sum" | "min" | "max") => {
                Err(SqlError::GroupingError)
            }
            Self::Call(_, args) => args.iter().try_for_each(Self::reject_nested_aggregate),
            Self::SubstringOrder(args) => args.iter().try_for_each(|a| a.reject_nested_aggregate()),
            Self::Cast(arg, _) | Self::Domain(arg, ..) | Self::Unary(_, arg) => {
                arg.reject_nested_aggregate()
            }
            Self::Binary(_, a, b) => {
                a.reject_nested_aggregate()?;
                b.reject_nested_aggregate()
            }
            _ => Ok(()),
        }
    }

    pub(super) fn parse(p: &mut Parser, depth: usize) -> R<Self> {
        Self::parse_context(p, depth, false)
    }
    pub(super) fn parse_aggregate_argument(p: &mut Parser, depth: usize) -> R<Self> {
        Self::parse_context(p, depth, true)
    }
    pub(super) fn parse_expr(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        match super::Filter::parse_context(p, 0, depth, aggregate_argument)? {
            super::Filter::Scalar(value) => {
                arithmetic::admit_shape(&value, depth, &mut 0)?;
                Ok(value)
            }
            filter => {
                // Count the scalar wrapper without adding syntactic nesting depth.
                filter.admit_shape(depth, &mut 1)?;
                Ok(Self::Predicate(Box::new(filter)))
            }
        }
    }
    pub(super) fn has_volatile_call(&self) -> bool {
        match self {
            Self::Predicate(filter) => filter.has_volatile_call(),
            Self::Case(case) => {
                case.arms.iter().any(|(condition, result)| {
                    condition.has_volatile_call() || result.has_volatile_call()
                }) || case.otherwise.as_ref().is_some_and(Self::has_volatile_call)
            }
            Self::Call(name, args) => {
                matches!(
                    name.as_str(),
                    "nextval"
                        | "setval"
                        | "currval"
                        | "random"
                        | "clock_timestamp"
                        | "pg_current_xact_id"
                        | "pg_current_xact_id_if_assigned"
                        | "pg_xact_status"
                ) || args.iter().any(Self::has_volatile_call)
            }
            Self::Cast(value, _) | Self::Domain(value, ..) | Self::Unary(_, value) => {
                value.has_volatile_call()
            }
            Self::Binary(_, left, right) => left.has_volatile_call() || right.has_volatile_call(),
            Self::SubstringOrder(args) => args.iter().any(|a| a.has_volatile_call()),
            Self::Param(_) | Self::Column(_) | Self::Literal(_) => false,
        }
    }
    fn parse_context(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        let value = Self::parse_binary(p, depth, aggregate_argument, 0)?;
        arithmetic::admit_shape(&value, depth, &mut 0)?;
        Ok(value)
    }
    fn parse_binary(
        p: &mut Parser,
        depth: usize,
        aggregate_argument: bool,
        minimum: u8,
    ) -> R<Self> {
        if depth > 32 {
            return Err(SqlError::Limit);
        }
        let mut value = Self::parse_unary(p, depth, aggregate_argument)?;
        while let Some((op, precedence)) = arithmetic::token_operator(p.tokens.get(p.at)) {
            if precedence < minimum {
                break;
            }
            p.at += 1;
            let right = Self::parse_binary(p, depth + 1, aggregate_argument, precedence + 1)?;
            value = Self::Binary(op, Box::new(value), Box::new(right));
            arithmetic::admit_shape(&value, depth, &mut 0)?;
        }
        Ok(value)
    }
    fn parse_unary(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        if depth > 32 {
            return Err(SqlError::Limit);
        }
        let sign = match p.tokens.get(p.at) {
            Some(Token::Op(op)) if op == "-" => Some(UnaryOp::Neg),
            Some(Token::Op(op)) if op == "+" => Some(UnaryOp::Plus),
            _ => None,
        };
        let value = if let Some(sign) = sign {
            p.at += 1;
            if sign == UnaryOp::Neg
                && let Some(Token::Number(n)) = p.tokens.get(p.at)
                && p.tokens.get(p.at + 1) != Some(&Token::Symbol(':'))
            {
                // Fold bare numeric tokens; postfix casts bind before unary minus.
                let n = if let Some(n) = n.strip_prefix('-') {
                    n.to_owned()
                } else {
                    format!("-{n}")
                };
                p.at += 1;
                let value = match n.parse::<i64>() {
                    Ok(n) => Value::Int(n),
                    Err(_) => Value::Numeric(Numeric::parse(&n).map_err(pg_error)?),
                };
                return Self::parse_casts(p, depth, Self::Literal(value));
            }
            Self::Unary(
                sign,
                Box::new(Self::parse_unary(p, depth + 1, aggregate_argument)?),
            )
        } else {
            let value = Self::primary_context(p, depth, aggregate_argument)?;
            return Self::parse_casts(p, depth, value);
        };
        Ok(value)
    }
    fn parse_casts(p: &mut Parser, depth: usize, mut value: Self) -> R<Self> {
        let mut casts = depth;
        loop {
            if p.sym('[') {
                let index = Self::parse_expr(p, depth + 1, false)?;
                p.punct(']')?;
                value = Self::Call("\0array_subscript".into(), vec![value, index]);
            } else if matches!(p.tokens.get(p.at), Some(Token::Op(op)) if op=="->" || op=="->>") {
                let Some(Token::Op(op)) = p.tokens.get(p.at).cloned() else {
                    unreachable!()
                };
                p.at += 1;
                let negative = matches!(p.tokens.get(p.at),Some(Token::Op(op)) if op=="-");
                if negative {
                    p.at += 1;
                }
                let mut key = Self::primary_context(p, depth + 1, false)?;
                if negative {
                    key = Self::Unary(UnaryOp::Neg, Box::new(key));
                }
                value = Self::Call(
                    if op == "->" {
                        "\0json_get"
                    } else {
                        "\0json_text"
                    }
                    .into(),
                    vec![value, key],
                );
            } else if p.sym(':') {
                p.punct(':')?;
                casts += 1;
                if casts > 32 {
                    return Err(SqlError::Limit);
                }
                value = cast::parse_expression(p, value)?;
            } else {
                break;
            }
        }
        Ok(value)
    }
    // Preserve quotedness for SQL construct dispatch and truncate names without
    // applying the stored-object reservation policy to builtin routines.
    fn routine_name(p: &mut Parser) -> R<(String, bool)> {
        let (name, quoted) = match p.tokens.get(p.at) {
            Some(Token::Word(name)) => (name, false),
            Some(Token::QuotedIdent(name)) => (name, true),
            _ => return Err(SqlError::Syntax),
        };
        let name = super::super::ident::truncate(name, 63).to_owned();
        p.at += 1;
        Ok((name, quoted))
    }
    fn primary_context(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        if depth > 32 {
            return Err(SqlError::Limit);
        }
        if p.word("case") {
            let operand = if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "when") {
                None
            } else {
                let operand = Self::parse_expr(p, depth + 1, aggregate_argument)?;
                if operand.has_volatile_call() {
                    return Err(SqlError::Unsupported);
                }
                Some(operand)
            };
            let mut arms = Vec::new();
            while p.word("when") {
                if arms.len() == MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                let condition = if operand.is_some() {
                    super::Filter::Scalar(Self::parse_expr(p, depth + 1, aggregate_argument)?)
                } else {
                    super::Filter::parse_context(p, 0, depth + 1, aggregate_argument)?
                };
                p.need("then")?;
                let result = Self::parse_expr(p, depth + 1, aggregate_argument)?;
                arms.push((condition, result));
            }
            if arms.is_empty() {
                return Err(SqlError::Syntax);
            }
            let otherwise = if p.word("else") {
                Some(Self::parse_expr(p, depth + 1, aggregate_argument)?)
            } else {
                None
            };
            p.need("end")?;
            if let Some(operand) = operand {
                super::admit_expansion(&operand, arms.len())?;
                for (condition, _) in &mut arms {
                    let super::Filter::Scalar(value) = condition else {
                        unreachable!()
                    };
                    *condition = super::Filter::Compare(operand.clone(), "=".into(), value.clone());
                }
            }
            return Ok(Self::Case(Box::new(CaseExpr { arms, otherwise })));
        }
        if matches!(p.tokens.get(p.at), Some(Token::Word(word)) if matches!(word.as_str(), "when" | "then" | "else" | "end"))
        {
            return Err(SqlError::Syntax);
        }
        if let Some(Token::Word(name)) = p.tokens.get(p.at)
            && matches!(
                name.as_str(),
                "current_timestamp"
                    | "localtimestamp"
                    | "current_date"
                    | "current_time"
                    | "localtime"
            )
        {
            let name = name.clone();
            p.at += 1;
            let mut args = vec![];
            if p.sym('(') {
                if name == "current_date" {
                    return Err(SqlError::Syntax);
                }
                let Some(Token::Number(value)) = p.tokens.get(p.at) else {
                    return Err(SqlError::Syntax);
                };
                let value = value.parse::<u32>().map_err(|_| SqlError::Syntax)?.min(6);
                p.at += 1;
                p.punct(')')?;
                args.push(Self::Literal(Value::Int(i64::from(value))));
            }
            return Ok(Self::Call(name, args));
        }
        if p.word("cast") {
            p.punct('(')?;
            let value = Self::parse_context(p, depth + 1, aggregate_argument)?;
            p.need("as")?;
            let value = cast::parse_expression(p, value)?;
            p.punct(')')?;
            return Ok(value);
        }
        if p.sym('(') {
            let value = Self::parse_expr(p, depth + 1, aggregate_argument)?;
            p.punct(')')?;
            return Ok(value);
        }
        if cast::typed_literal(p) {
            let (ty, modifier) = cast_type(p)?;
            if modifier.is_some() {
                return Err(SqlError::Unsupported);
            }
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
                    let (routine, quoted) = Self::routine_name(p)?;
                    let prefix = match name.as_str() {
                        "pg_catalog"
                            if pg18_routines::PG_CATALOG
                                .binary_search(&routine.as_str())
                                .is_ok() =>
                        {
                            ""
                        }
                        "pg_catalog" | "public" => UNDEFINED_ROUTINE_PREFIX,
                        "pg_toast" => PG_TOAST_ROUTINE_PREFIX,
                        "information_schema"
                            if pg18_routines::INFORMATION_SCHEMA
                                .binary_search(&routine.as_str())
                                .is_ok() =>
                        {
                            return Err(SqlError::Unsupported);
                        }
                        "information_schema" => UNDEFINED_ROUTINE_PREFIX,
                        _ => INVALID_SCHEMA_ROUTINE_PREFIX,
                    };
                    (format!("{prefix}{routine}"), quoted)
                } else if pg18_routines::PG_CATALOG
                    .binary_search(&name.as_str())
                    .is_ok()
                    || (!quoted
                        && pg18_routines::NON_UNRESERVED_KEYWORDS
                            .binary_search(&name.as_str())
                            .is_ok())
                {
                    (name, quoted)
                } else {
                    (format!("{UNDEFINED_ROUTINE_PREFIX}{name}"), quoted)
                };
                p.punct('(')?;
                if name == "position" && !qualified_call && !quoted {
                    if matches!(
                        p.tokens.get(p.at),
                        None | Some(Token::Symbol(')' | ',' | '*'))
                    ) {
                        return Err(SqlError::Syntax);
                    }
                    let needle = Self::parse_context(p, depth + 1, aggregate_argument)?;
                    p.need("in")?;
                    if matches!(
                        p.tokens.get(p.at),
                        None | Some(Token::Symbol(')' | ',' | '*'))
                    ) {
                        return Err(SqlError::Syntax);
                    }
                    let source = Self::parse_context(p, depth + 1, aggregate_argument)?;
                    p.punct(')')?;
                    return Ok(Self::Call(name, vec![source, needle]));
                }
                if deferred_routine_error(&name).is_some() && p.sym('*') {
                    p.punct(')')?;
                    return Ok(Self::Call(name, vec![Self::Literal(Value::Null)]));
                }
                if name == "count" && p.sym('*') {
                    p.punct(')')?;
                    // Nested scalar aggregate expressions use the same nonnull
                    // input as query grouping's COUNT(*) normalization.
                    return Ok(Self::Call(name, vec![Self::Literal(Value::Int(1))]));
                }
                if aggregate_argument && p.sym('*') {
                    p.punct(')')?;
                    return Err(SqlError::UndefinedFunction);
                }
                if name == "trim" && !qualified_call && !quoted {
                    return Self::parse_trim_construct(p, depth, aggregate_argument);
                }
                let mut args = vec![];
                if name == "extract" && !qualified_call && !quoted {
                    let field = match p.tokens.get(p.at).cloned() {
                        Some(Token::Word(f) | Token::QuotedIdent(f)) => {
                            super::super::ident::truncate(&f, 63).to_owned()
                        }
                        Some(Token::String(f)) => f,
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
                        if deferred_routine_error(&name).is_some()
                            && matches!(p.tokens.get(p.at), None | Some(Token::Symbol(')' | ',')))
                        {
                            return Err(SqlError::Syntax);
                        }
                        // regclass is an object reference only in the first sequence
                        // argument. Lower its literal spelling without adding a wire type.
                        if args.is_empty()
                            && sequence_name(&name)
                            && matches!(p.tokens.get(p.at), Some(Token::String(_)))
                            && p.tokens.get(p.at + 1) == Some(&Token::Symbol(':'))
                            && p.tokens.get(p.at + 2) == Some(&Token::Symbol(':'))
                            && matches!(p.tokens.get(p.at + 3), Some(Token::Word(t)) if t == "regclass")
                        {
                            let Some(Token::String(value)) = p.tokens.get(p.at).cloned() else {
                                unreachable!()
                            };
                            p.at += 4;
                            args.push(Self::Literal(Value::Text(value)));
                        } else {
                            args.push(
                                if !qualified_call
                                    && !quoted
                                    && matches!(name.as_str(), "substring" | "overlay")
                                {
                                    Self::parse_context(p, depth + 1, aggregate_argument)?
                                } else {
                                    Self::parse_expr(p, depth + 1, aggregate_argument)?
                                },
                            );
                        }
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
        if matches!(self, Self::Unary(..) | Self::Binary(..)) {
            return arithmetic::bind(self, resolve, tables, parameters);
        }
        let expression_parameter_error = |error| {
            if matches!(error, SqlError::Parameters) && parameters.iter().any(Option::is_none) {
                SqlError::IndeterminateDatatype
            } else {
                error
            }
        };
        let (ty, node) = match self {
            Self::Predicate(filter) => (
                DataType::Boolean,
                Node::Predicate(Box::new(
                    filter
                        .bind(parameters, resolve, tables)
                        .map_err(expression_parameter_error)?,
                )),
            ),
            Self::Case(case) => {
                let originals: Vec<_> = case
                    .arms
                    .iter()
                    .map(|(_, result)| result)
                    .chain(case.otherwise.iter())
                    .collect();
                let ty = common_result_type(
                    originals
                        .iter()
                        .filter(|result| {
                            !matches!(result, Self::Literal(Value::Null | Value::Text(_)))
                        })
                        .map(|result| {
                            result
                                .bind(resolve, tables, parameters)
                                .map_err(expression_parameter_error)
                                .map(|bound| bound.ty)
                        })
                        .collect::<R<Vec<_>>>()?,
                )?;
                let result = |value: &Scalar| -> R<BoundScalar> {
                    if let Self::Literal(value @ (Value::Text(_) | Value::Null)) = value {
                        return Ok(BoundScalar {
                            ty: ty.clone(),
                            node: literal_cast_node(value.clone(), &ty)?,
                        });
                    }
                    let bound = value
                        .bind(resolve, tables, parameters)
                        .map_err(expression_parameter_error)?;
                    if bound.ty == ty {
                        Ok(bound)
                    } else {
                        Ok(BoundScalar {
                            ty: ty.clone(),
                            node: cast_node(bound, &ty),
                        })
                    }
                };
                let arms = case
                    .arms
                    .iter()
                    .map(|(condition, value)| {
                        Ok((
                            condition
                                .bind(parameters, resolve, tables)
                                .map_err(expression_parameter_error)?,
                            result(value)?,
                        ))
                    })
                    .collect::<R<Vec<_>>>()?;
                let otherwise = case
                    .otherwise
                    .as_ref()
                    .map(result)
                    .transpose()?
                    .map(Box::new);
                (ty, Node::Case(arms, otherwise))
            }
            Self::Unary(..) | Self::Binary(..) => unreachable!(),
            Self::Param(n) => (
                parameters[*n - 1].clone().ok_or(SqlError::Parameters)?,
                Node::Param(*n),
            ),
            Self::Domain(a, _, target) => {
                let ty = target.as_ref().map_err(Clone::clone)?;
                let TypeDef::Domain { base, typmod, .. } = ty else {
                    return Err(SqlError::Unsupported);
                };
                let inner = cast::expression(*a.clone(), (base.clone(), *typmod))
                    .bind(resolve, tables, parameters)?;
                (base.clone(), Node::Domain(Box::new(inner), ty.clone()))
            }
            Self::Cast(a, ty) => {
                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) = a.as_ref()
                    && datetime_input::target(ty)
                {
                    return Ok(BoundScalar {
                        ty: ty.clone(),
                        node: match literal_cast_node(v.clone(), ty)? {
                            Node::Literal(value) => Node::Cast(Box::new(BoundScalar {
                                ty: ty.clone(),
                                node: Node::Literal(value),
                            })),
                            node => node,
                        },
                    });
                }
                if let Self::Literal(Value::Text(_) | Value::Null) = a.as_ref()
                    && *ty != DataType::TimestampTz
                {
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
                if a.ty == DataType::Bpchar
                    && !matches!(ty, DataType::Text | DataType::Varchar | DataType::Bpchar)
                {
                    return Err(SqlError::Unsupported);
                }
                if !(number(&a.ty) && number(ty)
                    || a.ty == *ty
                    || integer(&a.ty) && (integer(ty) || *ty == DataType::Bytes)
                    || a.ty == DataType::Bytes && integer(ty)
                    || matches!(a.ty, DataType::Text | DataType::Varchar | DataType::Bpchar)
                    || matches!(ty, DataType::Text | DataType::Varchar | DataType::Bpchar)
                    || a.ty == DataType::Boolean && *ty == DataType::Integer
                    || a.ty == DataType::Integer && *ty == DataType::Boolean
                    || session_sensitive(&a.ty, ty))
                {
                    return Err(SqlError::Type);
                }
                (ty.clone(), cast_node(a, ty))
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
            Self::Call(name, args) if name == CAST_TYPMOD => {
                let [inner, Self::Literal(Value::Int(modifier))] = args.as_slice() else {
                    return Err(SqlError::Syntax);
                };
                let inner = inner.bind(resolve, tables, parameters)?;
                return Ok(BoundScalar {
                    ty: inner.ty.clone(),
                    node: Node::TypmodCast(
                        Box::new(inner),
                        i32::try_from(*modifier).map_err(|_| SqlError::Type)?,
                    ),
                });
            }
            Self::Call(name, _) if deferred_routine_error(name).is_some() => {
                return Err(deferred_routine_error(name).unwrap());
            }
            Self::Call(name, args) if name == "overlay" => {
                return Self::bind_overlay(args, resolve, tables, parameters);
            }
            Self::Call(name, args) if greatest::known_name(name) => {
                return greatest::bind(name, args, resolve, tables, parameters);
            }
            Self::Call(name, args) => {
                if matches!(
                    name.as_str(),
                    "pg_current_xact_id" | "pg_current_xact_id_if_assigned" | "pg_xact_status"
                ) {
                    let status = name == "pg_xact_status";
                    if args.len() != usize::from(status) {
                        return Err(SqlError::UndefinedFunction);
                    }
                    let mut bound = Vec::with_capacity(args.len());
                    for arg in args {
                        let mut value = arg.bind(resolve, tables, parameters)?;
                        if value.ty != DataType::BigInt {
                            if let Self::Literal(raw @ (Value::Text(_) | Value::Null)) = arg {
                                value = BoundScalar {
                                    ty: DataType::BigInt,
                                    node: literal_cast_node(raw.clone(), &DataType::BigInt)?,
                                };
                            } else if integer(&value.ty) {
                                value = BoundScalar {
                                    ty: DataType::BigInt,
                                    node: Node::Cast(Box::new(value)),
                                };
                            } else {
                                return Err(SqlError::UndefinedFunction);
                            }
                        }
                        bound.push(value);
                    }
                    return Ok(BoundScalar {
                        ty: if status {
                            DataType::Text
                        } else {
                            DataType::BigInt
                        },
                        node: Node::Xact(name.clone(), bound, None),
                    });
                }
                if planning::sequence_volatility(name).is_some() {
                    if !(matches!(name.as_str(), "nextval" | "currval") && args.len() == 1
                        || name == "setval" && matches!(args.len(), 2 | 3))
                    {
                        return Err(SqlError::UndefinedFunction);
                    }
                    let mut bound = Vec::with_capacity(args.len());
                    for (i, arg) in args.iter().enumerate() {
                        let target = match i {
                            0 => DataType::Text,
                            1 => DataType::BigInt,
                            _ => DataType::Boolean,
                        };
                        let mut value = arg.bind(resolve, tables, parameters)?;
                        if value.ty != target {
                            if matches!(arg, Self::Literal(Value::Text(_) | Value::Null)) {
                                let Self::Literal(raw) = arg else {
                                    unreachable!()
                                };
                                value = BoundScalar {
                                    ty: target.clone(),
                                    node: literal_cast_node(raw.clone(), &target)?,
                                };
                            } else if i == 1 && integer(&value.ty) {
                                value = BoundScalar {
                                    ty: target.clone(),
                                    node: Node::Cast(Box::new(value)),
                                };
                            } else {
                                return Err(SqlError::UndefinedFunction);
                            }
                        }
                        bound.push(value);
                    }
                    return Ok(BoundScalar {
                        ty: DataType::BigInt,
                        node: Node::Sequence(name.clone(), bound, None),
                    });
                }
                if let Some((ty, _volatility)) = planning::clock_signature(name) {
                    let keyword = matches!(
                        name.as_str(),
                        "current_timestamp" | "localtimestamp" | "current_time" | "localtime"
                    );
                    let precision = match args.as_slice() {
                        [] => -1,
                        [Self::Literal(Value::Int(p))] if keyword && *p >= 0 => (*p as i32).min(6),
                        _ => return Err(SqlError::UndefinedFunction),
                    };
                    return Ok(BoundScalar {
                        ty,
                        node: Node::Clock(name.clone(), precision, None),
                    });
                }
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
                    .collect::<R<Vec<_>>>()
                    .map_err(|e| {
                        if name == "format" {
                            expression_parameter_error(e)
                        } else {
                            e
                        }
                    })?;
                if args.iter().enumerate().any(|(index, arg)| {
                    arg.ty == DataType::Bpchar && !(name == "format" && index > 0)
                }) {
                    return Err(SqlError::Unsupported);
                }
                if name == "format"
                    && args
                        .first()
                        .is_some_and(|a| !matches!(a.ty, DataType::Text | DataType::Varchar))
                    && !matches!(original.first(), Some(Self::Literal(Value::Null)))
                {
                    return Err(SqlError::UndefinedFunction);
                }
                if let Some(arity) = degree_math::arity(name)
                    && (args.len() != arity
                        || args.iter().zip(original).any(|(a, source)| {
                            !number(&a.ty)
                                && !matches!(source, Self::Literal(Value::Text(_) | Value::Null))
                        }))
                {
                    return Err(SqlError::UndefinedFunction);
                }
                for arg in &mut args {
                    if arg.ty == DataType::Varchar {
                        arg.ty = DataType::Text;
                    }
                }
                let null = |a: &BoundScalar| matches!(a.node, Node::Literal(Value::Null));
                let ty = if matches!(
                    name.as_str(),
                    "\0array_subscript" | "array_length" | "\0json_get" | "\0json_text"
                ) {
                    if args.len() != 2 {
                        return Err(SqlError::UndefinedFunction);
                    }
                    match name.as_str() {
                        "array_length" | "\0array_subscript" => {
                            if !integer(&args[1].ty) {
                                return Err(SqlError::UndefinedFunction);
                            }
                            let element = match args[0].ty {
                                DataType::IntArray => DataType::Integer,
                                DataType::BigIntArray => DataType::BigInt,
                                DataType::TextArray => DataType::Text,
                                _ => return Err(SqlError::UndefinedFunction),
                            };
                            if name == "array_length" {
                                DataType::Integer
                            } else {
                                element
                            }
                        }
                        _ => {
                            if args[0].ty != DataType::Jsonb
                                || !(integer(&args[1].ty) || args[1].ty == DataType::Text)
                            {
                                return Err(SqlError::UndefinedFunction);
                            }
                            if name == "\0json_get" {
                                DataType::Jsonb
                            } else {
                                DataType::Text
                            }
                        }
                    }
                } else if integer_formats::known_name(name) {
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
                                a.node = literal_cast_node(Value::Text(s.clone()), &ty)?;
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
                                return Err(SqlError::UndefinedFunction);
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
                                    return Err(SqlError::UndefinedFunction);
                                }
                                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[i]
                                {
                                    *a = BoundScalar {
                                        ty: DataType::Numeric,
                                        node: literal_cast_node(v.clone(), &DataType::Numeric)?,
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
                                        node: literal_cast_node(
                                            v.clone(),
                                            &DataType::DoublePrecision,
                                        )?,
                                    };
                                } else {
                                    if !number(&a.ty) {
                                        return Err(SqlError::UndefinedFunction);
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
                                    node: literal_cast_node(v.clone(), &target)?,
                                };
                            } else {
                                if !(integer(&args[0].ty) || args[0].ty == target) {
                                    return Err(SqlError::UndefinedFunction);
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
                                && original.iter().all(|a| a.aggregate_unknown(parameters))
                            {
                                return Err(SqlError::AmbiguousFunction);
                            }
                            if matches!(name.as_str(), "gcd" | "lcm")
                                && args.iter().all(|a| {
                                    !matches!(
                                        a.ty,
                                        DataType::Integer | DataType::BigInt | DataType::Numeric
                                    )
                                })
                            {
                                return Err(
                                    if args.iter().zip(original).all(|(a, source)| {
                                        a.ty == DataType::SmallInt
                                            || matches!(
                                                source,
                                                Self::Literal(Value::Text(_) | Value::Null)
                                            )
                                    }) {
                                        SqlError::AmbiguousFunction
                                    } else {
                                        SqlError::UndefinedFunction
                                    },
                                );
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
                                    return Err(SqlError::UndefinedFunction);
                                }
                                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[i]
                                {
                                    a.node = literal_cast_node(v.clone(), &target)?;
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
                                    node: literal_cast_node(v.clone(), &DataType::Integer)?,
                                };
                            }
                            if args.len() == 2
                                && ((!integer(&args[1].ty) || args[1].ty == DataType::BigInt)
                                    || matches!(
                                        args[0].ty,
                                        DataType::Real | DataType::DoublePrecision
                                    ))
                            {
                                return Err(SqlError::UndefinedFunction);
                            }
                            if args[0].ty != target {
                                if !number(&args[0].ty)
                                    && !matches!(
                                        original[0],
                                        Self::Literal(Value::Text(_) | Value::Null)
                                    )
                                {
                                    return Err(SqlError::UndefinedFunction);
                                }
                                if let Self::Literal(v @ (Value::Text(_) | Value::Null)) =
                                    &original[0]
                                {
                                    args[0].node = literal_cast_node(v.clone(), &target)?;
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
                                args[0].node = literal_cast_node(v, &DataType::DoublePrecision)?;
                                args[0].ty = DataType::DoublePrecision;
                            }
                            if !number(&args[0].ty) {
                                return Err(SqlError::UndefinedFunction);
                            }
                            args[0].ty.clone()
                        }
                        "length" | "lower" | "upper" if args.len() == 1 => {
                            if !null(&args[0]) && args[0].ty != DataType::Text {
                                return Err(SqlError::UndefinedFunction);
                            }
                            if name == "length" {
                                DataType::Integer
                            } else {
                                DataType::Text
                            }
                        }
                        "substring" | "substr" if (2..=3).contains(&args.len()) => {
                            if name == "substring"
                                && args.iter().all(|a| a.ty == DataType::Text)
                                && original[1..]
                                    .iter()
                                    .any(|a| !a.aggregate_unknown(parameters))
                            {
                                // PostgreSQL regex overloads exist but are not implemented here.
                                return Err(SqlError::Type);
                            }
                            for (i, a) in args.iter_mut().enumerate() {
                                let ty = if i == 0 {
                                    DataType::Text
                                } else {
                                    DataType::Integer
                                };
                                if let Self::Literal(Value::Text(s)) = &original[i] {
                                    a.node = literal_cast_node(Value::Text(s.clone()), &ty)?;
                                    a.ty = ty.clone();
                                }
                                if null(a) {
                                    a.ty = ty.clone();
                                }
                                if a.ty != ty {
                                    return Err(SqlError::UndefinedFunction);
                                }
                            }
                            DataType::Text
                        }
                        "coalesce" if !args.is_empty() => {
                            let ty = common_result_type(
                                args.iter()
                                    .zip(original)
                                    .filter(|(a, original)| {
                                        !matches!(original, Self::Literal(Value::Text(_)))
                                            && !null(a)
                                    })
                                    .map(|(a, _)| a.ty.clone()),
                            )?;
                            for (a, original) in args.iter_mut().zip(original) {
                                if let Self::Literal(Value::Text(s)) = original {
                                    a.node = literal_cast_node(Value::Text(s.clone()), &ty)?;
                                    a.ty = ty.clone();
                                }
                                if null(a) {
                                    a.ty = ty.clone();
                                }
                            }
                            ty
                        }
                        "coalesce" => return Err(SqlError::Type),
                        "abs" | "length" | "lower" | "upper" | "substring" | "substr" => {
                            return Err(SqlError::UndefinedFunction);
                        }
                        _ => return Err(SqlError::Unsupported),
                    }
                };
                if matches!(name.as_str(), "concat" | "concat_ws" | "format") {
                    for argument in &mut args {
                        if session_sensitive(&argument.ty, &DataType::Text) {
                            let old = std::mem::replace(
                                argument,
                                BoundScalar {
                                    ty: DataType::Text,
                                    node: Node::Literal(Value::Null),
                                },
                            );
                            argument.node = Node::SessionCast(Box::new(old), None);
                        }
                    }
                }
                (ty, Node::Call(name.clone(), args))
            }
        };
        Ok(BoundScalar { ty, node })
    }
}
impl BoundScalar {
    pub(in crate::relational) fn attach_clock(
        &mut self,
        clock: Option<&crate::relational::clock::ExecutionClock>,
    ) {
        match &mut self.node {
            Node::Predicate(filter) => filter.attach_clock(clock),
            Node::Case(arms, otherwise) => {
                for (condition, result) in arms {
                    condition.attach_clock(clock);
                    result.attach_clock(clock);
                }
                if let Some(result) = otherwise {
                    result.attach_clock(clock);
                }
            }
            Node::SessionCast(value, target) => {
                *target = clock.cloned();
                value.attach_clock(clock);
            }
            Node::CrossCompare(_, a, b, target) => {
                *target = clock.cloned();
                a.attach_clock(clock);
                b.attach_clock(clock);
            }
            Node::Clock(_, _, target) => *target = clock.cloned(),
            Node::Sequence(_, args, target)
            | Node::Xact(_, args, target)
            | Node::ZonedOperator(_, args, target) => {
                *target = clock.cloned();
                args.iter_mut().for_each(|a| a.attach_clock(clock));
            }
            Node::Cast(value) | Node::TypmodCast(value, _) | Node::Domain(value, _) => {
                value.attach_clock(clock)
            }
            Node::Call(_, args) => args.iter_mut().for_each(|a| a.attach_clock(clock)),
            _ => {}
        }
    }
    pub(in crate::relational) fn clock_typmod(&self) -> i32 {
        match &self.node {
            Node::Predicate(_) | Node::Case(..) => -1,
            Node::Clock(_, precision, _) | Node::TypmodCast(_, precision) => *precision,
            _ => -1,
        }
    }
    pub(super) fn enum_sort(self, state: &Relational, tables: &[&TableDef]) -> Self {
        if let Node::Column(c, _) = &self.node
            && let Some(TypeDef::Enum { labels, .. }) =
                state.column_type(tables[c.source].columns[c.column].id)
        {
            let labels = labels.clone();
            return Self {
                ty: DataType::BigInt,
                node: Node::EnumSort(Box::new(self), labels),
            };
        }
        self
    }
    fn strong_name(&self) -> Option<&str> {
        match &self.node {
            Node::EnumSort(..) | Node::Predicate(_) | Node::ZonedOperator(..) => None,
            Node::Case(..) => Some("case"),
            Node::Call(name, args) if name == "\0array_subscript" => args[0].strong_name(),
            Node::Call(name, _) if name.starts_with("\0json_") => None,
            Node::Call(name, _) if arithmetic::internal_name(name) => None,
            Node::Column(_, name)
            | Node::Call(name, _)
            | Node::Clock(name, ..)
            | Node::Sequence(name, ..)
            | Node::Xact(name, ..) => Some(name),
            Node::Cast(a) | Node::SessionCast(a, _) | Node::TypmodCast(a, _) => a.strong_name(),
            _ => None,
        }
    }
    pub(super) fn name(&self) -> &str {
        match &self.node {
            Node::EnumSort(..) | Node::Predicate(_) => "?column?",
            Node::Case(..) => "case",
            Node::CrossCompare(..) | Node::ZonedOperator(..) => "?column?",
            Node::Param(_) => "?column?",
            Node::Domain(_, ty) => ty.name(),
            Node::TypmodCast(inner, _) => inner.name(),
            Node::Cast(a) | Node::SessionCast(a, _) => {
                if let Some(name) = a.strong_name() {
                    name
                } else {
                    match self.ty {
                        DataType::SmallInt => "int2",
                        DataType::Integer => "int4",
                        DataType::BigInt => "int8",
                        DataType::Text => "text",
                        DataType::Varchar => "varchar",
                        DataType::Bpchar => "bpchar",
                        DataType::Uuid => "uuid",
                        DataType::Jsonb => "jsonb",
                        DataType::IntArray => "integer[]",
                        DataType::BigIntArray => "bigint[]",
                        DataType::TextArray => "text[]",

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
            Node::Call(n, args) if n == "\0array_subscript" => args[0].name(),
            Node::Call(n, _) if n.starts_with("\0json_") => "?column?",
            Node::Call(n, _) if arithmetic::internal_name(n) => "?column?",
            Node::Call(n, _) | Node::Clock(n, ..) | Node::Sequence(n, ..) | Node::Xact(n, ..) => n,
        }
    }
    pub(super) fn eval(&self, row: &Tuple, params: &[Value], budget: &mut usize) -> R<Value> {
        match &self.node {
            Node::EnumSort(inner, labels) => match inner.eval(row, params, budget)? {
                Value::Null => Ok(Value::Null),
                Value::Text(label) => labels
                    .iter()
                    .position(|s| *s == label)
                    .map(|n| Value::Int(n as i64))
                    .ok_or(SqlError::InvalidTextRepresentation),
                _ => Err(SqlError::Type),
            },
            Node::Predicate(filter) => Ok(filter
                .eval(row, params, budget)?
                .map_or(Value::Null, Value::Bool)),
            Node::Case(arms, otherwise) => {
                for (condition, result) in arms {
                    if condition.eval(row, params, budget)? == Some(true) {
                        return result.eval(row, params, budget);
                    }
                }
                otherwise
                    .as_ref()
                    .map_or(Ok(Value::Null), |result| result.eval(row, params, budget))
            }
            Node::ZonedOperator(name, args, clock) => {
                let values = args
                    .iter()
                    .map(|a| a.eval(row, params, budget))
                    .collect::<R<Vec<_>>>()?;
                arithmetic::eval_zoned(name, &values, clock.as_ref(), budget)
            }
            Node::CrossCompare(op, a, b, clock) => {
                let a = a.eval(row, params, budget)?;
                let b = b.eval(row, params, budget)?;
                if a == Value::Null || b == Value::Null {
                    return Ok(Value::Null);
                }
                let order = compare_cross(&a, &b, clock.as_ref())?;
                let value = Value::Bool(match op.as_str() {
                    "=" => order == Ordering::Equal,
                    "<>" | "!=" => order != Ordering::Equal,
                    "<" => order == Ordering::Less,
                    "<=" => order != Ordering::Greater,
                    ">" => order == Ordering::Greater,
                    ">=" => order != Ordering::Less,
                    _ => return Err(SqlError::UndefinedFunction),
                });
                charge(budget, &value)?;
                Ok(value)
            }
            Node::Xact(name, args, clock) => {
                let arg = match args.first() {
                    Some(arg) => match arg.eval(row, params, budget)? {
                        Value::Null => return Ok(Value::Null),
                        Value::Int(value) => Some(value),
                        _ => return Err(SqlError::Type),
                    },
                    None => None,
                };
                let value = clock
                    .as_ref()
                    .ok_or(SqlError::Unsupported)?
                    .xact(name, arg)?;
                charge(budget, &value)?;
                Ok(value)
            }
            Node::Sequence(name, args, clock) => {
                let clock = clock.as_ref().ok_or(SqlError::Unsupported)?;
                let values = args
                    .iter()
                    .map(|a| a.eval(row, params, budget))
                    .collect::<R<Vec<_>>>()?;
                if values.contains(&Value::Null) {
                    return Ok(Value::Null);
                }
                let Value::Text(sequence) = &values[0] else {
                    return Err(SqlError::Type);
                };
                let value = match values.get(1) {
                    Some(Value::Int(value)) => Some(*value),
                    None => None,
                    _ => return Err(SqlError::Type),
                };
                let called = match values.get(2) {
                    Some(Value::Bool(value)) => *value,
                    None => true,
                    _ => return Err(SqlError::Type),
                };
                let value = clock.sequence(name, sequence, value, called)?;
                charge(budget, &value)?;
                Ok(value)
            }
            Node::Clock(name, precision, clock) => {
                let value = clock
                    .as_ref()
                    .ok_or(SqlError::Unsupported)?
                    .eval(name, if *precision < 0 { 6 } else { *precision as u32 })?;
                charge(budget, &value)?;
                Ok(value)
            }
            Node::Param(n) => {
                let v = params.get(n - 1).ok_or(SqlError::Parameters)?;
                charge(budget, v)?;
                Ok(v.clone())
            }
            Node::Domain(a, ty) => {
                let value = a.eval(row, params, budget)?;
                user_types::coerce_domain(ty, &value)?;
                Ok(value)
            }
            Node::SessionCast(a, clock) => {
                let value = a.eval(row, params, budget)?;
                if value == Value::Null {
                    return Ok(value);
                }
                let value = session_cast(value, &a.ty, &self.ty, clock.as_ref())?;
                charge(budget, &value)?;
                Ok(value)
            }
            // Consumers may relabel varchar as text for overload resolution;
            // the inner cast retains the type whose modifier is applied.
            Node::TypmodCast(inner, modifier) => cast::apply(
                inner.eval(row, params, budget)?,
                &inner.ty,
                *modifier,
                budget,
            ),
            Node::Cast(a) => {
                let mut v = a.eval(row, params, budget)?;
                if a.ty == DataType::Bpchar
                    && matches!(self.ty, DataType::Text | DataType::Varchar)
                    && let Value::Text(text) = &mut v
                {
                    text.truncate(text.trim_end_matches(' ').len());
                }
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
                if arithmetic::internal_name(n) {
                    let values = args
                        .iter()
                        .map(|a| a.eval(row, params, budget))
                        .collect::<R<Vec<_>>>()?;
                    return arithmetic::eval(n, &self.ty, &values, budget);
                }
                if matches!(
                    n.as_str(),
                    "\0array_subscript" | "array_length" | "\0json_get" | "\0json_text"
                ) {
                    let values = args
                        .iter()
                        .map(|a| a.eval(row, params, budget))
                        .collect::<R<Vec<_>>>()?;
                    let result = crate::relational::logical_types::eval(n, &args[0].ty, &values)?;
                    charge(budget, &result)?;
                    return Ok(result);
                }
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
                if greatest::known_name(n) {
                    return Ok(greatest::eval(n, values));
                }
                if n != "concat"
                    && n != "format"
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
                if n == "format" {
                    return format::eval(&values, args, budget);
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

// UPDATE keeps its assignment orchestration outside the query executor. These
// helpers expose the existing scalar machinery through a single-source query.
impl Query {
    pub(in super::super) fn assignment(p: &mut Parser, table: &str) -> R<Self> {
        let value = Scalar::parse_aggregate_argument(p, 0)?;
        value.reject_nested_aggregate()?;
        Ok(Self {
            explain: false,
            sources: vec![Source {
                name: table.into(),
                alias: table.into(),
            }],
            joins: vec![],
            items: vec![(Item::Scalar(value), None)],
            predicates: vec![],
            filter: None,
            order: vec![],
            limit: None,
        })
    }

    pub(in super::super) fn infer_assignment(
        &self,
        table: &TableDef,
        target: &DataType,
        parameters: &mut [Option<DataType>],
    ) -> R<()> {
        let resolve = |column: &ColumnRef, _: usize| {
            if column
                .qualifier
                .as_ref()
                .is_some_and(|name| name != &table.name)
            {
                return Err(SqlError::UnknownTable);
            }
            Ok(BoundColumn {
                source: 0,
                column: col(table, &column.name)?,
            })
        };
        let Item::Scalar(value) = &self.items[0].0 else {
            return Err(SqlError::Unsupported);
        };
        // Parentheses around an otherwise plain unknown value still receive
        // the assignment context, just as INSERT scalar values do.
        let expected = matches!(value, Scalar::Param(_)).then_some(target);
        value.infer(expected, parameters, &resolve, &[table])?;
        if matches!(value, Scalar::Literal(Value::Text(_) | Value::Null)) {
            Scalar::Cast(Box::new(value.clone()), target.clone()).bind(
                &resolve,
                &[table],
                parameters,
            )?;
        } else {
            value
                .bind(&resolve, &[table], parameters)?
                .insert_assignment(target)?;
        }
        Ok(())
    }

    pub(in super::super) fn bind_assignment<'a>(
        &'a self,
        state: &'a Relational,
        parameters: Vec<Option<DataType>>,
        target: &DataType,
    ) -> R<BoundQuery<'a>> {
        let mut bound = self.bind_seeded(state, parameters)?;
        let item = bound.items.pop().ok_or(SqlError::Type)?;
        let BoundItem::Scalar(value) = item else {
            return Err(SqlError::Unsupported);
        };
        let value = if let Item::Scalar(Scalar::Literal(value @ (Value::Text(_) | Value::Null))) =
            &self.items[0].0
        {
            BoundScalar {
                ty: target.clone(),
                node: literal_cast_node(value.clone(), target)?,
            }
        } else {
            value.insert_assignment(target)?
        };
        bound.items.push(BoundItem::Scalar(value));
        Ok(bound)
    }

    pub(in super::super) fn admit_assignment_literals(&self, bytes: &mut usize) -> R<()> {
        fn admit(value: &Scalar, bytes: &mut usize) -> R<()> {
            match value {
                Scalar::Predicate(filter) => {
                    filter.visit_scalars(&mut |value| admit(value, bytes))?
                }
                Scalar::Case(case) => {
                    for (condition, result) in &case.arms {
                        condition.visit_scalars(&mut |value| admit(value, bytes))?;
                        admit(result, bytes)?;
                    }
                    if let Some(result) = &case.otherwise {
                        admit(result, bytes)?;
                    }
                }
                Scalar::Literal(value) => {
                    Relational::secondary_admit_values(std::iter::once(value))?;
                    let bound = if let Value::Numeric(value) = value {
                        value.display_byte_bound()
                    } else {
                        value_bytes(value)
                    };
                    *bytes = bytes.checked_add(bound).ok_or(SqlError::Limit)?;
                    if *bytes > MAX_ROW_BYTES {
                        return Err(SqlError::Limit);
                    }
                }
                Scalar::Cast(value, _) | Scalar::Domain(value, ..) | Scalar::Unary(_, value) => {
                    admit(value, bytes)?
                }
                Scalar::Binary(_, a, b) => {
                    admit(a, bytes)?;
                    admit(b, bytes)?;
                }
                Scalar::Call(_, args) => {
                    for arg in args {
                        admit(arg, bytes)?;
                    }
                }
                Scalar::SubstringOrder(args) => {
                    for arg in args {
                        admit(arg, bytes)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        let Item::Scalar(value) = &self.items[0].0 else {
            return Err(SqlError::Unsupported);
        };
        arithmetic::admit_shape(value, 0, &mut 0)?;
        admit(value, bytes)
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

fn cast_type(p: &mut Parser) -> R<(DataType, Option<i32>)> {
    let (mut ty, modifier) = cast::target(p)?;
    if p.sym('[') {
        p.punct(']')?;
        ty = match ty {
            DataType::Integer => DataType::IntArray,
            DataType::BigInt => DataType::BigIntArray,
            DataType::Text => DataType::TextArray,
            _ => return Err(SqlError::Unsupported),
        };
    }
    Ok((ty, modifier))
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
    if ty.is_extended_text() {
        let out = crate::relational::logical_types::coerce(v, ty)?;
        charge(budget, &out)?;
        return Ok(out);
    }
    // Varchar/Bpchar values are text; length and padding apply at the column (coerce_to_column).
    let ty = if matches!(ty, DataType::Varchar | DataType::Bpchar) {
        &DataType::Text
    } else {
        ty
    };
    let out = match (v, ty) {
        (Value::Date(v), DataType::Date) => Value::Date(v),
        (Value::Date(v), DataType::Timestamp) => Value::Timestamp(
            v.add_interval(Interval::new(0, 0, 0))
                .map_err(temporal::function_error)?,
        ),
        (Value::Time(v), DataType::Interval) => Value::Interval(v.to_interval()),
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
            Date::parse(&s, &giggles_pg_types::temporal::ParserSettings::default()).map_err(
                |e| datetime_input::zone_word_error(&s, ty, temporal::parse_error(e, false)),
            )?,
        ),
        (Value::Text(s), DataType::Time) => Value::Time(Time::parse(&s).map_err(|e| {
            datetime_input::zone_word_error(&s, ty, temporal::parse_error(e, false))
        })?),
        (Value::Text(s), DataType::TimeTz) => Value::TimeTz(
            TimeTz::parse(&s, &giggles_pg_types::temporal::ParserSettings::default()).map_err(
                |e| datetime_input::zone_word_error(&s, ty, temporal::parse_error(e, false)),
            )?,
        ),
        (Value::Text(s), DataType::Timestamp) => Value::Timestamp(
            Timestamp::parse(&s, &giggles_pg_types::temporal::ParserSettings::default()).map_err(
                |e| datetime_input::zone_word_error(&s, ty, temporal::parse_error(e, false)),
            )?,
        ),
        (Value::Text(s), DataType::TimestampTz) => Value::TimestampTz(
            TimestampTz::parse_in_session(
                &s,
                &giggles_pg_types::temporal::SessionSettings {
                    date_order: giggles_pg_types::temporal::DateOrder::default(),
                    zone: giggles_pg_types::temporal::SessionZone::Fixed(0),
                },
            )
            .map_err(|e| {
                datetime_input::zone_word_error(&s, ty, temporal::parse_error(e, false))
            })?,
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
            Value::Numeric(Numeric::parse(&s).map_err(|e| numeric_input_error(&s, e))?)
        }
        (Value::Text(s), DataType::Real) => {
            Value::Float4(Float4::parse(&s).map_err(float_input_error)?)
        }
        (Value::Text(s), DataType::DoublePrecision) => {
            Value::Float8(Float8::parse(&s).map_err(float_input_error)?)
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
        (Value::Text(s), DataType::Text | DataType::Bpchar) => Value::Text(s),
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
                return Err(SqlError::InvalidTextRepresentation);
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
            Self::Predicate(filter) => filter.infer(parameters, resolve, tables)?,
            Self::Case(case) => {
                for (condition, _) in &case.arms {
                    condition.infer(parameters, resolve, tables)?;
                }
                let results: Vec<_> = case
                    .arms
                    .iter()
                    .map(|(_, result)| result)
                    .chain(case.otherwise.iter())
                    .collect();
                for result in &results {
                    result.infer(None, parameters, resolve, tables)?;
                }
                let ty = common_result_type(
                    results
                        .iter()
                        .filter(|result| !result.aggregate_unknown(parameters))
                        .map(|result| {
                            result
                                .bind(resolve, tables, parameters)
                                .map(|bound| bound.ty)
                        })
                        .collect::<R<Vec<_>>>()?,
                )?;
                for result in results {
                    if result.aggregate_unknown(parameters) {
                        result.infer(Some(&ty), parameters, resolve, tables)?;
                    }
                }
            }
            Self::Unary(..) | Self::Binary(..) => {
                arithmetic::infer(self, parameters, resolve, tables)?
            }
            Self::Param(n) => {
                let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                if let Some(ty) = expected {
                    if slot.as_ref().is_some_and(|old| old != ty) {
                        return Err(SqlError::Type);
                    }
                    slot.get_or_insert_with(|| ty.clone());
                }
            }
            Self::Domain(a, _, target) => {
                let TypeDef::Domain { base, .. } = target.as_ref().map_err(Clone::clone)? else {
                    return Err(SqlError::Unsupported);
                };
                Self::Cast(a.clone(), base.clone()).infer(expected, parameters, resolve, tables)?;
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
            Self::Call(n, args)
                if matches!(
                    n.as_str(),
                    "\0array_subscript" | "array_length" | "\0json_get" | "\0json_text"
                ) =>
            {
                if args.len() != 2 {
                    return Err(SqlError::UndefinedFunction);
                }
                let first = if n.starts_with("\0json_") {
                    Some(&DataType::Jsonb)
                } else {
                    None
                };
                args[0].infer(first, parameters, resolve, tables)?;
                let key = if n.starts_with("\0json_") {
                    DataType::Text
                } else {
                    DataType::Integer
                };
                let hint = if args[1].aggregate_unknown(parameters) {
                    Some(&key)
                } else {
                    None
                };
                args[1].infer(hint, parameters, resolve, tables)?;
            }
            Self::Call(n, args) if n == CAST_TYPMOD => {
                let [inner, Self::Literal(Value::Int(_))] = args.as_slice() else {
                    return Err(SqlError::Syntax);
                };
                inner.infer(expected, parameters, resolve, tables)?;
            }
            Self::Call(n, args) if deferred_routine_error(n).is_some() => {
                // Validate ordinary arguments without inventing a signature for
                // unresolved parameters or binding aggregates as scalar calls.
                fn validate(
                    arg: &Scalar,
                    parameters: &[Option<DataType>],
                    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
                    tables: &[&TableDef],
                ) -> R<bool> {
                    let mut bindable = true;
                    match arg {
                        Scalar::Predicate(filter) => {
                            filter.visit_scalars(&mut |arg| {
                                bindable &= validate(arg, parameters, resolve, tables)?;
                                Ok(())
                            })?;
                        }
                        Scalar::Case(case) => {
                            for (condition, result) in &case.arms {
                                condition.visit_scalars(&mut |arg| {
                                    bindable &= validate(arg, parameters, resolve, tables)?;
                                    Ok(())
                                })?;
                                bindable &= validate(result, parameters, resolve, tables)?;
                            }
                            if let Some(result) = &case.otherwise {
                                bindable &= validate(result, parameters, resolve, tables)?;
                            }
                        }
                        Scalar::Param(n) => {
                            bindable = parameters.get(n - 1).is_some_and(Option::is_some);
                        }
                        Scalar::Cast(arg, _) => {
                            bindable = validate(arg, parameters, resolve, tables)?;
                        }
                        Scalar::Call(name, args) => {
                            for arg in args {
                                bindable &= validate(arg, parameters, resolve, tables)?;
                            }
                            bindable &= !matches!(name.as_str(), "count" | "sum" | "min" | "max");
                        }
                        Scalar::SubstringOrder(args) => {
                            for arg in args {
                                bindable &= validate(arg, parameters, resolve, tables)?;
                            }
                        }
                        _ => {}
                    }
                    if bindable {
                        arg.bind(resolve, tables, parameters)?;
                    }
                    Ok(bindable)
                }
                for arg in args {
                    arg.infer(None, parameters, resolve, tables)?;
                    validate(arg, parameters, resolve, tables)?;
                }
                return Err(deferred_routine_error(n).unwrap());
            }
            Self::Call(n, args) if n == "pg_xact_status" => {
                for arg in args {
                    arg.infer(Some(&DataType::BigInt), parameters, resolve, tables)?;
                }
            }
            Self::Call(n, args) if sequence_name(n) => {
                for (i, arg) in args.iter().enumerate() {
                    let target = match i {
                        0 => DataType::Text,
                        1 => DataType::BigInt,
                        _ => DataType::Boolean,
                    };
                    arg.infer(Some(&target), parameters, resolve, tables)?;
                }
            }
            Self::Call(n, args) if n == "overlay" => {
                Self::infer_overlay(args, parameters, resolve, tables)?;
            }
            Self::Call(n, args) if greatest::known_name(n) => {
                for arg in args {
                    arg.infer(None, parameters, resolve, tables)?;
                }
                let ty = greatest::hint(args, resolve, tables, parameters)?;
                for arg in args {
                    let expected = match arg {
                        Self::Param(slot)
                            if parameters.get(slot - 1).is_some_and(Option::is_some) =>
                        {
                            None
                        }
                        _ => Some(&ty),
                    };
                    arg.infer(expected, parameters, resolve, tables)?;
                }
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
                if matches!(n.as_str(), "mod" | "gcd" | "lcm")
                    && args.len() == 2
                    && args.iter().all(|a| a.aggregate_unknown(parameters))
                {
                    return Err(SqlError::AmbiguousFunction);
                }
                if n == "strpos" {
                    // Reject the missing bytea overload before text inference
                    // turns a declared argument into a parameter-type conflict.
                    for arg in args {
                        if arg
                            .bind(resolve, tables, parameters)
                            .is_ok_and(|a| a.ty == DataType::Bytes)
                        {
                            return Err(SqlError::UndefinedFunction);
                        }
                    }
                }
                let common = if n == "coalesce" {
                    coalesce_hint(args, resolve, tables, parameters)
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
                                    && (cast::explicit_target(&args[1])
                                        == Some(&DataType::TimestampTz)
                                        || args[1]
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
                    // Preserve declared parameter types for overload resolution at bind time.
                    let expected = if n != "coalesce"
                        && matches!(a, Self::Param(slot) if parameters.get(slot - 1).is_some_and(Option::is_some))
                    {
                        None
                    } else {
                        ty.as_ref()
                    };
                    a.infer(expected, parameters, resolve, tables)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

// PostgreSQL float4in/float8in: malformed text is 22P02, out-of-range is 22003.
fn float_input_error(error: giggles_pg_types::Error) -> SqlError {
    match error {
        giggles_pg_types::Error::Syntax => SqlError::InvalidTextRepresentation,
        e => pg_error(e),
    }
}

fn numeric_input_error(text: &str, error: giggles_pg_types::Error) -> SqlError {
    use giggles_pg_types::Error;
    match error {
        Error::Syntax | Error::Unimplemented | Error::Binary => SqlError::InvalidTextRepresentation,
        Error::Range | Error::TimeZoneDisplacement => SqlError::Overflow,
        Error::Limit if text.len() > giggles_pg_types::MAX_TEXT => SqlError::Limit,
        Error::Limit => {
            let text = text.trim_matches(|c: char| c.is_ascii_whitespace());
            let mut bytes = text.bytes().peekable();
            if matches!(bytes.peek(), Some(b'+' | b'-')) {
                bytes.next();
            }
            let mut digits = 0;
            let mut dot = false;
            while let Some(&b) = bytes.peek() {
                if b.is_ascii_digit() {
                    digits += 1;
                } else if b == b'.' && !dot {
                    dot = true;
                } else {
                    break;
                }
                bytes.next();
            }
            if digits == 0 {
                return SqlError::InvalidTextRepresentation;
            }
            if matches!(bytes.peek(), Some(b'e' | b'E')) {
                bytes.next();
                if matches!(bytes.peek(), Some(b'+' | b'-')) {
                    bytes.next();
                }
                let mut exponent_digits = 0;
                while bytes.peek().is_some_and(u8::is_ascii_digit) {
                    exponent_digits += 1;
                    bytes.next();
                }
                if exponent_digits == 0 {
                    return SqlError::InvalidTextRepresentation;
                }
            }
            if bytes.next().is_some() {
                SqlError::InvalidTextRepresentation
            } else {
                SqlError::Overflow
            }
        }
        Error::DivisionByZero => SqlError::DivisionByZero,
    }
}

fn pg_error(e: giggles_pg_types::Error) -> SqlError {
    match e {
        giggles_pg_types::Error::Limit => SqlError::Limit,
        giggles_pg_types::Error::Range => SqlError::Overflow,
        giggles_pg_types::Error::DivisionByZero => SqlError::DivisionByZero,
        giggles_pg_types::Error::TimeZoneDisplacement => SqlError::InvalidTimeZoneDisplacementValue,
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

pub(super) fn comparison_mismatch(_a: &DataType, _b: &DataType) -> SqlError {
    SqlError::UndefinedFunction
}

pub(in crate::relational) fn comparison_requires_filter(value: &Value, ty: &DataType) -> R<bool> {
    if matches!(value, Value::Null | Value::Text(_)) {
        return Ok(false);
    }
    let literal =
        Scalar::Literal(value.clone()).bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[])?;
    // Integer widths share their exact key representation. Other cross-type
    // comparisons must retain the original values in the full filter.
    Ok(literal.ty != *ty && !(integer(&literal.ty) && integer(ty)) && comparable(&literal.ty, ty))
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

pub(super) fn aggregate_numeric_average(sum: Value, count: i64, budget: &mut usize) -> R<Value> {
    let Value::Numeric(sum) = sum else {
        return Err(SqlError::Type);
    };
    let denominator = Numeric::aggregate_from_i64(count);
    reserve(budget, sum.aggregate_scratch_bound(&denominator))?;
    let out = sum.checked_div(&denominator).map_err(pg_error)?;
    reserve(budget, out.display_byte_bound())?;
    Ok(Value::Numeric(out))
}

impl BoundScalar {
    pub(in super::super) fn insert_assignment(self, target: &DataType) -> R<Self> {
        // Numeric character assignments use the existing text conversion, including
        // session float formatting. coerce_to_column applies the target typmod.
        if number(&self.ty) && matches!(target, DataType::Varchar | DataType::Bpchar) {
            return self.insert_assignment(&DataType::Text);
        }
        if &self.ty == target {
            return Ok(self);
        }
        if matches!(
            self.ty,
            DataType::Text | DataType::Varchar | DataType::Bpchar
        ) && matches!(
            target,
            DataType::Text | DataType::Varchar | DataType::Bpchar
        ) {
            return Ok(Self {
                ty: target.clone(),
                node: self.node,
            });
        }
        let node = if number(&self.ty) && number(target)
            || matches!(
                target,
                DataType::Text | DataType::Varchar | DataType::Bpchar
            ) && (number(&self.ty) || self.ty == DataType::Boolean)
        {
            cast_node(self, target)
        } else if assignment::compatible(&self.ty, target) {
            Node::SessionCast(Box::new(self), None)
        } else {
            return Err(SqlError::Type);
        };
        Ok(Self {
            ty: target.clone(),
            node,
        })
    }
}

#[cfg(test)]
mod xact_tests {
    use super::*;
    use crate::relational::clock::{ExecutionClock, StatementClock};
    use std::sync::{Arc, Mutex};

    fn clock() -> ExecutionClock {
        ExecutionClock::new(&StatementClock {
            unavailable: None,
            transaction_us: 0,
            statement_us: 0,
            zone: None,
            wall: None,
        })
    }
    fn bind(name: &str, args: Vec<Scalar>) -> R<BoundScalar> {
        let scalar = Scalar::Call(name.into(), args);
        let resolve = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
        let mut parameters = [None];
        scalar.infer(None, &mut parameters, &resolve, &[])?;
        scalar.bind(&resolve, &[], &parameters)
    }
    fn eval(bound: &mut BoundScalar, clock: &ExecutionClock, params: &[Value]) -> R<Value> {
        bound.attach_clock(Some(clock));
        bound.eval(
            &Tuple {
                values: vec![],
                version: 0,
            },
            params,
            &mut 0,
        )
    }
    #[test]
    fn current_ids_and_volatility() {
        let calls = Arc::new(Mutex::new(vec![]));
        let recorded = calls.clone();
        let clock = clock().with_xact(Arc::new(move |name, arg| {
            recorded.lock().unwrap().push((name.to_owned(), arg));
            Ok(Value::Int(42))
        }));
        for name in ["pg_current_xact_id", "pg_current_xact_id_if_assigned"] {
            let mut bound = bind(name, vec![]).unwrap();
            assert_eq!(bound.ty, DataType::BigInt);
            assert!(Scalar::Call(name.into(), vec![]).has_volatile_call());
            assert_eq!(bound.plan_constant(&mut 0), Ok(None));
            assert!(!bound.constraint_immutable());
            assert_eq!(eval(&mut bound, &clock, &[]), Ok(Value::Int(42)));
        }
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                ("pg_current_xact_id".into(), None),
                ("pg_current_xact_id_if_assigned".into(), None),
            ]
        );
    }
    #[test]
    fn status_arguments_and_null() {
        let calls = Arc::new(Mutex::new(vec![]));
        let recorded = calls.clone();
        let clock = clock().with_xact(Arc::new(move |name, arg| {
            recorded.lock().unwrap().push((name.to_owned(), arg));
            Ok(Value::Text("committed".into()))
        }));
        for arg in [
            Scalar::Literal(Value::Int(7)),
            Scalar::Literal(Value::Text("7".into())),
            Scalar::Param(1),
            Scalar::Cast(Box::new(Scalar::Literal(Value::Int(7))), DataType::SmallInt),
        ] {
            let mut bound = bind("pg_xact_status", vec![arg]).unwrap();
            assert_eq!(bound.ty, DataType::Text);
            assert_eq!(bound.plan_constant(&mut 0), Ok(None));
            assert_eq!(
                eval(&mut bound, &clock, &[Value::Int(7)]),
                Ok(Value::Text("committed".into()))
            );
        }
        let mut null = bind("pg_xact_status", vec![Scalar::Literal(Value::Null)]).unwrap();
        assert_eq!(eval(&mut null, &clock, &[]), Ok(Value::Null));
        assert_eq!(
            *calls.lock().unwrap(),
            vec![("pg_xact_status".into(), Some(7)); 4]
        );
        let resolve = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
        let mut parameters = [None];
        Scalar::Call("pg_xact_status".into(), vec![Scalar::Param(1)])
            .infer(None, &mut parameters, &resolve, &[])
            .unwrap();
        assert_eq!(parameters, [Some(DataType::BigInt)]);
    }
    #[test]
    fn invalid_signatures() {
        for (name, args) in [
            ("pg_current_xact_id", vec![Scalar::Literal(Value::Int(1))]),
            (
                "pg_current_xact_id_if_assigned",
                vec![Scalar::Literal(Value::Int(1))],
            ),
            ("pg_xact_status", vec![]),
            ("pg_xact_status", vec![Scalar::Literal(Value::Bool(true))]),
        ] {
            assert!(matches!(bind(name, args), Err(SqlError::UndefinedFunction)));
        }
    }
    #[test]
    fn missing_callback_and_error_bridge() {
        let clock = clock();
        for (name, args) in [
            ("pg_current_xact_id", vec![]),
            ("pg_current_xact_id_if_assigned", vec![]),
            ("pg_xact_status", vec![Scalar::Literal(Value::Int(7))]),
        ] {
            let mut bound = bind(name, args).unwrap();
            assert_eq!(eval(&mut bound, &clock, &[]), Err(SqlError::Unsupported));
        }
        let failing = clock.with_xact(Arc::new(|_, _| {
            Err(crate::CoreError::Storage("xact failure".into()))
        }));
        let mut bound = bind("pg_current_xact_id", vec![]).unwrap();
        assert_eq!(eval(&mut bound, &failing, &[]), Err(SqlError::Unsupported));
        assert!(
            matches!(clock.take_error(), Some(crate::CoreError::Storage(message)) if message == "xact failure")
        );
        assert!(failing.take_error().is_none());
    }
}
