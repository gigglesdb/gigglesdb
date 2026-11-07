//! Bounded default syntax, assignment validation, and PostgreSQL-style stored text.
use super::*;

#[derive(Debug, Clone, Default)]
pub(super) struct SequenceSpec {
    pub identity: Option<bool>,
    start: Option<i64>,
    increment: Option<i64>,
    min: Option<i64>,
    max: Option<i64>,
    cycle: bool,
}
fn integer(p: &mut Parser) -> R<i64> {
    match p.expr()? {
        Expr::Literal(Value::Int(n)) => Ok(n),
        _ => Err(SqlError::InvalidParameterValue),
    }
}
pub(super) fn identity(p: &mut Parser) -> R<SequenceSpec> {
    let always = if p.word("always") {
        true
    } else {
        p.need("by")?;
        p.need("default")?;
        false
    };
    p.need("as")?;
    p.need("identity")?;
    let mut spec = SequenceSpec {
        identity: Some(always),
        ..Default::default()
    };
    if p.sym('(') {
        let mut seen = BTreeSet::new();
        while !p.sym(')') {
            let (key, value) = if p.word("start") {
                p.word("with");
                ("start", Some(integer(p)?))
            } else if p.word("increment") {
                p.word("by");
                ("increment", Some(integer(p)?))
            } else if p.word("minvalue") {
                ("min", Some(integer(p)?))
            } else if p.word("maxvalue") {
                ("max", Some(integer(p)?))
            } else if p.word("cycle") {
                spec.cycle = true;
                ("cycle", None)
            } else if p.word("no") {
                if p.word("cycle") {
                    spec.cycle = false;
                    ("cycle", None)
                } else if p.word("minvalue") {
                    ("min", None)
                } else if p.word("maxvalue") {
                    ("max", None)
                } else {
                    return Err(SqlError::Unsupported);
                }
            } else {
                return Err(SqlError::Unsupported);
            };
            if !seen.insert(key) {
                return Err(SqlError::Syntax);
            }
            match key {
                "start" => spec.start = value,
                "increment" => spec.increment = value,
                "min" => spec.min = value,
                "max" => spec.max = value,
                _ => {}
            }
        }
    }
    Ok(spec)
}
fn sequence_definition(
    spec: &SequenceSpec,
    ty: &DataType,
    name: String,
    owner: (u64, u64),
) -> R<catalog_ext::SequenceDef> {
    let (lo, hi) = match ty {
        DataType::SmallInt => (i64::from(i16::MIN), i64::from(i16::MAX)),
        DataType::Integer => (i64::from(i32::MIN), i64::from(i32::MAX)),
        DataType::BigInt => (i64::MIN, i64::MAX),
        _ => return Err(SqlError::Type),
    };
    let increment = spec.increment.unwrap_or(1);
    let min = spec.min.unwrap_or(if increment < 0 { lo } else { 1 });
    let max = spec.max.unwrap_or(if increment < 0 { -1 } else { hi });
    let start = spec.start.unwrap_or(if increment < 0 { max } else { min });
    if increment == 0 || min >= max || min < lo || max > hi || start < min || start > max {
        return Err(SqlError::InvalidParameterValue);
    }
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| SqlError::Unsupported)?;
    Ok(catalog_ext::SequenceDef {
        name,
        data_type: ty.clone(),
        start,
        increment,
        min,
        max,
        cache: 1,
        cycle: spec.cycle,
        owned_by: Some(owner),
        identity: spec.identity.is_some(),
        incarnation: u128::from_le_bytes(bytes),
    })
}

pub(super) fn install_create(
    state: &mut Relational,
    table: TableDef,
    defaults: &[Option<String>],
    sequences: &[Option<SequenceSpec>],
    typmods: &[Option<i32>],
    next_id: u64,
    epoch: u64,
) -> R<()> {
    let mut candidate = state.clone();
    candidate.next_id = next_id;
    candidate.epoch = epoch;
    candidate.rows.insert(table.id, BTreeMap::new());
    candidate.tables.insert(table.name.clone(), table.clone());
    for ((column, _default), (spec, typmod)) in table
        .columns
        .iter()
        .zip(defaults)
        .zip(sequences.iter().zip(typmods))
    {
        if let Some(typmod) = typmod {
            candidate.ext.columns.entry(column.id).or_default().typmod = Some(*typmod);
        }
        if let Some(spec) = spec {
            let name = candidate.choose_sequence_name(&table.name, &column.name);
            let definition =
                sequence_definition(spec, &column.data_type, name.clone(), (table.id, column.id))?;
            let id = candidate.add_sequence(definition)?;
            if let Some(always) = spec.identity {
                candidate.set_column_identity(
                    column.id,
                    Some(catalog_ext::Identity {
                        always,
                        sequence_id: id,
                    }),
                )?;
            } else {
                candidate.set_column_default(
                    column.id,
                    Some(format!(
                        "nextval({}::regclass)",
                        quoted(&sequence_reference(&name))
                    )),
                )?;
            }
        }
    }
    for ((column, default), spec) in table.columns.iter().zip(defaults).zip(sequences) {
        if spec.is_none()
            && let Some(default) = default
            && default != "NULL"
        {
            let mut parser = Parser {
                tokens: lex(&format!("{default},"))?,
                at: 0,
                params: BTreeSet::new(),
            };
            let mut value = expression(&mut parser, 0)?;
            value.resolve(&candidate)?;
            candidate.set_column_default(column.id, Some(value.canonical(&column.data_type)?))?;
        }
    }
    candidate.validate_ext()?;
    *state = candidate;
    Ok(())
}

pub(super) fn default_sql(state: &Relational, column: &ColumnDef) -> Option<String> {
    if let Some(identity) = state.column_identity(column.id) {
        state.ext.sequences.get(&identity.sequence_id).map(|seq| {
            format!(
                "nextval({}::regclass)",
                quoted(&sequence_reference(&seq.name))
            )
        })
    } else {
        state.column_default(column.id).map(str::to_owned)
    }
}
pub(super) fn check_explicit(state: &Relational, column: &ColumnDef) -> R<()> {
    if state.column_identity(column.id).is_some_and(|i| i.always) {
        Err(SqlError::GeneratedAlways)
    } else {
        Ok(())
    }
}
pub(super) fn dependencies(sql: &str) -> R<Vec<String>> {
    let mut parser = Parser {
        tokens: lex(&format!("{sql},"))?,
        at: 0,
        params: BTreeSet::new(),
    };
    let value = expression(&mut parser, 0)?;
    if parser.at + 1 != parser.tokens.len() {
        return Err(SqlError::Unsupported);
    }
    let mut values = vec![];
    fn collect(value: &Expression, values: &mut Vec<String>) {
        match value {
            Expression::Sequence(name) => values.push(name.clone()),
            Expression::Cast(value, _) | Expression::Negative(value) => collect(value, values),
            _ => {}
        }
    }
    collect(&value, &mut values);
    Ok(values)
}

fn type_name(ty: &DataType) -> &'static str {
    match ty {
        DataType::SmallInt => "smallint",
        DataType::Integer => "integer",
        DataType::BigInt => "bigint",
        DataType::Boolean => "boolean",
        DataType::Numeric => "numeric",
        DataType::Real => "real",
        DataType::DoublePrecision => "double precision",
        DataType::Text => "text",
        DataType::Bytes => "bytea",
        DataType::Date => "date",
        DataType::Time => "time without time zone",
        DataType::TimeTz => "time with time zone",
        DataType::Timestamp => "timestamp without time zone",
        DataType::TimestampTz => "timestamp with time zone",
        DataType::Interval => "interval",
    }
}
fn quoted(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
fn sequence_reference(name: &str) -> String {
    if name
        .bytes()
        .enumerate()
        .all(|(i, b)| b == b'_' || b.is_ascii_lowercase() || i > 0 && b.is_ascii_digit())
    {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[derive(Clone)]
enum Expression {
    Sequence(String),
    Number(String),
    String(String),
    Boolean(bool),
    Null,
    Clock(String, DataType),
    Negative(Box<Expression>),
    Cast(Box<Expression>, DataType),
}
impl Expression {
    fn numeric_literal(&self) -> bool {
        match self {
            Self::Number(_) => true,
            Self::Cast(value, ty) => {
                (value.numeric_literal()
                    || matches!(value.as_ref(),Self::String(s) if Numeric::parse(s).is_ok()))
                    && matches!(
                        ty,
                        DataType::SmallInt
                            | DataType::Integer
                            | DataType::BigInt
                            | DataType::Numeric
                            | DataType::Real
                            | DataType::DoublePrecision
                    )
            }
            _ => false,
        }
    }
    fn resolve(&mut self, state: &Relational) -> R<()> {
        match self {
            Self::Sequence(name) => {
                let mut p = Parser {
                    tokens: lex(name)?,
                    at: 0,
                    params: BTreeSet::new(),
                };
                let normalized = p.table_name().map_err(|_| SqlError::UnknownTable)?;
                if p.at != p.tokens.len() {
                    return Err(SqlError::UnknownTable);
                }
                let (_, sequence) = state
                    .sequence_by_name(&normalized)
                    .ok_or(SqlError::UnknownTable)?;
                *name = sequence_reference(&sequence.name);
            }
            Self::Cast(value, _) | Self::Negative(value) => value.resolve(state)?,
            _ => {}
        }
        Ok(())
    }

    fn ty(&self) -> DataType {
        match self {
            Self::Sequence(_) => DataType::BigInt,
            Self::Number(n) => number_type(n),
            Self::String(_) | Self::Null => DataType::Text,
            Self::Boolean(_) => DataType::Boolean,
            Self::Clock(_, ty) | Self::Cast(_, ty) => ty.clone(),
            Self::Negative(value) => value.ty(),
        }
    }
    fn sql(&self) -> String {
        match self {
            Self::Sequence(name) => format!("nextval({}::regclass)", quoted(name)),
            Self::Number(n) => n.clone(),
            Self::String(s) => quoted(s),
            Self::Boolean(b) => b.to_string(),
            Self::Null => "NULL".into(),
            Self::Clock(s, _) => s.clone(),
            Self::Negative(value) => format!("-({})", value.sql()),
            Self::Cast(value, ty) => format!("({})::{}", value.sql(), type_name(ty)),
        }
    }
    fn canonical(&self, target: &DataType) -> R<String> {
        match self {
            Self::Sequence(name) => Ok(format!("nextval({}::regclass)", quoted(name))),
            Self::Number(n) => number_text(n),
            Self::String(_) => constant_text(&self.sql(), target),
            Self::Boolean(b) => Ok(b.to_string()),
            Self::Null => Ok("NULL".into()),
            Self::Clock(s, _) => Ok(s.clone()),
            Self::Negative(value) => {
                if let Self::Number(n) = value.as_ref() {
                    number_text(&format!("-{n}"))
                } else {
                    Ok(format!("(- {})", value.canonical(&value.ty())?))
                }
            }
            Self::Cast(value, ty) => {
                if matches!(value.as_ref(), Self::String(_) | Self::Null) {
                    constant_text(&self.sql(), ty)
                } else if value.ty() == *ty {
                    value.canonical(ty)
                } else {
                    Ok(format!(
                        "({})::{}",
                        value.canonical(&value.ty())?,
                        type_name(ty)
                    ))
                }
            }
        }
    }
}
fn number_type(n: &str) -> DataType {
    if n.parse::<i32>().is_ok() {
        DataType::Integer
    } else if n.parse::<i64>().is_ok() {
        DataType::BigInt
    } else {
        DataType::Numeric
    }
}
fn number_text(n: &str) -> R<String> {
    let ty = number_type(n);
    let display = match ty {
        DataType::Integer | DataType::BigInt => {
            n.parse::<i64>().map_err(|_| SqlError::Type)?.to_string()
        }
        _ => Numeric::parse(n).map_err(|_| SqlError::Type)?.to_string(),
    };
    Ok(match ty {
        DataType::Integer if !display.starts_with('-') => display,
        DataType::Numeric if !display.starts_with('-') && display.contains('.') => display,
        _ => format!("{}::{}", quoted(&display), type_name(&ty)),
    })
}
fn constant_text(sql: &str, ty: &DataType) -> R<String> {
    let value =
        query::InsertExpressions::default_value(sql, ty, None, true)?.ok_or(SqlError::Type)?;
    Ok(match value {
        Value::Int(n) if *ty == DataType::Integer && n >= 0 => n.to_string(),
        Value::Int(n) => format!("{}::{}", quoted(&n.to_string()), type_name(ty)),
        Value::Numeric(n) => format!("{}::numeric", quoted(&n.to_string())),
        Value::Bool(b) => b.to_string(),
        Value::Text(s) => format!("{}::text", quoted(&s)),
        Value::Float4(v) => format!("{}::real", quoted(&v.to_text())),
        Value::Float8(v) => format!("{}::double precision", quoted(&v.to_text())),
        Value::Bytes(bytes) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut text = String::with_capacity(2 + bytes.len() * 2);
            text.push_str("\\x");
            for byte in bytes {
                text.push(HEX[usize::from(byte >> 4)] as char);
                text.push(HEX[usize::from(byte & 15)] as char);
            }
            format!("{}::bytea", quoted(&text))
        }
        Value::Null => "NULL".into(),
        Value::Date(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::Time(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::TimeTz(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::Timestamp(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::TimestampTz(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::Interval(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
    })
}
pub(super) fn cast_type(p: &mut Parser) -> R<DataType> {
    let Some(Token::Word(word)) = p.tokens.get(p.at).cloned() else {
        return Err(SqlError::Unsupported);
    };
    p.at += 1;
    Ok(match word.as_str() {
        "int" | "int4" | "integer" => DataType::Integer,
        "int2" | "smallint" => DataType::SmallInt,
        "int8" | "bigint" => DataType::BigInt,
        "bool" | "boolean" => DataType::Boolean,
        "numeric" | "decimal" => DataType::Numeric,
        "real" | "float4" => DataType::Real,
        "float8" => DataType::DoublePrecision,
        "double" => {
            p.need("precision")?;
            DataType::DoublePrecision
        }
        "text" => DataType::Text,
        "bytea" => DataType::Bytes,
        "date" => DataType::Date,
        "time" | "timestamp" => {
            let with = p.word("with");
            if with || p.word("without") {
                p.need("time")?;
                p.need("zone")?;
            }
            match (word.as_str(), with) {
                ("time", false) => DataType::Time,
                ("time", true) => DataType::TimeTz,
                (_, false) => DataType::Timestamp,
                (_, true) => DataType::TimestampTz,
            }
        }
        "timetz" => DataType::TimeTz,
        "timestamptz" => DataType::TimestampTz,
        "interval" => DataType::Interval,
        _ => return Err(SqlError::Unsupported),
    })
}
fn expression(p: &mut Parser, depth: usize) -> R<Expression> {
    if depth > 16 {
        return Err(SqlError::Limit);
    }
    if matches!(p.tokens.get(p.at), Some(Token::Op(s)) if s == "-") {
        p.at += 1;
        let value = expression(p, depth + 1)?;
        if !value.numeric_literal() {
            return Err(SqlError::Unsupported);
        }
        return Ok(Expression::Negative(Box::new(value)));
    }
    let mut value = if p.sym('(') {
        let value = expression(p, depth + 1)?;
        p.punct(')')?;
        value
    } else {
        let token = p.tokens.get(p.at).cloned().ok_or(SqlError::Syntax)?;
        p.at += 1;
        match token {
            Token::String(s) => Expression::String(s),
            Token::Number(n) => Expression::Number(n),
            Token::Word(w) => match w.as_str() {
                "nextval" => {
                    p.punct('(')?;
                    let Some(Token::String(name)) = p.tokens.get(p.at).cloned() else {
                        return Err(SqlError::Unsupported);
                    };
                    p.at += 1;
                    if p.sym(':') {
                        p.punct(':')?;
                        p.need("regclass")?;
                    }
                    p.punct(')')?;
                    Expression::Sequence(name)
                }
                "null" => Expression::Null,
                "true" | "false" => Expression::Boolean(w == "true"),
                "now" | "transaction_timestamp" | "statement_timestamp" | "clock_timestamp" => {
                    p.punct('(')?;
                    p.punct(')')?;
                    Expression::Clock(format!("{w}()"), DataType::TimestampTz)
                }
                "current_date" | "current_time" | "current_timestamp" | "localtime"
                | "localtimestamp" => {
                    let mut sql = w.to_uppercase();
                    if w != "current_date" && p.sym('(') {
                        if matches!(p.tokens.get(p.at), Some(Token::Op(sign)) if sign == "-") {
                            return Err(SqlError::Syntax);
                        }
                        let Some(Token::Number(n)) = p.tokens.get(p.at) else {
                            return Err(SqlError::Unsupported);
                        };
                        let precision = n.parse::<u32>().map_err(|_| SqlError::Unsupported)?.min(6);
                        sql.push_str(&format!("({precision})"));
                        p.at += 1;
                        p.punct(')')?;
                    }
                    let ty = match w.as_str() {
                        "current_date" => DataType::Date,
                        "current_time" => DataType::TimeTz,
                        "localtime" => DataType::Time,
                        "localtimestamp" => DataType::Timestamp,
                        _ => DataType::TimestampTz,
                    };
                    Expression::Clock(sql, ty)
                }
                _ => return Err(SqlError::Unsupported),
            },
            _ => return Err(SqlError::Unsupported),
        }
    };
    if p.tokens.get(p.at) == Some(&Token::Symbol(':'))
        && p.tokens.get(p.at + 1) == Some(&Token::Symbol(':'))
    {
        p.at += 2;
        value = Expression::Cast(Box::new(value), cast_type(p)?);
    }
    Ok(value)
}

pub(super) fn parse(p: &mut Parser, target: &DataType) -> R<String> {
    let value = expression(p, 0)?;
    if !matches!(p.tokens.get(p.at), Some(Token::Symbol(',' | ')')))
        && !matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "not" || w == "null" || w == "primary")
    {
        return Err(SqlError::Unsupported);
    }
    query::InsertExpressions::default_value(&value.sql(), target, None, false)?;
    value.canonical(target)
}

pub(super) fn evaluate_sql(
    sql: &str,
    target: &DataType,
    clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    let mut parser = Parser {
        tokens: lex(&format!("{sql},"))?,
        at: 0,
        params: BTreeSet::new(),
    };
    let value = expression(&mut parser, 0)?;
    if parser.at + 1 != parser.tokens.len()
        || parser.tokens.get(parser.at) != Some(&Token::Symbol(','))
    {
        return Err(SqlError::Unsupported);
    }
    query::InsertExpressions::default_value(&value.sql(), target, clock, true)?
        .ok_or(SqlError::Type)
}
pub(super) fn evaluate(
    state: &Relational,
    column: &ColumnDef,
    clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    let value = match default_sql(state, column) {
        Some(sql) => evaluate_sql(&sql, &column.data_type, clock)?,
        None => Value::Null,
    };
    apply_typmod(value, state.column_typmod(column.id))
}
fn apply_typmod(value: Value, modifier: i32) -> R<Value> {
    if modifier == -1 {
        return Ok(value);
    }
    let temporal = |_| SqlError::DatetimeFieldOverflow;
    Ok(match value {
        Value::Numeric(v) => Value::Numeric(
            v.apply_typmod(
                giggles_pg_types::typmod::NumericTypmod::from_typmod(modifier)
                    .ok_or(SqlError::Type)?,
            )
            .map_err(|_| SqlError::Overflow)?,
        ),
        Value::Time(v) => Value::Time(v.with_precision(modifier as u32).map_err(temporal)?),
        Value::TimeTz(v) => Value::TimeTz(v.with_precision(modifier as u32).map_err(temporal)?),
        Value::Timestamp(v) => {
            Value::Timestamp(v.with_precision(modifier as u32).map_err(temporal)?)
        }
        Value::TimestampTz(v) => {
            Value::TimestampTz(v.with_precision(modifier as u32).map_err(temporal)?)
        }
        Value::Interval(v) => {
            use giggles_pg_types::temporal::IntervalField;
            let mask = modifier >> 16;
            let last = if mask & 4096 != 0 {
                IntervalField::Second
            } else if mask & 2048 != 0 {
                IntervalField::Minute
            } else if mask & 1024 != 0 {
                IntervalField::Hour
            } else if mask & 8 != 0 {
                IntervalField::Day
            } else if mask & 2 != 0 {
                IntervalField::Month
            } else {
                IntervalField::Year
            };
            let digits = modifier & 0xffff;
            Value::Interval(
                v.with_fields(last, if digits == 65535 { 6 } else { digits as u32 })
                    .map_err(temporal)?,
            )
        }
        v => v,
    })
}
pub(super) fn row(
    state: &Relational,
    table: &TableDef,
    supplied: &[usize],
    clock: Option<&clock::ExecutionClock>,
) -> R<Vec<Value>> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if supplied.contains(&index) || column.name == HIDDEN_ROWID {
                Ok(Value::Null)
            } else {
                evaluate(state, column, clock)
            }
        })
        .collect()
}

pub(super) fn install_alter(
    state: &mut Relational,
    column: &ColumnDef,
    sql: Option<&str>,
) -> R<()> {
    let value = if let Some(sql) = sql {
        let mut parser = Parser {
            tokens: lex(&format!("{sql},"))?,
            at: 0,
            params: BTreeSet::new(),
        };
        let mut value = expression(&mut parser, 0)?;
        value.resolve(state)?;
        let sql = value.canonical(&column.data_type)?;
        query::InsertExpressions::default_value(&value.sql(), &column.data_type, None, false)?;
        Some(sql)
    } else {
        None
    };
    state.set_column_default(column.id, value)
}

pub(super) fn parse_alter_untyped(p: &mut Parser) -> R<String> {
    Ok(expression(p, 0)?.sql())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sequence_metadata_uses_existing_wire_and_unique_incarnations() {
        let mut state = Relational::default();
        let sql = "CREATE TABLE meta (a smallserial,b bigserial,c integer GENERATED ALWAYS AS IDENTITY (INCREMENT BY -1))";
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        let mut second = state.clone();
        state.execute(&p, &[], None).unwrap();
        second.execute(&p, &[], None).unwrap();
        assert_eq!(state.ext.ext_version, 1);
        assert_eq!(state.ext.sequences.len(), 3);
        let table = &state.tables["meta"];
        for column in table.logical_columns() {
            assert!(column.not_null);
        }
        let (_, small) = state.sequence_by_name("meta_a_seq").unwrap();
        assert_eq!(
            (small.start, small.increment, small.min, small.max),
            (1, 1, 1, 32767)
        );
        assert_eq!(small.owned_by, Some((table.id, table.columns[0].id)));
        let (_, big) = state.sequence_by_name("meta_b_seq").unwrap();
        assert_eq!(big.max, i64::MAX);
        let (_, descending) = state.sequence_by_name("meta_c_seq").unwrap();
        assert_eq!(
            (
                descending.start,
                descending.increment,
                descending.min,
                descending.max
            ),
            (-1, -1, i64::from(i32::MIN), -1)
        );
        assert!(state.column_default(table.columns[2].id).is_none());
        assert!(state.column_identity(table.columns[2].id).unwrap().always);
        assert_ne!(
            small.incarnation,
            second.sequence_by_name("meta_a_seq").unwrap().1.incarnation
        );
    }
    #[test]
    fn canonical_defaults_preserve_expression_types_and_casts() {
        for (input, target, expected) in [
            ("'x'", DataType::Text, "'x'::text"),
            ("0", DataType::Integer, "0"),
            ("0007", DataType::Integer, "7"),
            ("01.20", DataType::Numeric, "1.20"),
            ("1e2", DataType::Numeric, "'100'::numeric"),
            ("2147483648", DataType::BigInt, "'2147483648'::bigint"),
            ("'12'::bigint", DataType::BigInt, "'12'::bigint"),
            ("'-7'::smallint", DataType::SmallInt, "'-7'::smallint"),
            ("NULL::numeric", DataType::Numeric, "NULL"),
            ("(-2147483648)", DataType::Integer, "'-2147483648'::integer"),
            (
                "-2147483648::bigint",
                DataType::BigInt,
                "(- '2147483648'::bigint)",
            ),
            ("'true'::boolean", DataType::Boolean, "true"),
            ("'1.5'::real", DataType::Real, "'1.5'::real"),
            (
                "'NaN'::double precision",
                DataType::DoublePrecision,
                "'NaN'::double precision",
            ),
            ("'Infinity'::real", DataType::Real, "'Infinity'::real"),
            ("'abc'::bytea", DataType::Bytes, r"'\x616263'::bytea"),
            (r"'\x00'::bytea", DataType::Bytes, r"'\x00'::bytea"),
            (
                "'2020-01-02 03:04:05+00'::timestamptz",
                DataType::TimestampTz,
                "'2020-01-02 03:04:05+00'::timestamp with time zone",
            ),
            (
                "'2020-01-02 03:04:05'::timestamp",
                DataType::Timestamp,
                "'2020-01-02 03:04:05'::timestamp without time zone",
            ),
            ("'1 day'::interval", DataType::Interval, "'1 day'::interval"),
            ("false", DataType::Boolean, "false"),
            ("-7", DataType::BigInt, "'-7'::integer"),
            ("-1.25", DataType::DoublePrecision, "'-1.25'::numeric"),
            ("'12'::int", DataType::Integer, "12"),
            ("7::smallint", DataType::SmallInt, "(7)::smallint"),
            ("7::bigint", DataType::BigInt, "(7)::bigint"),
            ("-7::bigint", DataType::Integer, "(- (7)::bigint)"),
            (
                "CURRENT_TIMESTAMP(7)",
                DataType::TimestampTz,
                "CURRENT_TIMESTAMP(6)",
            ),
            ("NULL", DataType::Integer, "NULL"),
        ] {
            let mut p = Parser {
                tokens: lex(&format!("{input},")).unwrap(),
                at: 0,
                params: BTreeSet::new(),
            };
            assert_eq!(parse(&mut p, &target).unwrap(), expected, "{input}");
            let mut p = Parser {
                tokens: lex(&format!("{expected},")).unwrap(),
                at: 0,
                params: BTreeSet::new(),
            };
            assert_eq!(
                parse(&mut p, &target).unwrap(),
                expected,
                "stored {expected}"
            );
        }
    }
}
