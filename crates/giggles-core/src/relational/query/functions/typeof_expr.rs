//! Expression-only PostgreSQL type introspection. Values remain textual.
use super::*;

pub(super) fn name(ty: &DataType) -> &'static str {
    match ty {
        DataType::Boolean => "boolean",
        DataType::SmallInt => "smallint",
        DataType::Integer => "integer",
        DataType::BigInt => "bigint",
        DataType::Text => "text",
        DataType::RegType => "regtype",
        DataType::Bytes => "bytea",
        DataType::Numeric => "numeric",
        DataType::Real => "real",
        DataType::DoublePrecision => "double precision",
        DataType::Date => "date",
        DataType::Time => "time without time zone",
        DataType::TimeTz => "time with time zone",
        DataType::Timestamp => "timestamp without time zone",
        DataType::TimestampTz => "timestamp with time zone",
        DataType::Interval => "interval",
        DataType::Varchar => "character varying",
        DataType::Bpchar => "character",
        DataType::Uuid => "uuid",
        DataType::Jsonb => "jsonb",
        DataType::IntArray => "integer[]",
        DataType::BigIntArray => "bigint[]",
        DataType::TextArray => "text[]",
    }
}

pub(super) fn bind(
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let [arg] = args else {
        return Err(SqlError::UndefinedFunction);
    };
    // pg_typeof needs only this PG18 routine's declared return type. Its
    // volatile value is never generated, just as clock arguments aren't run.
    let bound = if matches!(arg, Scalar::Call(name, args) if name == "gen_random_uuid" && args.is_empty()) {
        BoundScalar { ty: DataType::Uuid, node: Node::Literal(Value::Null) }
    } else {
        arg.bind(resolve, tables, parameters)?
    };
    let display = if let Some(ty) = bound.enum_type(tables) {
        ty.name().to_owned()
    } else if matches!(arg, Scalar::Literal(Value::Text(_) | Value::Null)) {
        "unknown".into()
    } else {
        name(&bound.ty).into()
    };
    Ok(BoundScalar {
        ty: DataType::RegType,
        node: Node::Call(
            "pg_typeof".into(),
            vec![BoundScalar {
                ty: DataType::Text,
                node: Node::Literal(Value::Text(display)),
            }],
        ),
    })
}

pub(super) fn regtype(value: Value) -> R<Value> {
    let Value::Text(input) = value else {
        return Err(SqlError::Type);
    };
    if input == "unknown" {
        return Ok(Value::Text(input));
    }
    let mut parser = Parser { privilege_seen: false, privilege_casts: false,
        tokens: lex(&input)?,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let (ty, _) = cast_type(&mut parser)?;
    if parser.at != parser.tokens.len() {
        return Err(SqlError::InvalidTextRepresentation);
    }
    Ok(Value::Text(name(&ty).into()))
}
