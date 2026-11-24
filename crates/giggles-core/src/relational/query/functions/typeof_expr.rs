//! Expression-only PostgreSQL type introspection. Values remain textual.
use super::*;

pub(super) const NAME: &str = "pg_typeof";

pub(in crate::relational::query) fn name(ty: &DataType) -> &'static str {
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
        DataType::UuidArray => "uuid[]",
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
    let bound = arg.bind(resolve, tables, parameters)?;
    let display = if bound.result_oid() == random_uuid::VOID_OID {
        "void".into()
    } else if bound.result_oid() == json_aggregate::OID {
        "json".into()
    } else if let Some(ty) = bound.user_enum_type(tables) {
        user_types::enum_display(&ty, bound.ty == DataType::TextArray)
    } else if matches!(arg, Scalar::Literal(Value::Text(_) | Value::Null)) {
        "unknown".into()
    } else {
        name(&bound.ty).into()
    };
    Ok(BoundScalar {
        ty: DataType::RegType,
        node: Node::Call(
            NAME.into(),
            vec![
                bound,
                BoundScalar {
                    ty: DataType::Text,
                    node: Node::Literal(Value::Text(display)),
                },
            ],
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
    if let [Token::Word(name), Token::Symbol('('), Token::Op(sign), ..] = parser.tokens.as_slice()
        && matches!(name.as_str(), "time" | "timestamp" | "interval")
        && matches!(sign.as_str(), "-" | "+")
    {
        user_types::type_name_syntax_error(sign, &input);
        return Err(SqlError::Syntax);
    }
    let ty = match cast_type_with_diagnostics(&mut parser, true) {
        Ok((ty, _)) => ty,
        Err(SqlError::UndefinedObject) => {
            parser.at = 0;
            if let Ok(reference) = user_types::TypeReference::parse(&mut parser)
                && parser.at == parser.tokens.len()
                && let Some(name) = user_types::enum_regtype(&reference)
            {
                return Ok(Value::Text(name));
            }
            return Err(SqlError::UndefinedObject);
        }
        Err(error) => return Err(error),
    };
    if parser.at != parser.tokens.len() {
        return Err(SqlError::InvalidTextRepresentation);
    }
    Ok(Value::Text(name(&ty).into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Scalar {
        let mut parser = Parser {
            privilege_seen: false,
            privilege_casts: false,
            tokens: lex(sql).unwrap(),
            at: 0,
            params: BTreeSet::new(),
            relations: vec![],
        };
        let expression = Scalar::parse(&mut parser, 0).unwrap();
        assert_eq!(parser.at, parser.tokens.len());
        expression
    }

    #[test]
    fn volatile_argument_is_not_plan_constant() {
        let expression = parse("pg_typeof(nextval('ts'))");
        assert!(expression.has_volatile_call());
        let bound = expression
            .bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[])
            .unwrap();
        assert_eq!(bound.plan_constant(&mut 0), Ok(None));
        assert!(!bound.constraint_immutable());
    }

    #[test]
    fn constant_argument_raises_during_planning() {
        let bound = parse("pg_typeof(1/0)")
            .bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[])
            .unwrap();
        assert_eq!(bound.plan_constant(&mut 0), Err(SqlError::DivisionByZero));
    }

    #[test]
    fn row_argument_uses_each_rows_value_and_budget() {
        let table = TableDef {
            id: 0,
            name: "t".into(),
            columns: vec![crate::relational::ColumnDef {
                id: 0,
                name: "x".into(),
                data_type: DataType::Integer,
                not_null: false,
            }],
            primary_key: vec![],
        };
        let expression = parse("pg_typeof(x)")
            .bind(
                &|_, _| {
                    Ok(BoundColumn {
                        source: 0,
                        column: 0,
                    })
                },
                &[&table],
                &[],
            )
            .unwrap();
        assert_eq!(expression.plan_constant(&mut 0), Ok(None));
        let mut row = Tuple {
            source_versions: vec![],
            values: vec![vec![Value::Int(5)]],
            version: 0,
        };
        let mut budget = 0;
        let display = Value::Text("integer".into());
        assert_eq!(expression.eval(&row, &[], &mut budget), Ok(display.clone()));
        assert_eq!(
            budget,
            value_bytes(&row.values[0][0]) + value_bytes(&display)
        );
        row.values[0][0] = Value::Null;
        assert_eq!(expression.eval(&row, &[], &mut 0), Ok(display));
    }
}
