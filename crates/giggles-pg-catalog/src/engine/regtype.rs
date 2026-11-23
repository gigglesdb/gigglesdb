use super::*;
use giggles_pg_types::{
    character::{CharKind, CharTypmod},
    typmod::{
        INTERVAL_FULL_RANGE, NumericTypmod, TemporalType, TypmodError, float_precision_is_real,
        interval_typmod,
    },
};

impl Runtime<'_> {
    pub(super) fn regtype(&mut self, input: &str) -> R<Value> {
        let input = input.trim();
        if input.is_empty() {
            return Ok(Value::Null);
        }
        if let Ok(oid) = input.parse::<u32>() {
            return Ok(Value::Int(i64::from(oid)));
        }
        let (name, token_count) = TypeName::parse(input).map_err(|error| match error {
            Error::TypeNameSyntax { near, .. } => Error::TypeNameSyntax {
                near,
                input: Some(input.into()),
            },
            error => error,
        })?;
        self.charge(token_count)?;
        let path = name.search_path(self.snapshot);
        let types = &self.catalogs["pg_type"].rows;
        let namespaces = &self.catalogs["pg_namespace"].rows;
        self.charge(path.len() * (types.len() + namespaces.len()))?;
        Ok(name
            .resolve(path, types, namespaces)?
            .map_or(Value::Null, |oid| Value::Int(oid.into())))
    }
}

struct TypeName {
    schema: Option<String>,
    explicit_schema: bool,
    name: String,
    array: bool,
    numeric_modifier: Option<(i32, i32)>,
    custom_modifiers: Vec<String>,
}

impl TypeName {
    fn parse(input: &str) -> R<(Self, usize)> {
        if input.len() > MAX_SQL {
            return Err(Error::Limit);
        }
        let mut tokens = Tokenizer::new(&PostgreSqlDialect {}, input)
            .tokenize()
            .map_err(|_| Error::Syntax)?;
        if tokens.len() > 256 {
            return Err(Error::Limit);
        }
        let token_count = tokens.len();
        tokens.retain(|token| !matches!(token, Token::Whitespace(_)));
        let explicit_schema = matches!(tokens.as_slice(), [Token::Word(_), Token::Period, ..]);
        let numeric_modifier = take_numeric_modifier(&mut tokens)?;
        let mut custom_modifiers = take_named_modifiers(&mut tokens)?;
        let mut parser = Parser::new(&PostgreSqlDialect {})
            .with_recursion_limit(32)
            .with_tokens(tokens);
        let mut ty = parser.parse_data_type().map_err(|_| Error::Syntax)?;
        if parser.peek_token().token != Token::EOF {
            return Err(Error::Syntax);
        }
        let mut array = false;
        while let a::DataType::Array(a::ArrayElemTypeDef::SquareBracket(inner, _)) = ty {
            array = true;
            ty = *inner;
        }
        let parts = if let a::DataType::Custom(name, modifiers) = &ty {
            if !modifiers.is_empty() {
                custom_modifiers.clone_from(modifiers);
            }
            object(name)
        } else {
            let float_name = validate_modifiers(&ty)?;
            let mut name = ty.to_string().to_ascii_lowercase();
            if let (Some(start), Some(end)) = (name.find('('), name.find(')')) {
                name.replace_range(start..=end, "");
            }
            let builtin = match name.trim() {
                "boolean" | "bool" => "bool",
                "smallint" | "int2" => "int2",
                "int" | "integer" | "int4" => "int4",
                "bigint" | "int8" => "int8",
                "real" | "float4" => "float4",
                "double precision" | "float8" | "float" => "float8",
                "decimal" | "numeric" => "numeric",
                "char" | "character" => "bpchar",
                "varchar" | "character varying" | "char varying" => "varchar",
                "bit varying" => "varbit",
                "time" | "time without time zone" => "time",
                "time with time zone" => "timetz",
                "timestamp" | "timestamp without time zone" => "timestamp",
                "timestamp with time zone" => "timestamptz",
                other => other,
            };
            vec!["pg_catalog".into(), float_name.unwrap_or(builtin).into()]
        };
        let (schema, name) = match parts.as_slice() {
            [name] => (None, name),
            [schema, name] => (Some(schema.as_str()), name),
            _ => return Err(Error::Syntax),
        };
        Ok((
            Self {
                schema: schema.map(str::to_owned),
                explicit_schema,
                name: name.clone(),
                array,
                numeric_modifier,
                custom_modifiers,
            },
            token_count,
        ))
    }

    fn search_path(&self, snapshot: &CatalogSnapshot) -> Vec<String> {
        let mut path = snapshot.search_path.clone();
        if !path.iter().any(|s| s == "pg_catalog") {
            path.insert(0, "pg_catalog".into());
        }
        self.schema.as_ref().map_or(path, |s| vec![s.clone()])
    }

    fn resolve(
        &self,
        path: Vec<String>,
        types: &[Vec<Value>],
        namespaces: &[Vec<Value>],
    ) -> R<Option<u32>> {
        for schema in path {
            if let Some(namespace) = namespaces
                .iter()
                .find(|r| matches!(&r[1], Value::Text(n) if n == &schema))
                && let Some(row) = types.iter().find(|r| {
                    matches!(&r[1], Value::Text(n) if n == &self.name) && r[2] == namespace[0]
                })
            {
                if schema == "pg_catalog" {
                    let modifier_oid = if row[12] == Value::Int(750) {
                        &row[10] // arrays use their element type's modifier input
                    } else {
                        &row[0]
                    };
                    self.validate_builtin_modifiers(modifier_oid)?;
                }
                return Ok(match if self.array {
                    match row[11] {
                        Value::Int(0) => Value::Null,
                        _ => row[11].clone(),
                    }
                } else {
                    row[0].clone()
                } {
                    Value::Int(oid) => u32::try_from(oid).ok(),
                    _ => None,
                });
            }
        }
        Ok(None)
    }

    fn validate_builtin_modifiers(&self, oid: &Value) -> R<()> {
        if let Some((precision, scale)) = self.numeric_modifier
            && *oid == Value::Int(1700)
        {
            NumericTypmod::new(precision, scale).map_err(invalid_modifier)?;
        }
        let items = self.custom_modifiers.as_slice();
        if items.is_empty() {
            return Ok(());
        }
        let integer = |item: &str| item.parse::<i32>().map_err(|_| Error::Syntax);
        match oid {
            Value::Int(1700) => {
                let [precision, scale @ ..] = items else { unreachable!("modifiers are nonempty") };
                let scale = match scale {
                    [] => 0,
                    [scale] => integer(scale)?,
                    _ => return Err(invalid_modifier(TypmodError::InvalidModifier)),
                };
                NumericTypmod::new(integer(precision)?, scale).map_err(invalid_modifier)?;
            }
            Value::Int(1042 | 1043) => {
                let [length] = items else {
                    return Err(invalid_modifier(TypmodError::InvalidTypeModifier));
                };
                let kind = if *oid == Value::Int(1042) {
                    CharKind::Bpchar
                } else {
                    CharKind::Varchar
                };
                CharTypmod::new(kind, integer(length)?).map_err(invalid_modifier)?;
            }
            Value::Int(1083 | 1266 | 1114 | 1184) => {
                let [precision] = items else {
                    return Err(invalid_modifier(TypmodError::InvalidTypeModifier));
                };
                let kind = match oid {
                    Value::Int(1083) => TemporalType::Time,
                    Value::Int(1266) => TemporalType::TimeTz,
                    Value::Int(1114) => TemporalType::Timestamp,
                    _ => TemporalType::TimestampTz,
                };
                kind.precision(integer(precision)?).map_err(invalid_modifier)?;
            }
            Value::Int(1186) => {
                let (range, precision) = match items {
                    [range] => (range, None),
                    [range, precision] => (range, Some(integer(precision)?)),
                    _ => return Err(invalid_modifier(TypmodError::InvalidIntervalModifier)),
                };
                interval_typmod(integer(range)?, precision).map_err(invalid_modifier)?;
            }
            // bit and varbit have typmod inputs. Regtype returns their base OIDs.
            Value::Int(1560 | 1562) => {}
            _ => {
                let name = match self.schema.as_ref().filter(|_| self.explicit_schema) {
                    Some(schema) => format!("{schema}.{}", self.name),
                    None => self.name.clone(),
                };
                return Err(Error::TypeModifierNotAllowed(name));
            }
        }
        Ok(())
    }
}

fn invalid_modifier(error: TypmodError) -> Error {
    Error::InvalidTypeModifier(error.message())
}

// PostgreSQL's named-type grammar permits signed integer arguments. Extract
// modifiers on named types before sqlparser can discard or reject them.
fn take_named_modifiers(tokens: &mut Vec<Token>) -> R<Vec<String>> {
    let (open, interval_precision) = match tokens.as_slice() {
        [Token::Word(_), Token::Period, Token::Word(_), Token::LParen, ..] => (3, false),
        [Token::Word(name), Token::LParen, rest @ ..] if name.quote_style.is_none() => {
            match name.value.to_ascii_lowercase().as_str() {
                "time" | "timestamp" | "interval"
                    if matches!(rest.first(), Some(Token::Minus | Token::Plus)) =>
                {
                    let near = if rest.first() == Some(&Token::Minus) { "-" } else { "+" };
                    return Err(Error::TypeNameSyntax { near, input: None });
                }
                "int" | "integer" | "smallint" | "bigint" | "boolean" | "real" => {
                    return Err(if rest.contains(&Token::RParen) {
                        Error::TypeNameSyntax { near: "(", input: None }
                    } else {
                        Error::Syntax
                    });
                }
                "float" | "char" | "character" | "varchar" | "bit" | "time" | "timestamp" => {
                    return Ok(Vec::new());
                }
                "interval" => (1, true),
                _ => (1, false),
            }
        }
        [Token::Word(_), Token::LParen, ..] => (1, false),
        _ => return Ok(Vec::new()),
    };
    let close = tokens.iter().enumerate().skip(open + 1)
        .find_map(|(i, token)| (*token == Token::RParen).then_some(i))
        .ok_or(Error::Syntax)?;
    let mut modifiers = Vec::new();
    for item in tokens[open + 1..close].split(|token| *token == Token::Comma) {
        let value = match item {
            [Token::Number(n, _) | Token::SingleQuotedString(n)] => n.clone(),
            [Token::Word(word)] => word.value.clone(),
            [Token::Plus, Token::Number(n, _)] => format!("+{n}"),
            [Token::Minus, Token::Number(n, _)] => format!("-{n}"),
            _ => return Err(Error::Syntax),
        };
        modifiers.push(value);
    }
    if interval_precision {
        if modifiers.len() != 1 {
            return Err(Error::Syntax);
        }
        modifiers.insert(0, INTERVAL_FULL_RANGE.to_string());
    }
    tokens.drain(open..=close);
    Ok(modifiers)
}

// sqlparser's numeric grammar takes unsigned modifiers. Consume a numeric
// modifier here and let it parse the base name and any array suffix as usual.
fn take_numeric_modifier(tokens: &mut Vec<Token>) -> R<Option<(i32, i32)>> {
    let open = match tokens.as_slice() {
        [Token::Word(name), Token::LParen, ..]
            if (name.quote_style.is_none()
                && ["numeric", "decimal", "dec"].iter().any(|alias| name.value.eq_ignore_ascii_case(alias)))
                || name.value == "numeric" => 1,
        [Token::Word(_), Token::Period, Token::Word(name), Token::LParen, ..]
            if name.value == "numeric"
                || name.quote_style.is_none() && name.value.eq_ignore_ascii_case("numeric") => 3,
        _ => return Ok(None),
    };
    fn signed_integer(tokens: &[Token]) -> R<(i32, &[Token])> {
        let (sign, tokens) = match tokens {
            [Token::Plus, rest @ ..] => ("+", rest),
            [Token::Minus, rest @ ..] => ("-", rest),
            _ => ("", tokens),
        };
        let [Token::Number(digits, _), rest @ ..] = tokens else {
            return Err(Error::Syntax);
        };
        if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Error::Syntax);
        }
        Ok((format!("{sign}{digits}").parse().map_err(|_| Error::Syntax)?, rest))
    }
    let (precision, rest) = signed_integer(&tokens[open + 1..])?;
    let (scale, rest) = match rest {
        [Token::Comma, rest @ ..] => signed_integer(rest)?,
        _ => (0, rest),
    };
    let [Token::RParen, tail @ ..] = rest else {
        return Err(Error::Syntax);
    };
    let end = tokens.len() - tail.len();
    tokens.drain(open..end);
    Ok(Some((precision, scale)))
}

// The AST separates type names from modifiers. Share the validation rules
// with DDL and casts, then resolve the base type OID without a typmod.
fn validate_modifiers(ty: &a::DataType) -> R<Option<&'static str>> {
    use a::{CharacterLength::IntegerLength, DataType::*, ExactNumberInfo};
    let int = |n| i32::try_from(n).map_err(|_| Error::Syntax);
    match ty {
        Float(precision) => {
            let real = match precision {
                Some(n) => float_precision_is_real(int(*n)? as u32).map_err(invalid_modifier)?,
                None => false,
            };
            return Ok(Some(if real { "float4" } else { "float8" }));
        }
        Numeric(info) | Decimal(info) | Dec(info) => {
            let (precision, scale) = match *info {
                ExactNumberInfo::None => return Ok(None),
                ExactNumberInfo::Precision(precision) => (precision, 0),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => (precision, scale),
            };
            NumericTypmod::new(int(precision)?, int(scale)?).map_err(invalid_modifier)?;
        }
        Time(Some(precision), zone) | Timestamp(Some(precision), zone) => {
            let zoned = matches!(zone, a::TimezoneInfo::WithTimeZone | a::TimezoneInfo::Tz);
            let kind = match (matches!(ty, Timestamp(..)), zoned) {
                (true, true) => TemporalType::TimestampTz,
                (true, false) => TemporalType::Timestamp,
                (false, true) => TemporalType::TimeTz,
                (false, false) => TemporalType::Time,
            };
            kind.precision(int(*precision)?).map_err(invalid_modifier)?;
        }
        Varchar(Some(length)) | CharVarying(Some(length)) | CharacterVarying(Some(length))
        | Char(Some(length)) | Character(Some(length)) => {
            let IntegerLength { length, unit: None } = *length else {
                return Err(Error::Syntax);
            };
            let kind = if matches!(ty, Char(_) | Character(_)) {
                CharKind::Bpchar
            } else {
                CharKind::Varchar
            };
            CharTypmod::new(kind, int(length)?).map_err(invalid_modifier)?;
        }
        _ => {}
    }
    Ok(None)
}

/// Resolve type declarations with the same grammar and search path as regtype.
pub fn resolve_type_names(
    snapshot: &CatalogSnapshot,
    declarations: &[String],
) -> R<Vec<Option<u32>>> {
    let requested = BTreeSet::from(["pg_type".into(), "pg_namespace".into()]);
    let catalogs = catalog::build(snapshot, &requested)?;
    declarations
        .iter()
        .map(|declaration| {
            let (name, _) = TypeName::parse(declaration)?;
            name.resolve(
                name.search_path(snapshot),
                &catalogs["pg_type"].rows,
                &catalogs["pg_namespace"].rows,
            )
        })
        .collect()
}
