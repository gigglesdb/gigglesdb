use super::*;

impl Runtime<'_> {
    pub(super) fn regtype(&mut self, input: &str) -> R<Value> {
        let input = input.trim();
        if input.is_empty() {
            return Ok(Value::Null);
        }
        if let Ok(oid) = input.parse::<u32>() {
            return Ok(Value::Int(i64::from(oid)));
        }
        if input.len() > MAX_SQL {
            return Err(Error::Limit);
        }
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, input)
            .tokenize()
            .map_err(|_| Error::Syntax)?;
        if tokens.len() > 256 {
            return Err(Error::Limit);
        }
        self.charge(tokens.len())?;
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
        let parts = if let a::DataType::Custom(name, _) = &ty {
            object(name)
        } else {
            let mut name = ty.to_string().to_ascii_lowercase();
            if let (Some(start), Some(end)) = (name.find('('), name.find(')')) {
                name.replace_range(start..=end, "");
            }
            let name = match name.trim() {
                "boolean" | "bool" => "bool",
                "smallint" | "int2" => "int2",
                "int" | "integer" | "int4" => "int4",
                "bigint" | "int8" => "int8",
                "real" | "float4" => "float4",
                "double precision" | "float8" => "float8",
                "decimal" | "numeric" => "numeric",
                "char" | "character" => "bpchar",
                "varchar" | "character varying" => "varchar",
                "bit varying" => "varbit",
                "time" | "time without time zone" => "time",
                "time with time zone" => "timetz",
                "timestamp" | "timestamp without time zone" => "timestamp",
                "timestamp with time zone" => "timestamptz",
                other => other,
            };
            vec!["pg_catalog".into(), name.into()]
        };
        let (schema, name) = match parts.as_slice() {
            [name] => (None, name),
            [schema, name] => (Some(schema.as_str()), name),
            _ => return Err(Error::Syntax),
        };
        let mut path = self.snapshot.search_path.clone();
        if !path.iter().any(|s| s == "pg_catalog") {
            path.insert(0, "pg_catalog".into());
        }
        let path = schema.map_or(path, |s| vec![s.into()]);
        let types = &self.catalogs["pg_type"].rows;
        let namespaces = &self.catalogs["pg_namespace"].rows;
        self.charge(path.len() * (types.len() + namespaces.len()))?;
        for schema in path {
            if let Some(namespace) = namespaces
                .iter()
                .find(|r| matches!(&r[1], Value::Text(n) if n == &schema))
                && let Some(row) = types
                    .iter()
                    .find(|r| matches!(&r[1], Value::Text(n) if n == name) && r[2] == namespace[0])
            {
                return Ok(if array {
                    match row[11] {
                        Value::Int(0) => Value::Null,
                        _ => row[11].clone(),
                    }
                } else {
                    row[0].clone()
                });
            }
        }
        Ok(Value::Null)
    }
}
