//! FROM functions share scalar argument binding, array codecs and row budgets.
use super::*;
use exec::Relation;

#[derive(Debug, Clone)]
pub(super) struct Function {
    name: String,
    pub args: Vec<Scalar>,
    pub input: Option<Box<Plan>>,
    ordinality: bool,
}
pub(super) fn parse(
    p: &mut Parser,
    name: &str,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> R<Function> {
    let name = name
        .strip_prefix("pg_catalog\0")
        .unwrap_or(name)
        .to_string();
    if !["generate_series", "unnest"].contains(&name.as_str()) {
        return Err(SqlError::UndefinedFunction);
    }
    let open = p.at;
    if !p.sym('(') {
        return Err(SqlError::Syntax);
    }
    let close = subquery_end(&p.tokens, open)?;
    let original = p.tokens[open + 1..close].to_vec();
    // Function arguments can contain subqueries, so admit before descending.
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    let (tokens, applies) =
        subquery::rewrite(&original, owner, depth + 1, next).map_err(|e| match e {
            ParseError::Definite(e) => e,
            ParseError::Decline => SqlError::Syntax,
        })?;
    p.at = close + 1;
    let mut arguments = parse::parser(&tokens);
    let mut args = vec![];
    loop {
        args.push(Scalar::parse_expr(&mut arguments, 0, false)?);
        if !arguments.sym(',') {
            break;
        }
    }
    if arguments.at != arguments.tokens.len() {
        return Err(SqlError::Syntax);
    }
    let input = if applies.is_empty() {
        owner.relations.extend(arguments.relations);
        None
    } else {
        let mut select = vec![Token::Word("select".into())];
        select.extend(tokens);
        let mut parser = parse::parser(&select);
        let query = Query::parse(&mut parser)?;
        owner.relations.extend(parser.relations);
        let mut select = Select {
            query,
            applies,
            windows: vec![],
            parameters: original
                .iter()
                .filter_map(|t| {
                    if let Token::Param(n) = t {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect(),
            input: None,
            from: None,
        };
        let names = subquery::internalize(&mut select)?;
        for arg in &mut args {
            subquery::rename_scalar(arg, &names)?;
        }
        Some(Box::new(Plan::new(Body::Select(Box::new(select)))))
    };
    let ordinality = p.word("with");
    if ordinality {
        p.need("ordinality")?;
    }
    Ok(Function {
        name,
        args,
        input,
        ordinality,
    })
}
pub(super) fn element(ty: &DataType) -> R<DataType> {
    match ty {
        DataType::IntArray => Ok(DataType::Integer),
        DataType::BigIntArray => Ok(DataType::BigInt),
        DataType::TextArray => Ok(DataType::Text),
        _ => Err(SqlError::Type),
    }
}
pub(super) fn array_values(value: Value, ty: &DataType) -> R<Vec<Value>> {
    match value {
        Value::Null => Ok(vec![]),
        Value::Text(s) => crate::relational::logical_types::array_values(&s)?
            .into_iter()
            .map(|v| {
                v.map_or(Ok(Value::Null), |s| {
                    functions::coerce_clocked(Value::Text(s), ty, None)
                })
            })
            .collect(),
        _ => Err(SqlError::Type),
    }
}
impl Function {
    fn ty(&self, state: &Relational, parameters: &mut [Option<DataType>]) -> R<DataType> {
        if let Some(input) = &self.input {
            let meta = input.clone().resolve(state, parameters)?;
            if self.name == "unnest" {
                if meta.columns.len() != 1 {
                    return Err(SqlError::UndefinedFunction);
                }
                return element(&meta.columns[0].data_type);
            }
            if !(2..=3).contains(&meta.columns.len())
                || meta.columns.iter().any(|c| !integer_type(&c.data_type))
            {
                return Err(SqlError::UndefinedFunction);
            }
            return Ok(
                if meta.columns.iter().any(|c| c.data_type == DataType::BigInt) {
                    DataType::BigInt
                } else {
                    DataType::Integer
                },
            );
        }
        if self.name == "unnest" {
            if self.args.len() != 1 {
                return Err(SqlError::UndefinedFunction);
            }
            return element(&bind::scalar_type(&self.args[0], parameters)?);
        }
        if !(2..=3).contains(&self.args.len()) {
            return Err(SqlError::UndefinedFunction);
        }
        let mut types = vec![];
        for s in &self.args {
            let hint = if matches!(s,Scalar::Param(n) if parameters.get(n-1).is_some_and(Option::is_none))
            {
                Some(&DataType::Integer)
            } else {
                None
            };
            s.infer(hint, parameters, &|_, _| Err(SqlError::UnknownColumn), &[])?;
            let ty = bind::scalar_type(s, parameters)?;
            if !integer_type(&ty) {
                return Err(SqlError::UndefinedFunction);
            }
            types.push(ty);
        }
        Ok(if types.contains(&DataType::BigInt) {
            DataType::BigInt
        } else {
            DataType::Integer
        })
    }
    pub fn resolve(
        &self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
    ) -> R<(Vec<ColumnDef>, Vec<i32>)> {
        let mut columns = vec![ColumnDef {
            id: 0,
            name: self.name.clone(),
            data_type: self.ty(state, parameters)?,
            not_null: false,
        }];
        if self.ordinality {
            columns.push(ColumnDef {
                id: 0,
                name: "ordinality".into(),
                data_type: DataType::BigInt,
                not_null: true,
            });
        }
        let typmods = vec![-1; columns.len()];
        Ok((columns, typmods))
    }
    pub fn run<'a>(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        parameters: &[Option<DataType>],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        execution: &'a Execution<'_>,
    ) -> R<Relation<'a>> {
        let (columns, typmods) = self.resolve(state, &mut parameters.to_vec())?;
        let mut out = Relation::new(columns, typmods, execution);
        let mut evaluated = self
            .input
            .as_ref()
            .map(|p| p.run(state, source, params, parameters, clock, execution))
            .transpose()?;
        let mut values = if let Some(rows) = &mut evaluated {
            rows.rows.pop().ok_or(SqlError::Syntax)?.values
        } else {
            let mut values = vec![];
            for s in &self.args {
                let mut b = s.bind(&|_, _| Err(SqlError::UnknownColumn), &[], parameters)?;
                b.attach_clock(clock);
                values.push(b.eval(
                    &Tuple {
                        values: vec![],
                        version: 0,
                    },
                    params,
                    &mut 0,
                )?);
            }
            values
        };
        if values.contains(&Value::Null) {
            return Ok(out);
        }
        let mut emit = |value: Value| -> R<()> {
            execution.comparison()?;
            let mut values = vec![value];
            if self.ordinality {
                values.push(Value::Int(out.rows.len() as i64 + 1));
            }
            out.push(SqlRow { values, version: 0 })
        };
        if self.name == "unnest" {
            let ty = self.ty(state, &mut parameters.to_vec())?;
            for value in array_values(values.remove(0), &ty)? {
                emit(value)?;
            }
        } else {
            let Value::Int(mut n) = values[0] else {
                return Err(SqlError::Type);
            };
            let Value::Int(stop) = values[1] else {
                return Err(SqlError::Type);
            };
            let step = match values.get(2) {
                Some(Value::Int(n)) => *n,
                None => 1,
                _ => return Err(SqlError::Type),
            };
            if step == 0 {
                return Err(SqlError::InvalidParameterValue);
            }
            while if step > 0 { n <= stop } else { n >= stop } {
                emit(Value::Int(n))?;
                let Some(next) = n.checked_add(step) else {
                    break;
                };
                n = next;
            }
        }
        Ok(out)
    }
}
