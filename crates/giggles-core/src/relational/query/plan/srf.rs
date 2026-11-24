//! FROM functions share scalar argument binding, array codecs and row budgets.
use super::*;
use exec::Relation;
use functions::regexp;

#[path = "text_search_srf.rs"]
mod text_search_srf;

#[derive(Debug, Clone)]
pub(super) struct Function {
    name: String,
    pub args: Vec<Scalar>,
    pub input: Option<Box<Plan>>,
    ordinality: bool,
}
fn known(name: &str) -> bool {
    projection_function(name)
        || json_srf::record(name)
        || text_search_srf::known(name)
        || regexp::set_type(name).is_some()
}

pub(super) fn projection_function(name: &str) -> bool {
    matches!(name, "generate_series" | "unnest")
        || (json_srf::known(name) && !json_srf::record(name))
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
    if !known(&name) {
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
    if !tokens.is_empty() || !text_search_srf::known(&name) {
        loop {
            args.push(Scalar::parse_expr(&mut arguments, 0, false)?);
            if !arguments.sym(',') {
                break;
            }
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
pub(in crate::relational::query) fn element(ty: &DataType) -> R<DataType> {
    match ty {
        DataType::IntArray => Ok(DataType::Integer),
        DataType::BigIntArray => Ok(DataType::BigInt),
        DataType::TextArray => Ok(DataType::Text),
        DataType::UuidArray => Ok(DataType::Uuid),
        DataType::TsVector => Ok(DataType::TsVector),
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
    pub(super) fn ordinality(&self) -> bool { self.ordinality }

    fn regex_args(
        &self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
    ) -> R<Vec<BoundScalar>> {
        if !(2..=3).contains(&self.args.len()) {
            return Err(SqlError::UndefinedFunction);
        }
        let no_column = |_: &ColumnRef, _| Err(SqlError::UnknownColumn);
        let args = if let Some(input) = &self.input {
            for arg in &self.args {
                if arg.aggregate_unknown(parameters) {
                    arg.infer(Some(&DataType::Text), parameters, &no_column, &[])?;
                }
            }
            let meta = input.clone().resolve(state, parameters)?;
            meta.columns.into_iter().zip(meta.collations).map(|(c, collation)| {
                functions::collation::materialized(collation.as_ref(), c.data_type)
            }).collect::<R<Vec<_>>>()?
        } else {
            self.args.iter().map(|arg| {
                let hint = arg.aggregate_unknown(parameters).then_some(&DataType::Text);
                arg.infer(hint, parameters, &no_column, &[])?;
                arg.bind(&no_column, &[], parameters)
            }).collect::<R<Vec<_>>>()?
        };
        if args.iter().any(|arg| !regexp::text_type(&arg.ty)) {
            return Err(SqlError::UndefinedFunction);
        }
        Ok(args)
    }

    fn ty(&self, state: &Relational, parameters: &mut [Option<DataType>]) -> R<DataType> {
        if let Some(ty) = regexp::set_type(&self.name) {
            self.regex_args(state, parameters)?;
            return Ok(ty);
        }
        if json_srf::known(&self.name) {
            if self.args.len() != 1 {
                return Err(SqlError::UndefinedFunction);
            }
            let argument = &self.args[0];
            let ty = if let Some(input) = &self.input {
                let meta = input.clone().resolve(state, parameters)?;
                meta.columns[0].data_type.clone()
            } else if argument.aggregate_unknown(parameters) {
                argument.infer(Some(&DataType::Jsonb), parameters, &|_, _| Err(SqlError::UnknownColumn), &[])?;
                DataType::Jsonb
            } else {
                bind::scalar_type(argument, parameters)?
            };
            if ty != DataType::Jsonb {
                return Err(SqlError::UndefinedFunction);
            }
            return Ok(json_srf::output_type(&self.name));
        }
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
            types.push(ty);
        }
        if let [start, stop, step] = types.as_slice() {
            let temporal = |ty: &DataType| {
                matches!(ty, DataType::Date | DataType::Timestamp | DataType::TimestampTz)
            };
            let unknown = |i: usize| self.args[i].aggregate_unknown(parameters);
            if (temporal(start) || temporal(stop))
                && (temporal(start) || unknown(0))
                && (temporal(stop) || unknown(1))
                && (*step == DataType::Interval || unknown(2))
            {
                return Err(SqlError::Unsupported);
            }
        }
        if types.iter().any(|ty| !integer_type(ty)) {
            return Err(SqlError::UndefinedFunction);
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
        let mut columns = if text_search_srf::known(&self.name) {
            if let Some(input) = &self.input {
                let meta = input.clone().resolve(state, parameters)?;
                let types = meta.columns.into_iter().zip(meta.unknown)
                    .map(|(column, unknown)| unknown.is_none().then_some(column.data_type)).collect::<Vec<_>>();
                functions::text_search::row_arg_types(&self.name, &types)?;
            } else {
                functions::text_search::bind_row_args(&self.name, &self.args, parameters)?;
            }
            text_search_srf::columns(&self.name)
        } else {
            self.scalar_columns(state, parameters)?
        };
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

    fn scalar_columns(&self, state: &Relational, parameters: &mut [Option<DataType>]) -> R<Vec<ColumnDef>> {
        let ty = self.ty(state, parameters)?;
        Ok(if ty == DataType::TsVector {
            [("lexeme", DataType::Text), ("positions", DataType::SmallIntArray), ("weights", DataType::PgCharArray)]
                .into_iter().map(|(name, data_type)| ColumnDef { id: 0, name: name.into(), data_type, not_null: false }).collect()
        } else if json_srf::record(&self.name) {
            vec![
                ColumnDef { id: 0, name: "key".into(), data_type: DataType::Text, not_null: false },
                ColumnDef { id: 0, name: "value".into(), data_type: ty, not_null: false },
            ]
        } else {
            let name = if json_srf::element_column(&self.name) { "value" } else { &self.name };
            vec![ColumnDef { id: 0, name: name.into(), data_type: ty, not_null: false }]
        })
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
        let mut regex_args = regexp::set_type(&self.name)
            .map(|_| self.regex_args(state, &mut parameters.to_vec()))
            .transpose()?;
        let row = Tuple {
            source_versions: vec![],
            values: vec![],
            version: 0,
        };
        let eval = |argument: &mut BoundScalar| {
            argument.attach_clock(clock);
            argument.eval(&row, params, &mut 0)
        };
        let mut values = if let Some(rows) = &mut evaluated {
            rows.rows.pop().ok_or(SqlError::Syntax)?.values
        } else if let Some(args) = &mut regex_args {
            args.iter_mut().map(eval).collect::<R<Vec<_>>>()?
        } else {
            let mut bound = if text_search_srf::known(&self.name) {
                functions::text_search::bind_row_args(&self.name, &self.args, &mut parameters.to_vec())?
            } else {
                self.args.iter().map(|scalar| {
                    if json_srf::known(&self.name) && scalar.aggregate_unknown(parameters) {
                        Scalar::Cast(Box::new(scalar.clone()), DataType::Jsonb)
                            .bind(&|_, _| Err(SqlError::UnknownColumn), &[], parameters)
                    } else {
                        scalar.bind(&|_, _| Err(SqlError::UnknownColumn), &[], parameters)
                    }
                }).collect::<R<Vec<_>>>()?
            };
            bound.iter_mut().map(eval).collect::<R<Vec<_>>>()?
        };
        if values.contains(&Value::Null) {
            return Ok(out);
        }
        let mut emit = |mut values: Vec<Value>| -> R<()> {
            execution.comparison()?;
            if self.ordinality {
                values.push(Value::Int(out.rows.len() as i64 + 1));
            }
            out.push(SqlRow { values, version: 0 })
        };
        if text_search_srf::known(&self.name) {
            text_search_srf::run(&self.name, &values, &mut emit)?;
        } else if self.name == "unnest" {
            let ty = self.ty(state, &mut parameters.to_vec())?;
            let value = values.remove(0);
            if ty == DataType::TsVector {
                let vector = text_search_types::vector(&value)?;
                for lexeme in vector.unnest() {
                    let array = |parts: Vec<String>| if parts.is_empty() {
                        Value::Null
                    } else {
                        Value::Text(format!("{{{}}}", parts.join(",")))
                    };
                    emit(vec![
                        Value::Text(lexeme.word.clone()),
                        array(lexeme.positions.iter().map(|p| p.index.to_string()).collect()),
                        array(lexeme.positions.iter().map(|p| p.weight.letter().to_string()).collect()),
                    ])?;
                }
            } else {
                for value in array_values(value, &ty)? {
                    emit(vec![value])?;
                }
            }
        } else if json_srf::known(&self.name) {
            json_srf::emit(&self.name, &values[0], &mut emit)?;
        } else if let Some(args) = &regex_args {
            regexp::emit(&self.name, args, &values, |value| emit(vec![value]))?;
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
                emit(vec![Value::Int(n)])?;
                let Some(next) = n.checked_add(step) else {
                    break;
                };
                n = next;
            }
        }
        Ok(out)
    }
}

impl Function {
    pub(super) fn scalar_element_row(&self) -> bool {
        json_srf::element_column(&self.name) && !self.ordinality
    }

    pub(super) fn uses_relation_alias(&self) -> bool {
        !json_srf::element_column(&self.name)
    }

    pub(super) fn local_column_names(
        &self, state: &Relational, parameters: &[Option<DataType>], alias: &str, aliases: &[String],
    ) -> Vec<String> {
        let mut names = if self.name == "unnest"
            && self.ty(state, &mut parameters.to_vec()) == Ok(DataType::TsVector)
        {
            vec!["lexeme".into(), "positions".into(), "weights".into()]
        } else if text_search_srf::known(&self.name) {
            text_search_srf::columns(&self.name).into_iter().map(|column| column.name).collect()
        } else if json_srf::record(&self.name) {
            vec!["key".into(), "value".into()]
        } else if json_srf::element_column(&self.name) {
            vec!["value".into()]
        } else {
            vec![alias.to_owned()]
        };
        if self.ordinality {
            names.push("ordinality".into());
        }
        for (column, name) in names.iter_mut().zip(aliases) {
            column.clone_from(name);
        }
        names
    }

    pub(super) fn collations(
        &self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
    ) -> R<Vec<Option<BoundScalar>>> {
        if self.name != "unnest" {
            return Ok(vec![]);
        }
        let original = if let Some(input) = &self.input {
            input
                .clone()
                .resolve(state, parameters)?
                .collations
                .into_iter()
                .next()
                .flatten()
        } else {
            Some(self.args[0].bind(&|_, _| Err(SqlError::UnknownColumn), &[], parameters)?)
        };
        let ty = self.ty(state, parameters)?;
        if ty == DataType::TsVector {
            return Ok(vec![None, None, None]);
        }
        Ok(vec![Some(functions::collation::materialized(original.as_ref(), ty)?)])
    }
}
