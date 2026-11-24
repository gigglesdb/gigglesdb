use super::*;
use crate::relational::generated_columns::{Definition, GeneratedColumn};

thread_local! {
    static VIRTUAL: std::cell::RefCell<BTreeMap<u64, GeneratedColumn>> =
        const { std::cell::RefCell::new(BTreeMap::new()) };
}

pub(super) struct Scope(BTreeMap<u64, GeneratedColumn>);

impl Scope {
    pub(super) fn enter(state: &Relational) -> Self {
        let virtuals = state
            .ext
            .generated_columns
            .iter()
            .filter(|(_, column)| !column.stored)
            .map(|(id, column)| (*id, column.clone()))
            .collect();
        Self(VIRTUAL.with(|scope| scope.replace(virtuals)))
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        VIRTUAL.with(|scope| scope.replace(std::mem::take(&mut self.0)));
    }
}

pub(super) fn is_virtual(id: u64) -> bool {
    VIRTUAL.with(|scope| scope.borrow().contains_key(&id))
}

pub(super) fn virtual_binding(tables: &[&TableDef], column: BoundColumn) -> R<Option<BoundScalar>> {
    let table = tables[column.source];
    let target = &table.columns[column.column];
    let definition = VIRTUAL.with(|scope| scope.borrow().get(&target.id).cloned());
    let Some(definition) = definition else {
        return Ok(None);
    };
    let expression = parse(lex(&definition.sql)?)?;
    let value = expression
        .bind(
            &|reference, _| {
                let offset = reference_offset(table, &definition, reference)?;
                Ok(BoundColumn {
                    source: column.source,
                    column: offset,
                })
            },
            tables,
            &[],
        )?
        .insert_assignment(&target.data_type)?;
    // An unmatched outer-join row has a NULL physical key. A real row with
    // NULL inputs still runs the generation expression.
    Ok(Some(if let Some(&key) = table.primary_key.first() {
        value.null_extend(
            BoundColumn {
                source: column.source,
                column: key,
            },
            &table.columns[key],
        )
    } else {
        value
    }))
}

fn reference_offset(
    table: &TableDef,
    definition: &GeneratedColumn,
    reference: &ColumnRef,
) -> R<usize> {
    let offset = col(table, &reference.name)?;
    if reference.qualifier.is_some() || !definition.references.contains(&table.columns[offset].id) {
        return Err(SqlError::Type);
    }
    Ok(offset)
}

fn parse(tokens: Vec<Token>) -> R<Scalar> {
    let mut p = Parser {
        privilege_seen: false,
        privilege_casts: false,
        tokens,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let expression = Filter::parse(&mut p, 0, 0)?;
    if p.at != p.tokens.len() || !p.params.is_empty() {
        return Err(SqlError::Syntax);
    }
    Ok(match expression {
        Filter::Scalar(value) => value,
        other => Scalar::Predicate(Box::new(other)),
    })
}

pub(in crate::relational) fn rename_reference(sql: &str, old: &str, new: &str) -> R<String> {
    let mut expression = parse(lex(sql)?)?;
    plan::subquery::map_scalar(&mut expression, &mut |value| {
        if let Scalar::Column(column) = value
            && column.name == old
        {
            column.name = new.into();
        }
        Ok(())
    })?;
    deparse(&expression)
}

// PostgreSQL prints the resolved types of these formerly unknown arguments.
fn deparse_argument(value: &Scalar, inferred: Option<&DataType>) -> R<String> {
    match (value, inferred) {
        (Scalar::Literal(Value::Text(text)), Some(ty)) => {
            let text = if *ty == DataType::RegConfig {
                text_search_types::output(ty, &text_search_types::input(ty, text)?)?
            } else {
                text.clone()
            };
            Ok(format!("'{}'::{}", text.replace('\'', "''"), defaults::type_name(ty)))
        }
        (Scalar::Literal(Value::Null), Some(ty)) => Ok(format!("NULL::{}", defaults::type_name(ty))),
        _ => deparse(value),
    }
}

fn deparse(value: &Scalar) -> R<String> {
    match value {
        Scalar::Cast(input, ty) if matches!(input.as_ref(), Scalar::Literal(Value::Null)) => {
            Ok(format!("NULL::{}", defaults::type_name(ty)))
        }
        Scalar::Cast(input, ty @ (DataType::Text | DataType::RegConfig | DataType::PgChar))
            if matches!(input.as_ref(), Scalar::Literal(Value::Text(_))) =>
        {
            deparse_argument(input, Some(ty))
        }
        Scalar::Call(name, args) if name == "coalesce" => {
            let args = args.iter().map(deparse).collect::<R<Vec<_>>>()?.join(", ");
            Ok(format!("COALESCE({args})"))
        }
        Scalar::Binary(op, left, right) => {
            let op = match op {
                functions::BinaryOp::Add => "+",
                functions::BinaryOp::Sub => "-",
                functions::BinaryOp::Mul => "*",
                functions::BinaryOp::Div => "/",
                functions::BinaryOp::Mod => "%",
                functions::BinaryOp::Concat => "||",
                _ => return Err(SqlError::Unsupported),
            };
            Ok(format!("({} {op} {})", deparse(left)?, deparse(right)?))
        }
        Scalar::Call(name, args)
            if !name.contains('.')
                && !name.contains('\0')
                && !matches!(
                    name.as_str(),
                    "nullif" | "width_bucket" | "normalize" | "is_normalized"
                ) =>
        {
            let args = args
                .iter()
                .enumerate()
                .map(|(index, argument)| {
                    let inferred = match (name.as_str(), args.len(), index) {
                        ("to_tsvector", 2, 0) => Some(&DataType::RegConfig),
                        ("setweight", _, 1) => Some(&DataType::PgChar),
                        _ => None,
                    };
                    deparse_argument(argument, inferred)
                })
                .collect::<R<Vec<_>>>()?
                .join(", ");
            Ok(format!("{}({args})", constraints::constraint_quote(name)))
        }
        _ => constraints::scalar(value),
    }
}

pub(in crate::relational) fn pretty(sql: &str) -> R<String> {
    fn render(value: &Scalar, parent: u8) -> R<String> {
        let (text, precedence) = match value {
            Scalar::Binary(op, left, right) => {
                let (op, precedence) = match op {
                    functions::BinaryOp::Add => ("+", 5),
                    functions::BinaryOp::Sub => ("-", 5),
                    functions::BinaryOp::Mul => ("*", 6),
                    functions::BinaryOp::Div => ("/", 6),
                    functions::BinaryOp::Mod => ("%", 6),
                    functions::BinaryOp::Concat => ("||", 4),
                    _ => return deparse(value),
                };
                // PostgreSQL preserves grouping for nested generic operators.
                let left_precedence = precedence + u8::from(op == "||");
                (
                    format!(
                        "{} {op} {}",
                        render(left, left_precedence)?,
                        render(right, precedence + 1)?
                    ),
                    precedence,
                )
            }
            Scalar::Cast(input, DataType::Text)
                if matches!(input.as_ref(), Scalar::Literal(Value::Text(_))) =>
            {
                return deparse(input);
            }
            Scalar::Cast(input, ty) => (
                format!("{}::{}", render(input, 8)?, defaults::type_name(ty)),
                8,
            ),
            Scalar::Call(name, args) if name == "coalesce" => (
                format!(
                    "COALESCE({})",
                    args.iter()
                        .map(|arg| render(arg, 0))
                        .collect::<R<Vec<_>>>()?
                        .join(", ")
                ),
                9,
            ),
            _ => return deparse(value),
        };
        Ok(if precedence < parent {
            format!("({text})")
        } else {
            text
        })
    }
    render(&parse(lex(sql)?)?, 0)
}

pub(in crate::relational) fn bind_definition(
    table: &TableDef,
    target: &ColumnDef,
    definition: &Definition,
    generated: &BTreeSet<u64>,
) -> R<GeneratedColumn> {
    if let Some(at) = definition.tokens.windows(2).position(|tokens| matches!(tokens,
        [Token::Word(name) | Token::QuotedIdent(name), Token::Symbol('(')] if Aggregate::from_name(name).is_some()))
    {
        if let Some(position) = definition.locations.get(at) {
            user_types::sql_error_position(*position);
        }
        user_types::view_replace_error("aggregate functions are not allowed in column generation expressions".into());
        return Err(SqlError::GroupingError);
    }
    let mut value = parse(definition.tokens.clone())?;
    if matches!(value, Scalar::Literal(Value::Null)) {
        value = Scalar::Cast(Box::new(value), target.data_type.clone());
    }
    let references = std::cell::RefCell::new(BTreeSet::new());
    let bound = value.bind(&|reference, _| {
        let locate_error = || {
            if let Some((_, position)) = definition.tokens.iter().zip(&definition.locations).enumerate()
                .find_map(|(at, (token, position))| {
                    (matches!(token, Token::Word(name) | Token::QuotedIdent(name) if name == &reference.name)
                        && definition.tokens.get(at + 1) != Some(&Token::Symbol('('))
                        && definition.tokens.get(at.wrapping_sub(1)) != Some(&Token::Symbol(':')))
                        .then_some((at, position))
                })
            {
                user_types::sql_error_position(*position);
            }
        };
        if reference.qualifier.is_some() {
            locate_error();
            return Err(SqlError::UnknownColumn);
        }
        let column = col(table, &reference.name).inspect_err(|_| {
            locate_error();
            user_types::view_replace_error(format!("column \"{}\" does not exist", reference.name));
        })?;
        let id = table.columns[column].id;
        if generated.contains(&id) {
            locate_error();
            user_types::view_replace_error(format!("cannot use generated column \"{}\" in column generation expression", reference.name));
            user_types::sql_error_detail("A generated column cannot reference another generated column.".into());
            return Err(SqlError::InvalidObjectDefinition);
        }
        references.borrow_mut().insert(id);
        Ok(BoundColumn { source: 0, column })
    }, &[table], &[])?;
    if !bound.generation_immutable() {
        user_types::view_replace_error("generation expression is not immutable".into());
        return Err(SqlError::InvalidObjectDefinition);
    }
    bound.insert_assignment(&target.data_type)?;
    Ok(GeneratedColumn {
        sql: deparse(&value)?,
        stored: definition.stored,
        not_null: !definition.stored && target.not_null,
        references: references.into_inner().into_iter().collect(),
    })
}

pub(in crate::relational) fn value(
    table: &TableDef,
    definition: &GeneratedColumn,
    target: &ColumnDef,
    values: &[Value],
) -> R<Value> {
    let expression = parse(lex(&definition.sql)?)?;
    let bound = expression
        .bind(
            &|reference, _| {
                Ok(BoundColumn {
                    source: 0,
                    column: reference_offset(table, definition, reference)?,
                })
            },
            &[table],
            &[],
        )?
        .insert_assignment(&target.data_type)?;
    bound.eval(
        &Tuple {
            source_versions: vec![0],
            values: vec![values.to_vec()],
            version: 0,
        },
        &[],
        &mut 0,
    )
}
