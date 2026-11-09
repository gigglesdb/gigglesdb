//! Input-only scalar expressions for single-row INSERT VALUES.
use super::*;

#[derive(Debug, Clone)]
pub(in super::super) struct InsertExpressions {
    pub(in super::super) name: String,
    pub(in super::super) columns: Vec<String>,
    pub(in super::super) returning: Option<Returning>,
    pub(in super::super) parameter_map: Option<Vec<usize>>,
    values: Vec<Scalar>,
}
struct BoundValues {
    values: Vec<BoundScalar>,
    parameters: Vec<DataType>,
}
pub(in super::super) struct Literals<'a> {
    roots: std::slice::Iter<'a, Scalar>,
    stack: [Option<(LiteralNode<'a>, usize)>; 64],
    depth: usize,
}
#[derive(Clone, Copy)]
enum LiteralNode<'a> {
    Scalar(&'a Scalar),
    Filter(&'a Filter),
}
impl<'a> Iterator for Literals<'a> {
    type Item = &'a Value;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.depth == 0 {
                self.stack[0] = Some((LiteralNode::Scalar(self.roots.next()?), 0));
                self.depth = 1;
            }
            let (node, cursor) = self.stack[self.depth - 1].unwrap();
            let child = match node {
                LiteralNode::Filter(f) => {
                    let child = match f {
                        Filter::Scalar(s) | Filter::Null(s, _) if cursor == 0 => {
                            Some(LiteralNode::Scalar(s))
                        }
                        Filter::Compare(a, _, b) if cursor < 2 => {
                            Some(LiteralNode::Scalar(if cursor == 0 { a } else { b }))
                        }
                        Filter::Like {
                            value,
                            pattern,
                            escape,
                            ..
                        } => match cursor {
                            0 => Some(LiteralNode::Scalar(value)),
                            1 => Some(LiteralNode::Scalar(pattern)),
                            2 => escape.as_ref().map(LiteralNode::Scalar),
                            _ => None,
                        },
                        Filter::Not(f) | Filter::Is(f, _, _) if cursor == 0 => {
                            Some(LiteralNode::Filter(f))
                        }
                        Filter::Logic(a, _, b) if cursor < 2 => {
                            Some(LiteralNode::Filter(if cursor == 0 { a } else { b }))
                        }
                        _ => None,
                    };
                    self.stack[self.depth - 1] = Some((node, cursor + 1));
                    child
                }
                LiteralNode::Scalar(scalar) => {
                    let child = match scalar {
                        Scalar::Literal(value) => {
                            let out = value;
                            self.depth -= 1;
                            return Some(out);
                        }
                        Scalar::Cast(value, _) | Scalar::Unary(_, value) if cursor == 0 => {
                            self.stack[self.depth - 1] = Some((node, 1));
                            Some(value.as_ref())
                        }
                        Scalar::Binary(_, a, b) if cursor < 2 => {
                            self.stack[self.depth - 1] = Some((node, cursor + 1));
                            Some(if cursor == 0 { a.as_ref() } else { b.as_ref() })
                        }
                        Scalar::Call(_, args) if cursor < args.len() => {
                            let out = &args[cursor];
                            self.stack[self.depth - 1] = Some((node, cursor + 1));
                            Some(out)
                        }
                        Scalar::SubstringOrder(args) if cursor < args.len() => {
                            let out = args[cursor].as_ref();
                            self.stack[self.depth - 1] = Some((node, cursor + 1));
                            Some(out)
                        }
                        Scalar::Predicate(f) if cursor == 0 => {
                            self.stack[self.depth - 1] = Some((node, 1));
                            self.stack[self.depth] = Some((LiteralNode::Filter(f), 0));
                            self.depth += 1;
                            continue;
                        }
                        Scalar::Case(c) => {
                            let child = if cursor < c.arms.len() * 2 {
                                let (f, s) = &c.arms[cursor / 2];
                                Some(if cursor % 2 == 0 {
                                    LiteralNode::Filter(f)
                                } else {
                                    LiteralNode::Scalar(s)
                                })
                            } else if cursor == c.arms.len() * 2 {
                                c.otherwise.as_ref().map(LiteralNode::Scalar)
                            } else {
                                None
                            };
                            self.stack[self.depth - 1] = Some((node, cursor + 1));
                            if let Some(child) = child {
                                self.stack[self.depth] = Some((child, 0));
                                self.depth += 1;
                                continue;
                            }
                            None
                        }
                        _ => None,
                    };
                    child.map(LiteralNode::Scalar)
                }
            };
            if let Some(child) = child {
                // This wrapper admits AST depth and node count before constructing the iterator.
                self.stack[self.depth] = Some((child, 0));
                self.depth += 1;
            } else {
                self.depth -= 1;
            }
        }
    }
}
fn bytes(value: &Value) -> usize {
    if let Value::Numeric(value) = value {
        value.display_byte_bound()
    } else {
        value_bytes(value)
    }
}
pub(super) fn shape(value: &Scalar, depth: usize, nodes: &mut usize) -> R<()> {
    if depth > 32 || *nodes == MAX_WORK {
        return Err(SqlError::Limit);
    }
    *nodes += 1;
    match value {
        Scalar::Cast(child, _) | Scalar::Unary(_, child) => shape(child, depth + 1, nodes),
        Scalar::Binary(_, a, b) => {
            shape(a, depth + 1, nodes)?;
            shape(b, depth + 1, nodes)
        }
        Scalar::Call(_, children) => children
            .iter()
            .try_for_each(|child| shape(child, depth + 1, nodes)),
        Scalar::SubstringOrder(children) => children
            .iter()
            .try_for_each(|child| shape(child, depth + 1, nodes)),
        Scalar::Predicate(f) => f.admit_shape(depth + 1, nodes),
        Scalar::Case(c) => {
            for (f, s) in &c.arms {
                f.admit_shape(depth + 1, nodes)?;
                shape(s, depth + 1, nodes)?;
            }
            if let Some(s) = &c.otherwise {
                shape(s, depth + 1, nodes)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
impl InsertExpressions {
    pub(in super::super) fn check_identity(&self, state: &Relational) -> R<()> {
        let table = state.tables.get(&self.name).ok_or(SqlError::UnknownTable)?;
        let indexes = if self.columns.is_empty() {
            table.logical_offsets()
        } else {
            self.columns
                .iter()
                .map(|name| col(table, name))
                .collect::<R<Vec<_>>>()?
        };
        for index in indexes {
            defaults::check_explicit(state, &table.columns[index])?;
        }
        Ok(())
    }
    pub(in super::super) fn sequence_dependencies(&self) -> Vec<Option<String>> {
        let mut dependencies = vec![];
        for value in &self.values {
            value.sequence_dependencies(&mut dependencies);
        }
        dependencies
    }
    /// Bind stored default syntax using the same assignment rules as VALUES.
    pub(in super::super) fn default_value(
        sql: &str,
        target: &DataType,
        clock: Option<&crate::relational::clock::ExecutionClock>,
        evaluate: bool,
    ) -> R<Option<Value>> {
        let mut parser = Parser {
            tokens: lex(sql)?,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        let value = Scalar::parse_aggregate_argument(&mut parser, 0)?;
        if parser.at != parser.tokens.len() || !parser.params.is_empty() {
            return Err(SqlError::Unsupported);
        }
        shape(&value, 0, &mut 0)?;
        let no_columns = |_: &ColumnRef, _: usize| Err(SqlError::Unsupported);
        let mut parameters = vec![];
        value.infer(None, &mut parameters, &no_columns, &[])?;
        let value = if matches!(value, Scalar::Literal(Value::Text(_) | Value::Null)) {
            Scalar::Cast(Box::new(value), target.clone())
        } else {
            value
        };
        let mut value = value
            .bind(&no_columns, &[], &parameters)?
            .insert_assignment(target)?;
        if !value.insert_input_only() {
            return Err(SqlError::Unsupported);
        }
        let constant = value.plan_constant(&mut 0)?;
        if !evaluate {
            return Ok(constant);
        }
        value.attach_clock(clock);
        let empty = Tuple {
            values: vec![],
            version: 0,
        };
        value.eval(&empty, &[], &mut 0).map(Some)
    }
    pub(in super::super) fn parse(p: &mut Parser) -> R<Self> {
        p.need("insert")?;
        p.need("into")?;
        let name = p.table_name()?;
        let columns = if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
            p.names()?
        } else {
            vec![]
        };
        p.need("values")?;
        p.punct('(')?;
        let values_start = p.at;
        let mut values = vec![];
        loop {
            if values.len() == MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            let value = Scalar::parse_aggregate_argument(p, 0)?;
            shape(&value, 0, &mut 0)?;
            value.reject_nested_aggregate()?;
            values.push(value);
            if !p.sym(',') {
                break;
            }
        }
        p.punct(')')?;
        let values_end = p.at;
        let returning = Returning::parse(p, &name)?;
        // The established VALUES binder takes dense input slots. Carry the
        // original slot IDs so RETURNING can add independently typed inputs.
        let parameter_map = if returning.is_some() {
            let map = p.tokens[values_start..values_end]
                .iter()
                .filter_map(|token| {
                    if let Token::Param(n) = token {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            fn remap(value: &mut Scalar, map: &[usize]) -> R<()> {
                fn filter(f: &mut Filter, map: &[usize]) -> R<()> {
                    match f {
                        Filter::Scalar(s) | Filter::Null(s, _) => remap(s, map),
                        Filter::Compare(a, _, b) => {
                            remap(a, map)?;
                            remap(b, map)
                        }
                        Filter::Like {
                            value,
                            pattern,
                            escape,
                            ..
                        } => {
                            remap(value, map)?;
                            remap(pattern, map)?;
                            if let Some(s) = escape {
                                remap(s, map)?;
                            }
                            Ok(())
                        }
                        Filter::Not(f) | Filter::Is(f, _, _) => filter(f, map),
                        Filter::Logic(a, _, b) => {
                            filter(a, map)?;
                            filter(b, map)
                        }
                        Filter::InSubquery { .. } => Err(SqlError::Unsupported),
                        Filter::Clauses(_) => unreachable!("VALUES has no query clauses"),
                    }
                }
                match value {
                    Scalar::Param(n) => {
                        *n = map.binary_search(n).unwrap() + 1;
                        Ok(())
                    }
                    Scalar::Cast(value, _) | Scalar::Unary(_, value) => remap(value, map),
                    Scalar::Binary(_, a, b) => {
                        remap(a, map)?;
                        remap(b, map)
                    }
                    Scalar::Call(_, args) => args.iter_mut().try_for_each(|arg| remap(arg, map)),
                    Scalar::SubstringOrder(args) => args.iter_mut().try_for_each(|arg| remap(arg, map)),
                    Scalar::Predicate(f) => filter(f, map),
                    Scalar::Case(c) => {
                        for (f, s) in &mut c.arms {
                            filter(f, map)?;
                            remap(s, map)?;
                        }
                        if let Some(s) = &mut c.otherwise {
                            remap(s, map)?;
                        }
                        Ok(())
                    }
                    _ => Ok(()),
                }
            }
            values.iter_mut().try_for_each(|value| remap(value, &map))?;
            Some(map)
        } else {
            None
        };

        p.sym(';');
        if p.at != p.tokens.len() {
            return Err(SqlError::Unsupported);
        }
        Ok(Self {
            name,
            columns,
            values,
            returning,
            parameter_map,
        })
    }
    fn literals(&self) -> Literals<'_> {
        Literals {
            roots: self.values.iter(),
            stack: [None; 64],
            depth: 0,
        }
    }
    pub(in super::super) fn admit_literals(&self) -> R<()> {
        let mut nodes = 0;
        for value in &self.values {
            shape(value, 0, &mut nodes)?;
        }
        Relational::secondary_admit_values(self.literals())?;
        let mut total = 0usize;
        for value in self.literals() {
            total = total.checked_add(bytes(value)).ok_or(SqlError::Limit)?;
            if total > MAX_ROW_BYTES {
                return Err(SqlError::Limit);
            }
        }
        Ok(())
    }
    fn bind(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<BoundValues> {
        let table = state.tables.get(&self.name).ok_or(SqlError::UnknownTable)?;
        self.admit_literals()?;
        if count > MAX_PARAMS || self.values.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        let indexes = if self.columns.is_empty() {
            table.logical_offsets()
        } else {
            self.columns
                .iter()
                .map(|name| col(table, name))
                .collect::<R<Vec<_>>>()?
        };
        unique(&indexes)?;
        if indexes.len() != self.values.len() {
            return Err(SqlError::Type);
        }
        let no_columns = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
        let mut parameters = vec![None; count];
        if declared.len() > count {
            return Err(SqlError::Parameters);
        }
        parameters[..declared.len()].clone_from_slice(declared);
        for (value, index) in self.values.iter().zip(&indexes) {
            let expected = match value {
                Scalar::Param(n) if parameters[n - 1].is_none() => {
                    Some(&table.columns[*index].data_type)
                }
                _ => None,
            };
            value.infer(expected, &mut parameters, &no_columns, &[])?;
        }
        let mut values = Vec::with_capacity(self.values.len());
        for (value, index) in self.values.iter().zip(indexes) {
            let target = &table.columns[index].data_type;
            let bound = if matches!(value, Scalar::Literal(Value::Text(_) | Value::Null)) {
                Scalar::Cast(Box::new(value.clone()), target.clone()).bind(
                    &no_columns,
                    &[],
                    &parameters,
                )?
            } else {
                value.bind(&no_columns, &[], &parameters)?
            };
            let bound = bound.insert_assignment(target)?;
            if !bound.insert_input_only() {
                return Err(SqlError::Unsupported);
            }
            values.push(bound);
        }
        Ok(BoundValues {
            values,
            parameters: parameters
                .into_iter()
                .map(|v| v.ok_or(SqlError::Parameters))
                .collect::<R<_>>()?,
        })
    }
    pub(in super::super) fn describe(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<Description> {
        let bound = self.bind(state, count, declared)?;
        Ok(Description {
            parameters: bound.parameters,
            typmods: vec![],
            columns: vec![],
        })
    }
    pub(in super::super) fn validate_constants(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<()> {
        let bound = self.bind(state, count, declared)?;
        let mut budget = 0;
        for value in &bound.values {
            value.plan_constant(&mut budget)?;
        }
        Ok(())
    }
    pub(in crate::relational) fn evaluate_clocked(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
        params: &[Value],
        cancel: &mut impl FnMut() -> bool,
        clock: Option<&crate::relational::clock::ExecutionClock>,
    ) -> R<Vec<Expr>> {
        let mut bound = self.bind(state, count, declared)?;
        for value in &mut bound.values {
            value.attach_clock(clock);
        }
        if params.len() != bound.parameters.len() {
            return Err(SqlError::Parameters);
        }
        for (value, ty) in params.iter().zip(&bound.parameters) {
            check(
                value,
                &ColumnDef {
                    id: 0,
                    name: String::new(),
                    data_type: ty.clone(),
                    not_null: false,
                },
            )?;
        }
        let empty = Tuple {
            values: vec![],
            version: 0,
        };
        let mut budget = 0;
        let mut values = Vec::with_capacity(bound.values.len());
        for value in bound.values {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            values.push(value.eval(&empty, params, &mut budget)?);
        }
        // Output admission precedes normalized Expr/legacy row payload clones.
        Relational::secondary_admit_values(values.iter())?;
        let omitted = state.tables[&self.name].columns.len() - values.len();
        let mut total = omitted.checked_mul(9).ok_or(SqlError::Limit)?;
        for value in &values {
            total = total.checked_add(bytes(value)).ok_or(SqlError::Limit)?;
        }
        if total > MAX_ROW_BYTES {
            return Err(SqlError::Limit);
        }
        if cancel() {
            return Err(SqlError::Aborted);
        }
        Ok(values.into_iter().map(Expr::Literal).collect())
    }
}

#[cfg(test)]
#[path = "insert_values_tests.rs"]
mod tests;
