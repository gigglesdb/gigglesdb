//! SQL-only two-row scalar binding for ON CONFLICT. No persisted representation.
use super::*;
#[cfg(test)]
use crate::operations;

#[derive(Debug, Clone)]
pub(in crate::relational) struct Expression(Scalar);

impl Expression {
    // Preserve the private admission-test constructors without a second SQL AST.
    #[cfg(test)]
    pub(in crate::relational) fn admission_value(value: Expr) -> Self {
        Self(match value {
            Expr::Literal(value) => Scalar::Literal(value),
            Expr::Param(number) => Scalar::Param(number),
        })
    }
    #[cfg(test)]
    pub(in crate::relational) fn admission_field(
        source: operations::ConflictSource,
        name: String,
    ) -> Self {
        Self(Scalar::Column(ColumnRef {
            qualifier: Some(
                match source {
                    operations::ConflictSource::Incoming => "excluded",
                    operations::ConflictSource::Existing => "items",
                }
                .into(),
            ),
            name,
        }))
    }
    #[cfg(test)]
    pub(in crate::relational) fn admission_compare(a: Self, op: operations::Compare, b: Self) -> Self {
        let op = match op {
            operations::Compare::Eq => "=",
            operations::Compare::Ne => "<>",
            operations::Compare::Lt => "<",
            operations::Compare::Le => "<=",
            operations::Compare::Gt => ">",
            operations::Compare::Ge => ">=",
        };
        Self(Scalar::Predicate(Box::new(Filter::Compare(
            a.0,
            op.into(),
            b.0,
        ))))
    }
    pub(in crate::relational) fn sequence_dependencies(&self) -> Vec<Option<String>> {
        let mut dependencies = vec![];
        self.0.sequence_dependencies(&mut dependencies);
        dependencies
    }
    // Multi-row VALUES keeps its existing literal grammar, with explicit casts
    // admitted through the same scalar cast implementation as UPDATE.
    pub(in crate::relational) fn input_cast(p: &mut Parser) -> R<Self> {
        let expression = Self::parse(p)?;
        fn input(value: &Scalar) -> bool {
            match value {
                Scalar::Cast(value, _) | Scalar::Unary(_, value) => input(value),
                Scalar::Literal(_) | Scalar::Param(_) => true,
                _ => false,
            }
        }
        if !matches!(expression.0, Scalar::Cast(..)) || !input(&expression.0) {
            return Err(SqlError::Unsupported);
        }
        Ok(expression)
    }
    pub(in crate::relational) fn parse(p: &mut Parser) -> R<Self> {
        let start = p.at;
        let value = Scalar::parse_expr(p, 0, true)
            .map_err(|error| Self::subquery_error(p, start, error))?;
        value.reject_nested_aggregate()?;
        insert_values::shape(&value, 0, &mut 0)?;
        Ok(Self(value))
    }
    pub(in crate::relational) fn condition(p: &mut Parser) -> R<Self> {
        let start = p.at;
        let filter = Filter::parse_context(p, 0, 0, true)
            .map_err(|error| Self::subquery_error(p, start, error))?;
        let value = Scalar::Predicate(Box::new(filter));
        value.reject_nested_aggregate()?;
        insert_values::shape(&value, 0, &mut 0)?;
        Ok(Self(value))
    }
    // Only inspect the failed expression's consumed prefix. The scalar SELECT
    // baseline stays Syntax; membership subqueries are outside this grammar.
    fn subquery_error(p: &Parser, start: usize, error: SqlError) -> SqlError {
        if p.tokens[start..p.at.min(p.tokens.len())]
            .windows(3)
            .any(|tokens| {
                matches!(&tokens[0], Token::Word(word) if word == "in")
                    && tokens[1] == Token::Symbol('(')
                    && matches!(&tokens[2], Token::Word(word) if word == "select")
            })
        {
            SqlError::Unsupported
        } else {
            error
        }
    }
    fn resolver<'a>(
        table: &'a TableDef,
        alias: Option<&'a str>,
    ) -> impl Fn(&ColumnRef, usize) -> R<BoundColumn> + 'a {
        move |column, _| {
            let source = match column.qualifier.as_deref() {
                Some("excluded") => 1,
                Some(name) if name == table.name || Some(name) == alias => 0,
                Some(_) => return Err(SqlError::UnknownTable),
                None => {
                    col(table, &column.name)?;
                    return Err(SqlError::AmbiguousColumn);
                }
            };
            Ok(BoundColumn {
                source,
                column: col(table, &column.name)?,
            })
        }
    }
    fn assignment_target(target: &DataType) -> &DataType {
        // Keep scalar operators' result type as text during evaluation. Relabeling
        // their nodes as varchar/bpchar makes text overloads reject the result;
        // the conflict planner applies the destination column's typmod afterward.
        match target {
            DataType::Varchar | DataType::Bpchar => &DataType::Text,
            _ => target,
        }
    }
    pub(in crate::relational) fn infer(
        &self,
        table: &TableDef,
        alias: Option<&str>,
        target: &DataType,
        parameters: &mut [Option<DataType>],
    ) -> R<()> {
        let resolve = Self::resolver(table, alias);
        self.0.infer(
            matches!(self.0, Scalar::Param(n) if parameters.get(n - 1).is_none_or(Option::is_none))
                .then_some(target),
            parameters,
            &resolve,
            &[table, table],
        )?;
        if matches!(self.0, Scalar::Literal(Value::Text(_) | Value::Null)) {
            Scalar::Cast(Box::new(self.0.clone()), target.clone()).bind(
                &resolve,
                &[table, table],
                parameters,
            )?;
        } else {
            self.0
                .bind(&resolve, &[table, table], parameters)?
                .insert_assignment(Self::assignment_target(target))?;
        }
        Ok(())
    }
    pub(in crate::relational) fn bind(
        &self,
        table: &TableDef,
        alias: Option<&str>,
        target: &DataType,
        parameters: &[Option<DataType>],
        clock: Option<&clock::ExecutionClock>,
    ) -> R<BoundExpression> {
        let literal = matches!(self.0, Scalar::Literal(_));
        let mut value = self
            .0
            .bind(&Self::resolver(table, alias), &[table, table], parameters)?;
        if !literal {
            value = value.insert_assignment(Self::assignment_target(target))?;
        }
        value.attach_clock(clock);
        Ok(BoundExpression {
            value,
            literal,
            target: target.clone(),
            clock: clock.cloned(),
        })
    }
    pub(in crate::relational) fn visit_values(
        &self,
        params: &[Value],
        visit: &mut impl FnMut(&Value) -> R<()>,
    ) -> R<()> {
        fn scalar(
            value: &Scalar,
            params: &[Value],
            visit: &mut impl FnMut(&Value) -> R<()>,
        ) -> R<()> {
            match value {
                Scalar::Literal(value) => visit(value)?,
                Scalar::Param(n) => visit(params.get(n - 1).ok_or(SqlError::Parameters)?)?,
                Scalar::Predicate(f) => f.visit_scalars(&mut |s| scalar(s, params, visit))?,
                Scalar::Case(c) => {
                    for (f, s) in &c.arms {
                        f.visit_scalars(&mut |s| scalar(s, params, visit))?;
                        scalar(s, params, visit)?;
                    }
                    if let Some(s) = &c.otherwise {
                        scalar(s, params, visit)?;
                    }
                }
                Scalar::Cast(s, _) | Scalar::Unary(_, s) => scalar(s, params, visit)?,
                Scalar::Binary(_, a, b) => {
                    scalar(a, params, visit)?;
                    scalar(b, params, visit)?;
                }
                Scalar::Call(_, args) => {
                    for s in args {
                        scalar(s, params, visit)?;
                    }
                }
                Scalar::SubstringOrder(args) => {
                    for s in args {
                        scalar(s, params, visit)?;
                    }
                }
                Scalar::Column(_) => {}
            }
            Ok(())
        }
        scalar(&self.0, params, visit)
    }
}

pub(in crate::relational) struct BoundExpression {
    value: BoundScalar,
    literal: bool,
    target: DataType,
    clock: Option<clock::ExecutionClock>,
}
impl BoundExpression {
    pub(in crate::relational) fn evaluate(
        &self,
        old: &SqlRow,
        incoming: &[Value],
        params: &[Value],
        bytes: &mut usize,
    ) -> R<Value> {
        let row = Tuple {
            values: vec![old.values.clone(), incoming.to_vec()],
            version: old.version,
        };
        let value = self.value.eval(&row, params, bytes)?;
        if self.literal {
            assignment::literal(value, &self.target, self.clock.as_ref())
        } else {
            Ok(value)
        }
    }
}
