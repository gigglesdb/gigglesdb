//! Distinguish enum comparisons from comparisons of the label's rendered text.
use super::*;

pub(in crate::relational) fn rename_enum_check(
    state: &Relational,
    table: &TableDef,
    expr: &str,
    columns: &BTreeSet<&str>,
    old: &str,
    new: &str,
) -> R<Option<String>> {
    let _scope = enum_text::Scope::stored(state, table, false);
    let body = expr.strip_suffix(" NOT VALID").unwrap_or(expr);
    let body = body
        .strip_prefix("CHECK (")
        .and_then(|body| body.strip_suffix(')'))
        .ok_or(SqlError::Unsupported)?;
    let mut filter = expression(lex(body)?)?;
    let mut rename = Rename {
        columns,
        old,
        new,
        changed: false,
    };
    rename.filter(&mut filter)?;
    if !rename.changed {
        return Ok(None);
    }
    let suffix = if expr.ends_with(" NOT VALID") {
        " NOT VALID"
    } else {
        ""
    };
    Ok(Some(format!("CHECK ({}){suffix}", deparse(&filter)?)))
}

struct Rename<'a> {
    columns: &'a BTreeSet<&'a str>,
    old: &'a str,
    new: &'a str,
    changed: bool,
}

impl Rename<'_> {
    fn enum_column(&self, value: &Scalar) -> bool {
        matches!(value, Scalar::Column(column) if self.columns.contains(column.name.as_str()))
    }

    fn comparison(&mut self, other: &mut Scalar) -> R<()> {
        if self.enum_column(other) {
            return Ok(());
        }
        let label = enum_text::text_literal(other).ok_or(SqlError::Unsupported)?;
        // The existing CHECK deparser writes enum input literals as ::text.
        // Keep that representation while updating the label paired with an enum.
        let mut value = label.to_owned();
        if label == self.old {
            value = self.new.into();
            self.changed = true;
        }
        *other = Scalar::Literal(Value::Text(value));
        Ok(())
    }

    fn filter(&mut self, filter: &mut Filter) -> R<()> {
        match filter {
            Filter::Compare(left, _, right) if self.enum_column(left) => self.comparison(right),
            Filter::Compare(left, _, right) if self.enum_column(right) => self.comparison(left),
            Filter::Compare(left, _, right) => {
                self.scalar(left)?;
                self.scalar(right)
            }
            Filter::Logic(left, _, right) => {
                self.filter(left)?;
                self.filter(right)
            }
            Filter::Not(inner) | Filter::Is(inner, ..) => self.filter(inner),
            Filter::Null(value, _) if self.enum_column(value) => Ok(()),
            Filter::Null(value, _) | Filter::Scalar(value) => self.scalar(value),
            _ => Err(SqlError::Unsupported),
        }
    }

    fn scalar(&mut self, value: &mut Scalar) -> R<()> {
        if self.enum_column(value) {
            return Err(SqlError::Unsupported);
        }
        match value {
            Scalar::Cast(inner, DataType::Text)
                if matches!(inner.as_ref(), Scalar::Literal(Value::Text(_))) =>
            {
                *value = *inner.clone();
            }
            Scalar::Cast(inner, DataType::Text | DataType::Varchar) if self.enum_column(inner) => {}
            Scalar::Predicate(filter) => self.filter(filter)?,
            Scalar::Cast(inner, _) | Scalar::Domain(inner, ..) | Scalar::Unary(_, inner) => {
                self.scalar(inner)?
            }
            Scalar::Binary(_, left, right) => {
                self.scalar(left)?;
                self.scalar(right)?;
            }
            Scalar::Call(_, args) => {
                for arg in args {
                    self.scalar(arg)?;
                }
            }
            Scalar::SubstringOrder(args) => {
                for arg in args {
                    self.scalar(arg)?;
                }
            }
            Scalar::Case(case) => {
                for (condition, result) in &mut case.arms {
                    self.filter(condition)?;
                    self.scalar(result)?;
                }
                if let Some(otherwise) = &mut case.otherwise {
                    self.scalar(otherwise)?;
                }
            }
            Scalar::Column(_) | Scalar::Literal(_) | Scalar::Param(_) => (),
        }
        Ok(())
    }
}
