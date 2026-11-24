//! Enum metadata is captured at binding time; persisted values remain text.
use super::*;
use std::{cell::RefCell, sync::Arc};

thread_local! {
    static TYPES: RefCell<BTreeMap<u64, TypeDef>> = const { RefCell::new(BTreeMap::new()) };
}
pub(in crate::relational::query) struct Scope(BTreeMap<u64, TypeDef>);
impl Scope {
    pub(in crate::relational::query) fn enter(state: &Relational) -> Self {
        let mut types = BTreeMap::new();
        if !state.ext.types.is_empty() {
            let mut candidates: BTreeMap<_, _> = state
                .ext
                .columns
                .iter()
                .filter_map(|(&id, column)| {
                    let ty = state.ext.types.get(&column.type_id?)?;
                    matches!(ty, TypeDef::Enum { .. }).then_some((id, ty))
                })
                .collect();

            // Private catalog states can contain metadata for absent columns.
            if !candidates.is_empty() {
                for column in state.tables.values().flat_map(|table| &table.columns) {
                    if let Some(ty) = candidates.remove(&column.id) {
                        types.insert(column.id, ty.clone());
                        if candidates.is_empty() {
                            break;
                        }
                    }
                }
            }
        }
        Self(TYPES.with(|slot| slot.replace(types)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        TYPES.with(|slot| slot.replace(std::mem::take(&mut self.0)));
    }
}
impl BoundScalar {
    pub(in crate::relational::query) fn enum_type(&self, tables: &[&TableDef]) -> Option<TypeDef> {
        matches!(self.ty, DataType::Text | DataType::TextArray)
            .then(|| self.user_enum_type(tables))
            .flatten()
    }

    pub(super) fn user_enum_type(&self, tables: &[&TableDef]) -> Option<TypeDef> {
        if let Node::Column(c, _) = &self.node {
            column_type(tables[c.source].columns[c.column].id)
        } else {
            self.attached_enum().cloned()
        }
    }

    pub(in crate::relational::query) fn attached_enum(&self) -> Option<&TypeDef> {
        self.enum_definition().map(Arc::as_ref)
    }

    pub(in crate::relational::query) fn enum_definition(&self) -> Option<&Arc<TypeDef>> {
        match &self.node {
            Node::Call(name, args)
                if aggregate_order::internal(name)
                    || collation::internal(name)
                    || matches!(name.as_str(), "\0array_subscript" | "\0plan_cardinality" | "nullif") =>
            {
                args[0].enum_definition()
            }
            Node::Call(name, args)
                if greatest::known_name(name)
                    || concat::array_operator(name)
                    || matches!(name.as_str(), "coalesce" | array_constructor::NAME) =>
            {
                args.iter().find_map(Self::enum_definition)
            }
            Node::Case(arms, otherwise) => otherwise
                .iter()
                .map(Box::as_ref)
                .chain(arms.iter().map(|(_, value)| value))
                .find_map(Self::enum_definition),
            Node::Domain(_, ty) if matches!(ty.as_ref(), TypeDef::Enum { .. }) => Some(ty),
            _ => None,
        }
    }

    pub(in crate::relational::query) fn cast_enum_oid(&self, state: &Relational) -> u32 {
        self.enum_oid(state, self.ty == DataType::TextArray)
    }

    pub(in crate::relational::query) fn enum_oid(&self, state: &Relational, array: bool) -> u32 {
        if let Some(ty) = self.attached_enum()
            && let Some((id, _)) = state.type_by_name(ty.name())
            && let Ok(oid) = type_oid(id)
        {
            oid + u32::from(array)
        } else {
            0
        }
    }

    pub(in crate::relational::query) fn enum_metadata(ty: DataType, enum_type: &TypeDef) -> Self {
        Self { ty, node: Node::Literal(Value::Null) }.with_enum(enum_type)
    }

    pub(in crate::relational::query) fn with_enum(self, ty: &TypeDef) -> Self {
        if self.attached_enum() == Some(ty) {
            return self;
        }
        Self {
            ty: self.ty.clone(),
            node: Node::Domain(Box::new(self), Arc::new(ty.clone())),
        }
    }

    pub(in crate::relational::query) fn enum_literal(
        value: &Value,
        storage: &DataType,
        ty: &TypeDef,
    ) -> R<Self> {
        let node = literal_cast_node(value.clone(), storage)?;
        let Node::Literal(value) = &node else {
            return Err(SqlError::InternalError);
        };
        user_types::coerce_user_type(ty, storage, value)?;
        Ok(Self { ty: storage.clone(), node }.with_enum(ty))
    }

    pub(in crate::relational::query) fn enum_rank(self, ty: &TypeDef) -> R<Self> {
        let TypeDef::Enum { labels, .. } = ty else {
            return Err(SqlError::Type);
        };
        if let Node::Literal(Value::Text(label)) = &self.node {
            enum_alter::check_value(ty, labels, label)?;
        }
        Ok(Self {
            ty: DataType::BigInt,
            node: Node::EnumSort(Box::new(self), labels.clone()),
        })
    }
    pub(in crate::relational::query) fn compare_values(&self, a: &Value, b: &Value) -> Ordering {
        if let Some(ty) = self.attached_enum() {
            return compare_stored(ty, &self.ty, a, b);
        }
        collation::compare(self.explicit_collation().expect("collation bound"), a, b, &self.ty)
    }

    pub(in crate::relational::query) fn type_name(&self) -> String {
        type_name(&self.ty, self.attached_enum())
    }

    pub(in crate::relational::query) fn type_identity(&self) -> (&DataType, Option<&TypeDef>) {
        (&self.ty, self.attached_enum())
    }
}

pub(super) fn missing_operator(op: &str, a: &BoundScalar, b: &BoundScalar) -> SqlError {
    missing_operator_types(op, &a.type_name(), &b.type_name())
}

pub(super) fn missing_operator_types(op: &str, left: &str, right: &str) -> SqlError {
    user_types::view_replace_error(format!(
        "operator does not exist: {left} {op} {right}",
    ));
    user_types::sql_error_hint("No operator matches the given name and argument types. You might need to add explicit type casts.");
    SqlError::UndefinedFunction
}

fn type_name(storage: &DataType, ty: Option<&TypeDef>) -> String {
    ty.map_or_else(
        || type_display(storage).to_owned(),
        |ty| user_types::enum_display(ty, *storage == DataType::TextArray),
    )
}

/// Common-type selection for expressions with an enum input. Ignore SQL
/// unknowns before calling this; equal storage types can have different identities.
pub(in crate::relational::query) fn common_enum<'a>(
    construct: &str,
    mut inputs: impl Iterator<Item = (&'a DataType, Option<&'a TypeDef>)> + Clone,
) -> R<Option<&'a TypeDef>> {
    let Some((storage, enum_type)) = inputs.next() else {
        return Ok(None);
    };
    if enum_type.is_none() && inputs.clone().all(|(_, ty)| ty.is_none()) {
        return Ok(None);
    }
    for (other_storage, other) in inputs {
        if other_storage == storage && other == enum_type {
            continue;
        }
        let from = type_name(other_storage, other);
        let to = type_name(storage, enum_type);
        if other_storage.is_array() && storage.is_array()
            || !other_storage.is_array() && !storage.is_array() && other.is_some() && enum_type.is_some()
        {
            user_types::view_replace_error(format!("{construct} could not convert type {from} to {to}"));
            return Err(SqlError::CannotCoerce);
        }
        let construct = if construct == "CASE/WHEN" { "CASE" } else { construct };
        user_types::view_replace_error(format!("{construct} types {to} and {from} cannot be matched"));
        return Err(SqlError::Type);
    }
    Ok(enum_type)
}

pub(in crate::relational::query) fn compare_stored(
    ty: &TypeDef,
    storage: &DataType,
    a: &Value,
    b: &Value,
) -> Ordering {
    let TypeDef::Enum { labels, .. } = ty else {
        unreachable!("enum comparison requires enum metadata");
    };
    if *storage == DataType::TextArray {
        return user_types::probe_preserving_error_message(|| compare_arrays(labels, a, b))
            .unwrap_or_else(|_| cmp(a, b));
    }
    if let (Value::Text(a), Value::Text(b)) = (a, b) {
        return compare_labels(labels, a, b);
    }
    cmp(a, b)
}

pub(super) fn has_column(id: u64) -> bool {
    TYPES.with_borrow(|types| types.contains_key(&id))
}

pub(super) fn column_type(id: u64) -> Option<TypeDef> {
    TYPES.with_borrow(|types| types.get(&id).cloned())
}

pub(super) fn compare_labels(labels: &[String], a: &str, b: &str) -> Ordering {
    labels.iter().position(|label| label == a).cmp(&labels.iter().position(|label| label == b))
}

pub(super) fn compare_arrays(labels: &[String], a: &Value, b: &Value) -> R<Ordering> {
    logical_types::compare_arrays(a, b, |a, b| Ok(compare_labels(labels, a, b)))
}

#[cfg(test)]
#[path = "enum_order_tests.rs"]
mod tests;
