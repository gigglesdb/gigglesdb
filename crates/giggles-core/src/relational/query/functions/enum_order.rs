//! Enum metadata is captured at binding time; persisted values remain text.
use super::*;
use std::cell::RefCell;

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
        match &self.node {
            Node::Call(name, args) if aggregate_order::internal(name) || collation::internal(name) => {
                args[0].attached_enum()
            }
            Node::Domain(_, ty @ TypeDef::Enum { .. }) => Some(ty),
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
            node: Node::Domain(Box::new(self), ty.clone()),
        }
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
        if let Node::Call(name,args) = &self.node
            && (aggregate_order::internal(name) || greatest::known_name(name)
                && args.first().is_some_and(|a| matches!(a.node, Node::Domain(_, TypeDef::Enum { .. })))) {
            return args[0].compare_values(a,b);
        }
        if let Some(TypeDef::Enum { labels, .. }) = self.attached_enum() {
            if self.ty == DataType::TextArray {
                return user_types::probe_preserving_error_message(|| compare_arrays(labels, a, b))
                    .unwrap_or_else(|_| cmp(a, b));
            }
            if let (Value::Text(a), Value::Text(b)) = (a, b) {
                return compare_labels(labels, a, b);
            }
        }
        collation::compare(self.explicit_collation().expect("collation bound"), a, b, &self.ty)
    }
}

pub(super) fn has_column(id: u64) -> bool {
    TYPES.with_borrow(|types| types.contains_key(&id))
}

pub(super) fn column_type(id: u64) -> Option<TypeDef> {
    TYPES.with_borrow(|types| types.get(&id).cloned())
}

fn compare_labels(labels: &[String], a: &str, b: &str) -> Ordering {
    labels.iter().position(|label| label == a).cmp(&labels.iter().position(|label| label == b))
}

pub(super) fn compare_arrays(labels: &[String], a: &Value, b: &Value) -> R<Ordering> {
    logical_types::compare_arrays(a, b, |a, b| Ok(compare_labels(labels, a, b)))
}

#[cfg(test)]
#[path = "enum_order_tests.rs"]
mod tests;
