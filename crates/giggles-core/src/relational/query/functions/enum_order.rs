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
        if self.ty == DataType::Text { self.user_enum_type(tables) } else { None }
    }

    pub(super) fn user_enum_type(&self, tables: &[&TableDef]) -> Option<TypeDef> {
        match &self.node {
            Node::Call(name, args) if aggregate_order::internal(name) => args[0].user_enum_type(tables),
            Node::Column(c, _) => TYPES.with(|slot| {
                slot.borrow()
                    .get(&tables[c.source].columns[c.column].id)
                    .cloned()
            }),
            Node::Domain(_, ty @ TypeDef::Enum { .. }) => Some(ty.clone()),
            _ => None,
        }
    }
    pub(in crate::relational::query) fn cast_enum_oid(&self, state: &Relational) -> u32 {
        if let Node::Domain(_, TypeDef::Enum { name, .. }) = &self.node
            && let Some((id, _)) = state.type_by_name(name)
            && let Ok(oid) = type_oid(id)
        {
            oid + u32::from(self.ty == DataType::TextArray)
        } else {
            0
        }
    }

    pub(in crate::relational::query) fn with_enum(self, ty: &TypeDef) -> Self {
        Self {
            ty: self.ty.clone(),
            node: Node::Domain(Box::new(self), ty.clone()),
        }
    }
    pub(in crate::relational::query) fn enum_rank(self, ty: &TypeDef) -> R<Self> {
        let TypeDef::Enum { labels, name, .. } = ty else {
            return Err(SqlError::Type);
        };
        if let Node::Literal(Value::Text(label)) = &self.node
            && !labels.contains(label)
        {
            return Err(user_types::enum_input_error(name, label));
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
        if self.ty == DataType::Text
            && let Node::Domain(_, TypeDef::Enum { labels, .. }) = &self.node
            && let (Value::Text(a), Value::Text(b)) = (a, b)
        {
            return labels
                .iter()
                .position(|v| v == a)
                .cmp(&labels.iter().position(|v| v == b));
        }
        collation::compare(self.explicit_collation().expect("collation bound"), a, b, &self.ty)
    }
}

#[cfg(test)]
#[path = "enum_order_tests.rs"]
mod tests;
