//! Enum metadata is captured at binding time; persisted values remain text.
use super::*;
use std::cell::RefCell;

thread_local! {
    static TYPES: RefCell<BTreeMap<u64, TypeDef>> = const { RefCell::new(BTreeMap::new()) };
}
pub(in crate::relational::query) struct Scope(BTreeMap<u64, TypeDef>);
impl Scope {
    pub(in crate::relational::query) fn enter(state: &Relational) -> Self {
        let types = state
            .tables
            .values()
            .flat_map(|table| &table.columns)
            .filter_map(|column| {
                let ty = state.column_type(column.id)?;
                matches!(ty, TypeDef::Enum { .. }).then(|| (column.id, ty.clone()))
            })
            .collect();
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
        match &self.node {
            Node::Column(c, _) => TYPES.with(|slot| {
                slot.borrow()
                    .get(&tables[c.source].columns[c.column].id)
                    .cloned()
            }),
            Node::Domain(_, ty @ TypeDef::Enum { .. }) => Some(ty.clone()),
            _ => None,
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
            user_types::view_replace_error(format!(
                "invalid input value for enum {name}: \"{label}\""
            ));
            return Err(SqlError::InvalidTextRepresentation);
        }
        Ok(Self {
            ty: DataType::BigInt,
            node: Node::EnumSort(Box::new(self), labels.clone()),
        })
    }
    pub(in crate::relational::query) fn compare_values(&self, a: &Value, b: &Value) -> Ordering {
        if let Node::Domain(_, TypeDef::Enum { labels, .. }) = &self.node
            && let (Value::Text(a), Value::Text(b)) = (a, b)
        {
            return labels
                .iter()
                .position(|v| v == a)
                .cmp(&labels.iter().position(|v| v == b));
        }
        cmp(a, b)
    }
}
