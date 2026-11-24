//! Logical type information for enum inputs, whose values use text storage.
use super::*;
use std::{cell::RefCell, sync::Arc};

#[derive(Clone, Debug)]
pub struct EnumParameter {
    oid: u32,
    storage: DataType,
    definition: Arc<TypeDef>,
}

impl EnumParameter {
    pub fn oid(&self) -> u32 {
        self.oid
    }

    pub fn storage_type(&self) -> &DataType {
        &self.storage
    }

    pub(crate) fn display_name(&self, search_path: &[String]) -> String {
        let _path = user_types::TypeDisplayScope::enter(search_path, None);
        user_types::enum_display(&self.definition, self.storage == DataType::TextArray)
    }

    pub fn validate(&self, value: &Value) -> R<()> {
        user_types::coerce_user_type(&self.definition, &self.storage, value)
    }
}

pub type EnumParameters = BTreeMap<usize, EnumParameter>;

pub(crate) fn could_be_enum_oid(oid: u32) -> bool {
    oid >= user_types::USER_TYPE_OID_BASE
        && (oid - user_types::USER_TYPE_OID_BASE) % user_types::USER_TYPE_OID_STRIDE <= 1
}

pub(crate) fn declared(state: &Relational, oids: &[u32]) -> EnumParameters {
    oids.iter()
        .enumerate()
        .filter_map(|(index, &oid)| {
            if !could_be_enum_oid(oid) {
                return None;
            }
            let slot = oid - user_types::USER_TYPE_OID_BASE;
            let id = u64::from(slot / user_types::USER_TYPE_OID_STRIDE);
            let definition @ TypeDef::Enum { .. } = state.ext.types.get(&id)? else {
                return None;
            };
            let storage = if slot % user_types::USER_TYPE_OID_STRIDE == 1 {
                DataType::TextArray
            } else {
                DataType::Text
            };
            Some((index, EnumParameter { oid, storage, definition: Arc::new(definition.clone()) }))
        })
        .collect()
}

type Bindings = BTreeMap<usize, (DataType, Arc<TypeDef>)>;

thread_local! {
    static PARAMETERS: RefCell<Option<Bindings>> = const { RefCell::new(None) };
}

// Binder passes can clone or reseed their storage type slots. The logical
// types follow the statement parameter numbers across those passes.
pub(crate) struct Scope(Option<Bindings>);

impl Scope {
    pub(crate) fn enter(declared: Option<&EnumParameters>) -> Self {
        let bindings = declared
            .into_iter()
            .flatten()
            .map(|(&index, parameter)| (index, (parameter.storage.clone(), parameter.definition.clone())))
            .collect();
        Self(PARAMETERS.with(|slot| slot.replace(Some(bindings))))
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        PARAMETERS.with(|slot| slot.replace(std::mem::take(&mut self.0)));
    }
}

pub(super) fn infer(
    index: usize,
    slot: &mut Option<DataType>,
    storage: &DataType,
    definition: &TypeDef,
) {
    if slot.is_none() {
        PARAMETERS.with_borrow_mut(|parameters| {
            if let Some(parameters) = parameters {
                parameters
                    .entry(index)
                    .or_insert_with(|| (storage.clone(), Arc::new(definition.clone())));
            }
        });
        *slot = Some(storage.clone());
    }
}

pub(super) fn definition(index: usize, storage: &DataType) -> Option<Arc<TypeDef>> {
    PARAMETERS.with_borrow(|parameters| {
        parameters
            .as_ref()?
            .get(&index)
            .filter(|(ty, _)| ty == storage)
            .map(|(_, definition)| definition.clone())
    })
}

pub(super) fn describe(state: &Relational) -> R<EnumParameters> {
    PARAMETERS.with_borrow(|parameters| {
        parameters
            .iter()
            .flatten()
            .map(|(&index, (storage, definition))| {
                let (id, _) = state
                    .type_by_name(definition.name())
                    .ok_or(SqlError::UndefinedObject)?;
                let oid = type_oid(id)? + u32::from(*storage == DataType::TextArray);
                Ok((
                    index,
                    EnumParameter {
                        oid,
                        storage: storage.clone(),
                        definition: definition.clone(),
                    },
                ))
            })
            .collect()
    })
}

pub(super) fn infer_column(
    expr: &Expr,
    column: &ColumnDef,
    state: &Relational,
    parameters: &mut [Option<DataType>],
) -> R<()> {
    if let Expr::Param(n) = expr
        && let Some(id) = state
            .ext
            .columns
            .get(&column.id)
            .and_then(|column| column.type_id)
        && let Some(definition @ TypeDef::Enum { .. }) = state.ext.types.get(&id)
    {
        infer(
            n - 1,
            parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?,
            &column.data_type,
            definition,
        );
    }
    Ok(())
}

pub(super) fn validate(index: usize, storage: &DataType, value: &Value) -> R<()> {
    if let Some(definition) = definition(index, storage) {
        user_types::coerce_user_type(&definition, storage, value)?;
    }
    Ok(())
}
