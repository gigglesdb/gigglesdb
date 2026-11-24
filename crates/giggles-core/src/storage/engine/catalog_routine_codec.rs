//! V2 catalog kinds 28 and 29, independent of the Legacy namespace.
use super::{corrupt, decode_version, encode_version, object_key};
use crate::CoreError;
use crate::relational::{self, Routine, Trigger, routines::TRIGGER_TYPE, triggers};
use crate::storage::engine::catalog::CatalogParts;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const ROUTINE: u8 = 28;
pub(crate) const TRIGGER: u8 = 29;

pub(crate) fn routine_key(id: u64) -> Vec<u8> {
    object_key(ROUTINE, id)
}
pub(crate) fn trigger_key(id: u64) -> Vec<u8> {
    object_key(TRIGGER, id)
}

// Routine and Trigger are the frozen v1 DTOs. All integers use fixed-width BE
// bincode. Strings and vectors carry a u64 length, Option and bool use u8 tags.
pub(crate) fn encode_routine(routine: &Routine) -> Result<Vec<u8>, CoreError> {
    validate_routine(routine)?;
    encode_version(1, routine)
}
pub(crate) fn decode_routine(bytes: &[u8]) -> Result<Routine, CoreError> {
    let routine = decode_version(1, bytes)?;
    validate_routine(&routine)?;
    Ok(routine)
}
pub(crate) fn encode_trigger(trigger: &Trigger) -> Result<Vec<u8>, CoreError> {
    validate_trigger(trigger)?;
    encode_version(1, trigger)
}
pub(crate) fn decode_trigger(bytes: &[u8], id: u64) -> Result<Trigger, CoreError> {
    let trigger: Trigger = decode_version(1, bytes)?;
    validate_trigger(&trigger)?;
    if trigger.id != id {
        return Err(corrupt("trigger key and value IDs differ"));
    }
    Ok(trigger)
}
fn identifier(text: &str) -> bool {
    !text.is_empty() && text.len() <= 63 && !text.contains('\0')
}
fn validate_routine(r: &Routine) -> Result<(), CoreError> {
    let cost = f32::from_bits(r.cost_bits);
    if !identifier(&r.name)
        || r.language != "plpgsql"
        || r.return_type != TRIGGER_TYPE
        || !r.arguments.is_empty()
        || r.body.contains('\0')
        || r.body.len() > relational::V2_MAX_SQL
        || !matches!(r.volatility, b'v' | b's' | b'i')
        || !matches!(r.parallel, b'u' | b'r' | b's')
        || !cost.is_finite()
        || cost <= 0.0
        || r.owner == 0
    {
        return Err(corrupt("invalid routine definition"));
    }
    Ok(())
}
fn unique_ids(ids: &[u64]) -> bool {
    ids.len() <= relational::MAX_COLUMNS
        && !ids.contains(&0)
        && ids.iter().collect::<BTreeSet<_>>().len() == ids.len()
}
fn validate_trigger(t: &Trigger) -> Result<(), CoreError> {
    if t.id == 0
        || t.table == 0
        || t.routine == 0
        || !identifier(&t.name)
        || t.events == 0
        || t.events & !(triggers::INSERT | triggers::DELETE | triggers::UPDATE) != 0
        || !matches!(t.enabled, b'O' | b'A' | b'R' | b'D')
        || !unique_ids(&t.update_columns)
        || !unique_ids(&t.when_columns)
        || (!t.update_columns.is_empty() && t.events & triggers::UPDATE == 0)
        || (!t.when_columns.is_empty() && (t.when.is_none() || !t.row))
        || t.args.len() > i16::MAX as usize
        || t.args.iter().any(|arg| arg.contains('\0'))
        || t.when
            .as_ref()
            .is_some_and(|text| text.is_empty() || text.contains('\0'))
    {
        return Err(corrupt("invalid trigger definition"));
    }
    Ok(())
}
pub(crate) fn validate_catalog(parts: &CatalogParts) -> Result<(), CoreError> {
    if parts.ext.routines.is_empty() && parts.ext.triggers.is_empty() {
        return Ok(());
    }
    let schemas: BTreeSet<_> = parts.ext.schemas.values().map(|s| s.id).collect();
    let mut signatures = BTreeSet::new();
    for (&id, routine) in &parts.ext.routines {
        validate_routine(routine)?;
        relational::type_oid(id).map_err(CoreError::Sql)?;
        if id == 0
            || (routine.schema != 0 && !schemas.contains(&routine.schema))
            || !signatures.insert((routine.schema, &routine.name))
        {
            return Err(corrupt("invalid routine identity or schema"));
        }
    }
    let tables: BTreeMap<_, _> = parts.tables.values().map(|t| (t.id, t)).collect();
    for (&id, triggers) in &parts.ext.triggers {
        let table = tables
            .get(&id)
            .ok_or_else(|| corrupt("trigger table is absent"))?;
        let columns: BTreeSet<_> = table.logical_columns().map(|c| c.id).collect();
        if triggers.is_empty() {
            return Err(corrupt("empty trigger table index"));
        }
        for (name, trigger) in triggers {
            validate_trigger(trigger)?;
            relational::type_oid(trigger.id).map_err(CoreError::Sql)?;
            if trigger.table != id
                || trigger.name != *name
                || !parts.ext.routines.contains_key(&trigger.routine)
                || trigger
                    .update_columns
                    .iter()
                    .chain(&trigger.when_columns)
                    .any(|c| !columns.contains(c))
            {
                return Err(corrupt("invalid trigger identity or dependency"));
            }
        }
    }
    Ok(())
}
pub(crate) fn by_table(
    triggers: impl IntoIterator<Item = Trigger>,
) -> Result<BTreeMap<u64, BTreeMap<String, Trigger>>, CoreError> {
    let mut tables = BTreeMap::<u64, BTreeMap<String, Trigger>>::new();
    for trigger in triggers {
        if tables
            .entry(trigger.table)
            .or_default()
            .insert(trigger.name.clone(), trigger)
            .is_some()
        {
            return Err(corrupt("duplicate trigger name for table"));
        }
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::catalog_codec::{CatalogKind, classify_catalog_key};

    #[test]
    fn routine_and_trigger_v1() {
        let routine = Routine {
            schema: 0,
            name: "f".into(),
            arguments: vec![],
            return_type: TRIGGER_TYPE,
            language: "plpgsql".into(),
            body: " BEGIN RETURN NEW; END ".into(),
            volatility: b'v',
            strict: false,
            security_definer: false,
            leakproof: false,
            parallel: b'u',
            cost_bits: 100f32.to_bits(),
            owner: 10,
            program: Default::default(),
        };
        let trigger = Trigger {
            id: 48,
            table: 44,
            name: "t".into(),
            routine: 46,
            events: triggers::UPDATE,
            before: true,
            row: true,
            enabled: b'O',
            update_columns: vec![45],
            args: vec!["foo".into()],
            when: None,
            when_columns: vec![],
        };
        let mut r = encode_routine(&routine).unwrap();
        routine.compiled_function().unwrap();
        assert_eq!(encode_routine(&routine).unwrap(), r);
        let mut t = encode_trigger(&trigger).unwrap();
        assert_eq!(r[0], 1);
        assert_eq!(t[0], 1);
        assert_eq!(&r[1..9], &0u64.to_be_bytes());
        assert_eq!(&t[1..9], &48u64.to_be_bytes());
        assert_eq!(decode_routine(&r).unwrap(), routine);
        assert_eq!(decode_trigger(&t, 48).unwrap(), trigger);
        assert!(decode_trigger(&t, 46).is_err());
        assert_eq!(
            classify_catalog_key(&routine_key(46)).unwrap(),
            CatalogKind::Routine
        );
        assert_eq!(
            classify_catalog_key(&trigger_key(48)).unwrap(),
            CatalogKind::Trigger
        );
        assert!(classify_catalog_key(&trigger_key(0)).is_err());
        r[0] = 2;
        t[0] = 2;
        assert!(decode_routine(&r).is_err());
        assert!(decode_trigger(&t, 48).is_err());
    }
}
