//! SQL defaults use the existing v2 counter codec and durable reservations.
use super::*;

pub(super) fn callback(
    db: Arc<v2::V2Db>,
    image: relational::Relational,
    baseline: BTreeMap<u64, u128>,
    pending: sequences::Pending,
    values: sequences::SessionValues,
) -> relational::clock::SequenceCallback {
    Arc::new(move |operation, name, value, called| {
        crate::cancellation::check()?;
        let name = sequences::sequence_name(name).map_err(CoreError::Sql)?;
        let (schema, name) = split_key(&name);
        let name = relation_key(schema, name);
        let (&id, def) = image
            .catalog_ext()
            .sequences
            .iter()
            .find(|(_, def)| def.name == name)
            .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
        let mut live = crate::cancellation::lock(&db.sql_counters)?;
        let mut check = db.begin(Isolation::ReadCommitted);
        // Preserve the fresh committed-incarnation check without cloning every
        // table, constraint and sequence into CatalogParts on every value.
        let committed = check
            .get(Keyspace::Catalog, &engine::catalog_codec::sequence_key(id))?
            .map(|bytes| engine::catalog_codec::decode_sequence(&bytes))
            .transpose()?
            .is_some_and(|current| current.incarnation == def.incarnation);
        if !committed && baseline.get(&id) == Some(&def.incarnation) {
            return Err(CoreError::Sql(SqlError::UnknownTable));
        }
        if operation == "currval" {
            let values = values.lock().map_err(|_| CoreError::Fenced)?;
            return sequences::currval(&values, id, def.incarnation)
                .map(relational::Value::Int)
                .map_err(CoreError::Sql);
        }
        let mut state = empty(0);
        state.sql = image.clone();
        let mut pending = pending.lock().map_err(|_| CoreError::Fenced)?;
        let key = engine::catalog_codec::counter_key(id, def.incarnation);
        let prior = if committed {
            match live
                .get(&id)
                .filter(|counter| counter.incarnation == def.incarnation)
            {
                Some(counter) => Some(counter.clone()),
                None => db
                    .kv
                    .get(Keyspace::Seq, &key)?
                    .map(|bytes| engine::catalog_codec::decode_counter(&key, &bytes, def))
                    .transpose()?,
            }
        } else {
            pending.get(&id).cloned()
        };
        if let Some(counter) = &prior {
            state.seq.insert(id, counter.clone());
        }
        let result = match operation {
            "nextval" => sequences::next(&mut state, id),
            "setval" => sequences::setval(
                &mut state,
                id,
                value.ok_or(CoreError::Sql(SqlError::Parameters))?,
                called,
            ),
            _ => Err(SqlError::Unsupported),
        }
        .map_err(CoreError::Sql)?;
        let counter = state
            .seq
            .remove(&id)
            .expect("successful sequence evaluation");
        if committed {
            // next() already computes the bounded 32-step recovery lookahead.
            // The unchanged codec writes log_hw as the recovered last_value.
            // A wrap must be written even when its highwater equals the old one.
            let reservation_changed = prior.as_ref().is_none_or(|old| {
                !old.is_called
                    || old.log_hw != counter.log_hw
                    || (def.cycle
                        && if def.increment > 0 {
                            counter.last_value < old.last_value
                        } else {
                            counter.last_value > old.last_value
                        })
            });
            if operation == "setval" || reservation_changed {
                db.sequencer.write_counter(
                    key,
                    engine::catalog_codec::encode_counter(&counter, def)?,
                    256,
                )?;
            }
            live.insert(id, counter);
        } else {
            pending.insert(id, counter);
        }
        if operation == "nextval" || called {
            values
                .lock()
                .map_err(|_| CoreError::Fenced)?
                .insert((id, def.incarnation), result);
        }
        Ok(relational::Value::Int(result))
    })
}

pub(super) fn stage_pending(
    db: &v2::V2Db,
    txn: &mut Transaction,
    pending: &sequences::Pending,
) -> Result<(), CoreError> {
    let parts = db.catalog().parts(txn)?;
    for (id, counter) in pending.lock().map_err(|_| CoreError::Fenced)?.iter() {
        if let Some(def) = parts.ext.sequences.get(id)
            && def.incarnation == counter.incarnation
        {
            txn.put_counter(
                engine::catalog_codec::counter_key(*id, def.incarnation),
                engine::catalog_codec::encode_counter(counter, def)?,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "v2_sequence_reservation_tests.rs"]
mod reservation_tests;
