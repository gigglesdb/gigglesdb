//! SQL defaults use the existing v2 counter codec and durable reservations.
use super::*;

pub(super) fn callback(
    db: Arc<v2::V2Db>,
    image: Arc<relational::Relational>,
    baseline: BTreeMap<u64, u128>,
    pending: sequences::Pending,
    values: sequences::SessionValues,
    search_path: Vec<String>,
) -> relational::clock::SequenceCallback {
    Arc::new(move |operation, name, value, called| {
        crate::cancellation::check()?;
        let name = sequences::sequence_name(name).map_err(CoreError::Sql)?;
        let name = image.resolve_relation_key(&name, &search_path);
        let (&id, def) = image
            .catalog_ext()
            .sequences
            .iter()
            .find(|(_, def)| def.name == name)
            .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
        let committed = if db.cluster_services().is_some() {
            // Gateways retain the routed fresh read; their cache can lag remote DDL.
            let mut check = db.begin(Isolation::ReadCommitted);
            check
                .get(Keyspace::Catalog, &engine::catalog_codec::sequence_key(id))?
                .map(|bytes| engine::catalog_codec::decode_sequence(&bytes))
                .transpose()?
                .is_some_and(|current| current.incarnation == def.incarnation)
        } else {
            db.catalog()
                .current_local_view()?
                .sequences
                .get(&id)
                .is_some_and(|current| current.incarnation == def.incarnation)
        };
        if !committed && baseline.get(&id) == Some(&def.incarnation) {
            return Err(CoreError::Sql(SqlError::UnknownTable));
        }
        if operation == "currval" {
            let values = values.lock().map_err(|_| CoreError::Fenced)?;
            return sequences::currval(&values, id, def.incarnation)
                .map(relational::Value::Int)
                .map_err(CoreError::Sql);
        }
        if committed && let Some(services) = db.cluster_services() {
            // A cluster gateway keeps no counter: the system range leader evaluates every
            // nextval/setval of a committed sequence (GD-0075 R5-43, design.md Changes).
            use crate::range_api::{SequenceOp, SequenceReply};
            let op = match operation {
                "nextval" => SequenceOp::Next,
                "setval" => SequenceOp::Set {
                    value: value.ok_or(CoreError::Sql(SqlError::Parameters))?,
                    called,
                },
                _ => return Err(CoreError::Sql(SqlError::Unsupported)),
            };
            let reply = services
                .client()
                .sequence(id, def.incarnation, op)
                .map_err(|e| {
                    CoreError::from(engine::txn::TxnError::Kv(match e {
                        crate::range_api::RangeError::Corrupt(m) => engine::kv::KvError::Corrupt(m),
                        other => engine::kv::KvError::Io(other.to_string()),
                    }))
                })?;
            let result = match reply {
                SequenceReply::Value(result) => result,
                SequenceReply::Gone => return Err(CoreError::Sql(SqlError::UnknownTable)),
                SequenceReply::Limit => return Err(CoreError::Sql(SqlError::SequenceLimit)),
                SequenceReply::OutOfRange => return Err(CoreError::Sql(SqlError::Overflow)),
            };
            if operation == "nextval" || called {
                values
                    .lock()
                    .map_err(|_| CoreError::Fenced)?
                    .insert((id, def.incarnation), result);
            }
            return Ok(relational::Value::Int(result));
        }
        // Release the directory before waiting on an individual sequence or I/O.
        let slot = if committed {
            let mut live = crate::cancellation::lock(&db.sql_counters)?;
            Some(live.entry(id).or_default().clone())
        } else {
            None
        };
        let mut live = slot
            .as_ref()
            .map(|slot| crate::cancellation::lock(&slot.counter))
            .transpose()?;
        let mut pending = if committed {
            None
        } else {
            Some(pending.lock().map_err(|_| CoreError::Fenced)?)
        };
        let key = engine::catalog_codec::counter_key(id, def.incarnation);
        let prior = if committed {
            match live
                .as_ref()
                .expect("committed slot")
                .as_ref()
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
            pending
                .as_ref()
                .expect("private sequence")
                .get(&id)
                .cloned()
        };
        let (result, counter) = match operation {
            "nextval" => sequences::next_on(def, prior.clone()),
            "setval" => sequences::set_on(
                def,
                prior.clone(),
                value.ok_or(CoreError::Sql(SqlError::Parameters))?,
                called,
            ),
            _ => Err(SqlError::Unsupported),
        }
        .map_err(CoreError::Sql)?;
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
            **live.as_mut().expect("committed slot") = Some(counter);
        } else {
            pending
                .as_mut()
                .expect("private sequence")
                .insert(id, counter);
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

pub(in crate::storage) fn stage_pending(
    db: &v2::V2Db,
    txn: &mut Transaction,
    pending: &sequences::Pending,
) -> Result<(), CoreError> {
    let parts = db.catalog().parts(txn)?;
    let mut pending = pending.lock().map_err(|_| CoreError::Fenced)?;
    // A savepoint rollback can discard a RESTART's private definition. Exclude
    // its counter from both durable staging and the caller's live publication.
    pending.retain(|id, counter| {
        parts
            .ext
            .sequences
            .get(id)
            .is_some_and(|def| def.incarnation == counter.incarnation)
    });
    for (id, counter) in pending.iter() {
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

#[cfg(test)]
#[path = "v2_sequence_restart_tests.rs"]
mod restart_tests;
