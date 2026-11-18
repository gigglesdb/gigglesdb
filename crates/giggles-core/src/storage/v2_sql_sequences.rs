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
        if committed {
            let slot = {
                let mut live = db.sql_counters.lock_cancellable()?;
                live.entry(id).or_default().clone()
            };
            let result = committed_value(&db, &slot, id, def, operation, value, called)?;
            if operation == "nextval" || called {
                values
                    .lock()
                    .map_err(|_| CoreError::Fenced)?
                    .insert((id, def.incarnation), result);
            }
            return Ok(relational::Value::Int(result));
        }
        let mut pending = if committed {
            None
        } else {
            Some(pending.lock().map_err(|_| CoreError::Fenced)?)
        };
        let prior = pending
            .as_ref()
            .expect("private sequence")
            .get(&id)
            .cloned();
        let (result, counter) = match operation {
            "nextval" => sequences::next_on(def, prior),
            "setval" => sequences::set_on(
                def,
                prior,
                value.ok_or(CoreError::Sql(SqlError::Parameters))?,
                called,
            ),
            _ => Err(SqlError::Unsupported),
        }
        .map_err(CoreError::Sql)?;
        pending
            .as_mut()
            .expect("private sequence")
            .insert(id, counter);
        if operation == "nextval" || called {
            values
                .lock()
                .map_err(|_| CoreError::Fenced)?
                .insert((id, def.incarnation), result);
        }
        Ok(relational::Value::Int(result))
    })
}

type SlotGuard<'a> = crate::cancellation::WaitGuard<'a, v2::SeqSlotState>;

#[derive(Debug)]
enum ReceiveError {
    Core(CoreError),
    Gone(u128),
}
impl From<CoreError> for ReceiveError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}
impl From<ReceiveError> for CoreError {
    fn from(error: ReceiveError) -> Self {
        match error {
            ReceiveError::Core(error) => error,
            ReceiveError::Gone(_) => engine::txn::TxnError::SequenceGone.into(),
        }
    }
}

/// One receiver owns the oldest ack; all other callers release the slot while waiting.
fn receive_one<'a>(
    slot: &'a v2::SeqSlot,
    mut guard: SlotGuard<'a>,
) -> Result<SlotGuard<'a>, ReceiveError> {
    if guard.receiving {
        return guard.wait().map_err(ReceiveError::Core);
    }
    let Some(write) = guard.inflight.pop_front() else {
        return Ok(guard);
    };
    guard.receiving = true;
    drop(guard);
    let outcome = loop {
        if let Err(error) = crate::cancellation::check() {
            // Cancellation must not lose the oldest ack or leave receiving set.
            let mut guard = slot.counter.lock().map_err(|_| CoreError::Fenced)?;
            guard.inflight.push_front(write);
            guard.receiving = false;
            return Err(error.into());
        }
        match write
            .ack
            .0
            .recv_timeout(std::time::Duration::from_millis(5))
        {
            Ok(result) => break result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                break Err(engine::txn::TxnError::Fenced);
            }
        }
    };
    let mut guard = slot.counter.lock().map_err(|_| CoreError::Fenced)?;
    guard.receiving = false;
    if outcome.is_ok() {
        guard.acknowledge(write.incarnation, write.hw);
    }
    match outcome {
        Ok(()) => Ok(guard),
        Err(engine::txn::TxnError::SequenceGone) => Err(ReceiveError::Gone(write.incarnation)),
        Err(error) => Err(ReceiveError::Core(error.into())),
    }
}

fn drain_ready(guard: &mut v2::SeqSlotState) {
    if guard.receiving {
        return;
    }
    while let Some(write) = guard.inflight.front() {
        match write.ack.0.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Empty) => break,
            result => {
                let write = guard.inflight.pop_front().unwrap();
                if matches!(result, Ok(Ok(()))) {
                    guard.acknowledge(write.incarnation, write.hw);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn committed_value(
    db: &v2::V2Db,
    slot: &v2::SeqSlot,
    id: u64,
    def: &relational::catalog_ext::SequenceDef,
    operation: &str,
    value: Option<i64>,
    called: bool,
) -> Result<i64, CoreError> {
    let key = engine::catalog_codec::counter_key(id, def.incarnation);
    let mut guard = slot.counter.lock_cancellable()?;
    // Re-evaluate after waiting: another allocator may have advanced or wrapped.
    let (mut counter, result, synchronous, prior_hw) = loop {
        drain_ready(&mut guard);
        let in_memory = guard
            .counter
            .as_ref()
            .is_some_and(|c| c.incarnation == def.incarnation);
        if !in_memory {
            if !db
                .catalog()
                .current_local_view()?
                .sequences
                .get(&id)
                .is_some_and(|current| current.incarnation == def.incarnation)
            {
                return Err(CoreError::Sql(SqlError::UnknownTable));
            }
            if guard.receiving || !guard.inflight.is_empty() {
                guard = receive_one(slot, guard)?;
                continue;
            }
            if let Some(bytes) = db.kv.get(Keyspace::Seq, &key)? {
                guard.install(engine::catalog_codec::decode_counter(&key, &bytes, def)?);
            }
        }
        let prior = guard
            .counter
            .as_ref()
            .filter(|c| c.incarnation == def.incarnation)
            .cloned();
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
        let wrap = prior.as_ref().is_some_and(|old| {
            def.cycle
                && if def.increment > 0 {
                    counter.last_value < old.last_value
                } else {
                    counter.last_value > old.last_value
                }
        });
        let synchronous = operation == "setval"
            || !in_memory
            || wrap
            || prior.as_ref().is_none_or(|c| !c.is_called);
        if synchronous
            && (guard.receiving || !guard.inflight.is_empty() || guard.waiting_returns != 0)
        {
            guard = if guard.receiving || !guard.inflight.is_empty() {
                receive_one(slot, guard)?
            } else {
                guard.wait()?
            };
            continue;
        }
        break (counter, result, synchronous, prior.map(|c| c.log_hw));
    };
    if synchronous {
        db.sequencer.write_counter(
            key,
            engine::catalog_codec::encode_counter(&counter, def)?,
            256,
        )?;
        guard.install(counter);
        return Ok(result);
    }
    let remaining = (i128::from(counter.log_hw) - i128::from(result)) / i128::from(def.increment);
    if remaining <= 24 {
        let mut high = result;
        for _ in 0..sequences::SEQ_LOG_VALS {
            match high
                .checked_add(def.increment)
                .filter(|v| *v >= def.min && *v <= def.max)
            {
                Some(value) => high = value,
                None => break,
            }
        }
        counter.log_hw = high;
    }
    if prior_hw != Some(counter.log_hw) {
        let ack = db.sequencer.send_counter(
            key.clone(),
            engine::catalog_codec::encode_counter(&counter, def)?,
            256,
        )?;
        guard.inflight.push_back(v2::InFlight {
            incarnation: def.incarnation,
            hw: counter.log_hw,
            ack,
        });
    }
    guard.counter = Some(counter);
    if guard.durable_hw.is_some_and(|hw| {
        if def.increment > 0 {
            hw >= result
        } else {
            hw <= result
        }
    }) {
        return Ok(result);
    }
    guard.waiting_returns += 1;
    let outcome = await_coverage(db, slot, guard, id, &key, def, result);
    // Even an error or cancellation releases the backwards-move barrier.
    let mut guard = slot.counter.lock().map_err(|_| CoreError::Fenced)?;
    guard.waiting_returns -= 1;
    outcome
}

#[allow(clippy::too_many_arguments)]
fn await_coverage<'a>(
    db: &v2::V2Db,
    slot: &'a v2::SeqSlot,
    mut guard: SlotGuard<'a>,
    id: u64,
    key: &[u8],
    def: &relational::catalog_ext::SequenceDef,
    result: i64,
) -> Result<i64, CoreError> {
    loop {
        drain_ready(&mut guard);
        if !guard
            .counter
            .as_ref()
            .is_some_and(|c| c.incarnation == def.incarnation)
        {
            if !db
                .catalog()
                .current_local_view()?
                .sequences
                .get(&id)
                .is_some_and(|current| current.incarnation == def.incarnation)
            {
                // A durable RESTART/DROP retired this incarnation before install.
                // Its allocated value can never be reissued by this incarnation.
                return Ok(result);
            }
            // With installs restricted to the committed-current incarnation, this cannot happen.
            debug_assert!(false, "current sequence incarnation missing from slot");
            return Err(CoreError::Fenced);
        }
        if guard.durable_hw.is_some_and(|hw| {
            if def.increment > 0 {
                hw >= result
            } else {
                hw <= result
            }
        }) {
            return Ok(result);
        }
        if !guard.receiving && guard.inflight.is_empty() {
            let counter = guard.counter.as_ref().ok_or(CoreError::Fenced)?;
            let hw = counter.log_hw;
            let ack = match db.sequencer.send_counter(
                key.to_vec(),
                engine::catalog_codec::encode_counter(counter, def)?,
                256,
            ) {
                Ok(ack) => ack,
                Err(engine::txn::TxnError::SequenceGone) => {
                    drop(guard);
                    return await_retirement(db, id, def.incarnation, result);
                }
                Err(error) => return Err(error.into()),
            };
            guard.inflight.push_back(v2::InFlight {
                incarnation: def.incarnation,
                hw,
                ack,
            });
        }
        guard = match receive_one(slot, guard) {
            Ok(guard) => guard,
            Err(ReceiveError::Gone(incarnation)) if incarnation == def.incarnation => {
                return await_retirement(db, id, def.incarnation, result);
            }
            Err(error) => return Err(error.into()),
        };
    }
}

// SequenceGone can precede the DDL's Sync and catalog publication. Release
// the slot so publication can retire it, and remain cancellable until then.
fn await_retirement(
    db: &v2::V2Db,
    id: u64,
    incarnation: u128,
    result: i64,
) -> Result<i64, CoreError> {
    loop {
        crate::cancellation::check()?;
        if !db.catalog().current_local_view()?.sequences.get(&id)
            .is_some_and(|current| current.incarnation == incarnation)
        {
            return Ok(result);
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
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
