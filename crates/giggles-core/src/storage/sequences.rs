//! Live sequence allocation and nontransactional durable reservations.
use super::*;
use relational::SqlError as E;
use relational::catalog_ext::SequenceDef;
type R<T> = Result<T, relational::SqlError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeqCounter {
    pub(crate) incarnation: u128,
    pub(crate) last_value: i64,
    pub(crate) is_called: bool,
    pub(crate) log_hw: i64,
    pub(crate) stamp: u64,
}
#[derive(Default)]
pub(super) struct Live {
    counters: BTreeMap<u64, SeqCounter>,
    stamp: u64,
}
pub(super) type SessionValues = Arc<Mutex<BTreeMap<(u64, u128), i64>>>;
pub(super) type Pending = Arc<Mutex<BTreeMap<u64, SeqCounter>>>;

fn definition(s: &State, id: u64) -> R<SequenceDef> {
    s.sql
        .catalog_ext()
        .sequences
        .get(&id)
        .cloned()
        .ok_or(E::UnknownTable)
}
fn counter<'a>(s: &'a mut State, id: u64, def: &SequenceDef) -> &'a mut SeqCounter {
    let entry = s.seq.entry(id).or_insert_with(|| fresh(def));
    if entry.incarnation != def.incarnation {
        *entry = fresh(def);
    }
    entry
}
fn step(value: i64, def: &SequenceDef) -> R<i64> {
    match value
        .checked_add(def.increment)
        .filter(|v| *v >= def.min && *v <= def.max)
    {
        Some(value) => Ok(value),
        None if def.cycle => Ok(if def.increment > 0 { def.min } else { def.max }),
        None => Err(E::SequenceLimit),
    }
}
pub(crate) fn next(s: &mut State, id: u64) -> R<i64> {
    // CACHE is stored for PostgreSQL catalog/dump compatibility. Allocation is
    // deliberately one value at a time; the durable recovery reservation below
    // is independent of per-session CACHE preallocation.
    let def = definition(s, id)?;
    let stamp = s
        .seq
        .values()
        .map(|v| v.stamp)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(E::Overflow)?;
    advance(counter(s, id, &def), &def, stamp)
}
/// Evaluate against the single counter held by a v2 SQL callback.
fn single_counter(def: &SequenceDef, prior: Option<SeqCounter>) -> R<(SeqCounter, u64)> {
    // Match State's stamp calculation before replacing a stale incarnation.
    let stamp = prior
        .as_ref()
        .map_or(0, |c| c.stamp)
        .checked_add(1)
        .ok_or(E::Overflow)?;
    let counter = prior
        .filter(|c| c.incarnation == def.incarnation)
        .unwrap_or_else(|| fresh(def));
    Ok((counter, stamp))
}
pub(super) fn next_on(def: &SequenceDef, prior: Option<SeqCounter>) -> R<(i64, SeqCounter)> {
    let (mut counter, stamp) = single_counter(def, prior)?;
    let value = advance(&mut counter, def, stamp)?;
    Ok((value, counter))
}
pub(super) fn set_on(
    def: &SequenceDef,
    prior: Option<SeqCounter>,
    value: i64,
    called: bool,
) -> R<(i64, SeqCounter)> {
    // Bounds precede stamp overflow and incarnation replacement, as in setval.
    if value < def.min || value > def.max {
        return Err(E::Overflow);
    }
    let (mut counter, stamp) = single_counter(def, prior)?;
    set(&mut counter, value, called, stamp);
    Ok((value, counter))
}
/// A counter that has handed out nothing yet.
pub(crate) fn fresh(def: &SequenceDef) -> SeqCounter {
    SeqCounter {
        incarnation: def.incarnation,
        last_value: def.start,
        is_called: false,
        log_hw: def.start,
        stamp: 0,
    }
}
/// nextval on one counter: the next value, with the recovery reservation `log_hw`
/// moved ahead when the value leaves it.
pub(crate) fn advance(c: &mut SeqCounter, def: &SequenceDef, stamp: u64) -> R<i64> {
    let was_called = c.is_called;
    let value = if was_called {
        step(c.last_value, def)?
    } else {
        c.last_value
    };
    c.last_value = value;
    c.is_called = true;
    c.stamp = stamp;
    // Reserve a bounded run. A cycle starts a fresh reservation.
    let outside = if def.increment > 0 {
        value > c.log_hw
    } else {
        value < c.log_hw
    };
    if !was_called
        || outside
        || value == def.start
        || (def.cycle && value == if def.increment > 0 { def.min } else { def.max })
    {
        let mut high = value;
        for _ in 0..32 {
            match high
                .checked_add(def.increment)
                .filter(|v| *v >= def.min && *v <= def.max)
            {
                Some(v) => high = v,
                None => break,
            }
        }
        c.log_hw = high;
    }
    Ok(value)
}
pub(crate) fn setval(s: &mut State, id: u64, value: i64, called: bool) -> R<i64> {
    let def = definition(s, id)?;
    if value < def.min || value > def.max {
        return Err(E::Overflow);
    }
    let stamp = s
        .seq
        .values()
        .map(|v| v.stamp)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(E::Overflow)?;
    set(counter(s, id, &def), value, called, stamp);
    Ok(value)
}
/// setval on one counter whose bounds the caller checked.
pub(crate) fn set(c: &mut SeqCounter, value: i64, called: bool, stamp: u64) {
    c.last_value = value;
    c.is_called = called;
    c.log_hw = value;
    c.stamp = stamp;
}
pub(crate) fn currval(values: &BTreeMap<(u64, u128), i64>, id: u64, incarnation: u128) -> R<i64> {
    values
        .get(&(id, incarnation))
        .copied()
        .ok_or(E::ObjectNotInPrerequisiteState)
}
pub(super) fn merge(s: &mut State, source: &BTreeMap<u64, SeqCounter>) {
    for (&id, value) in source {
        if s.sql
            .catalog_ext()
            .sequences
            .get(&id)
            .is_some_and(|d| d.incarnation == value.incarnation)
            && s.seq
                .get(&id)
                .is_none_or(|old| old.incarnation != value.incarnation || old.stamp < value.stamp)
        {
            s.seq.insert(id, value.clone());
        }
    }
    s.seq.retain(|id, c| {
        s.sql
            .catalog_ext()
            .sequences
            .get(id)
            .is_some_and(|d| d.incarnation == c.incarnation)
    });
}

pub(crate) fn sequence_name(text: &str) -> R<String> {
    let mut parts = Vec::new();
    let mut chars = text.trim().chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next() {
                    Some('"') if chars.peek() == Some(&'"') => {
                        chars.next();
                        part.push('"');
                    }
                    Some('"') => break,
                    Some(c) => part.push(c),
                    None => return Err(E::InvalidTextRepresentation),
                }
            }
        } else {
            while chars
                .peek()
                .is_some_and(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
            {
                part.extend(chars.next().unwrap().to_lowercase());
            }
        }
        if part.is_empty() {
            return Err(E::InvalidTextRepresentation);
        }
        parts.push(part);
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.next() {
            None => break,
            Some('.') => {}
            _ => return Err(E::InvalidTextRepresentation),
        }
    }
    match parts.as_slice() {
        [name] => Ok(name.clone()),
        [schema, name] => Ok(format!("{schema}\0{name}")),
        _ => Err(E::UnknownTable),
    }
}

impl Inner {
    pub(super) fn merge_live_sequences(&self, s: &mut State) -> Result<(), CoreError> {
        let live = self.sequences.lock().map_err(|_| CoreError::Fenced)?;
        merge(s, &live.counters);
        // Equal stamps identify the same operation. The live copy contains the
        // actual position; disk contains its reserved recovery high-water mark.
        for (&id, c) in &live.counters {
            if s.seq
                .get(&id)
                .is_some_and(|old| old.incarnation == c.incarnation && old.stamp == c.stamp)
            {
                s.seq.insert(id, c.clone());
            }
        }
        Ok(())
    }
    pub(super) fn accept_sequences(&self, s: &State) -> Result<(), CoreError> {
        let mut live = self.sequences.lock().map_err(|_| CoreError::Fenced)?;
        for (&id, c) in &s.seq {
            if live
                .counters
                .get(&id)
                .is_none_or(|old| old.incarnation != c.incarnation || old.stamp < c.stamp)
            {
                live.counters.insert(id, c.clone());
            }
            live.stamp = live.stamp.max(c.stamp);
        }
        live.counters.retain(|id, c| {
            s.sql
                .catalog_ext()
                .sequences
                .get(id)
                .is_some_and(|d| d.incarnation == c.incarnation)
        });
        Ok(())
    }
    pub(super) fn sequence_callback(
        self: &Arc<Self>,
        snapshot: State,
        baseline: BTreeMap<u64, u128>,
        pending: Pending,
        values: SessionValues,
        allowed_tables: Option<std::collections::BTreeSet<String>>,
        search_path: Vec<String>,
    ) -> relational::clock::SequenceCallback {
        let inner = self.clone();
        Arc::new(move |operation, text, value, called| {
            if allowed_tables
                .as_ref()
                .is_some_and(|tables| tables.is_empty())
            {
                return Err(CoreError::Policy(Error::Scope));
            }
            let name = sequence_name(text).map_err(CoreError::Sql)?;
            let name = snapshot.sql.resolve_relation_key(&name, &search_path);
            let (&id, def) = snapshot
                .sql
                .catalog_ext()
                .sequences
                .iter()
                .find(|(_, d)| d.name == name)
                .ok_or(CoreError::Sql(E::UnknownTable))?;
            if let Some(allowed) = &allowed_tables {
                let owner = def
                    .owned_by
                    .and_then(|(id, _)| snapshot.sql.tables.values().find(|t| t.id == id));
                if !allowed.contains(&name)
                    && owner.is_none_or(|t| !allowed.contains(&snapshot.sql.table_key(t)))
                {
                    return Err(CoreError::Policy(Error::Scope));
                }
            }
            let _guard = inner.gate.lock_cancellable()?;
            if inner.fenced.load(Ordering::Acquire) {
                return Err(CoreError::Fenced);
            }
            let mut current = inner.read()?;
            let committed = current
                .sql
                .catalog_ext()
                .sequences
                .get(&id)
                .is_some_and(|d| d.incarnation == def.incarnation);
            if !committed && baseline.get(&id) == Some(&def.incarnation) {
                return Err(CoreError::Sql(E::UnknownTable));
            }
            let mut pending = pending.lock().map_err(|_| CoreError::Fenced)?;
            if !committed {
                current = snapshot.clone();
                merge(&mut current, &pending);
            }
            if operation == "currval" {
                let values = values.lock().map_err(|_| CoreError::Fenced)?;
                return currval(&values, id, def.incarnation)
                    .map(relational::Value::Int)
                    .map_err(CoreError::Sql);
            }
            let mut live = inner.sequences.lock().map_err(|_| CoreError::Fenced)?;
            let stamp = live
                .stamp
                .max(current.seq.values().map(|c| c.stamp).max().unwrap_or(0))
                .checked_add(1)
                .ok_or(CoreError::Sql(E::Overflow))?;
            let previous = current.seq.get(&id).cloned();
            let result = match operation {
                "nextval" => next(&mut current, id),
                "setval" => setval(
                    &mut current,
                    id,
                    value.ok_or(CoreError::Sql(E::Parameters))?,
                    called,
                ),
                _ => Err(E::Unsupported),
            }
            .map_err(CoreError::Sql)?;
            let c = current
                .seq
                .get_mut(&id)
                .expect("successful sequence operation");
            c.stamp = stamp;
            let counter = c.clone();
            let reservation_changed = previous.as_ref().is_none_or(|old| {
                !old.is_called
                    || old.log_hw != counter.log_hw
                    || (def.cycle
                        && if def.increment > 0 {
                            counter.last_value < old.last_value
                        } else {
                            counter.last_value > old.last_value
                        })
            });
            live.stamp = stamp;
            drop(live);
            if committed {
                if operation == "setval" || reservation_changed {
                    inner.persist_counters(&mut current)?;
                } else {
                    inner.accept_sequences(&current)?;
                }
            } else {
                // A transactional CREATE owns no durable sequence until COMMIT.
                pending.insert(id, counter);
            }
            if operation == "nextval" || called {
                let mut values = values.lock().map_err(|_| CoreError::Fenced)?;
                // ALTER creates transactional physical storage, but currval is
                // session state and follows the logical sequence across rollback.
                // Keys remain incarnation-aware so rolled-back CREATE id reuse
                // cannot expose a previous object's currval.
                for ((sequence_id, _), value) in values.iter_mut() {
                    if *sequence_id == id {
                        *value = result;
                    }
                }
                values.insert((id, def.incarnation), result);
            }
            Ok(relational::Value::Int(result))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn def(increment: i64, cycle: bool) -> SequenceDef {
        SequenceDef {
            name: "s".into(),
            data_type: relational::DataType::BigInt,
            start: 2,
            increment,
            min: -2,
            max: 2,
            cache: 1,
            cycle,
            owned_by: None,
            identity: false,
            incarnation: 1,
        }
    }
    #[test]
    fn bounds_descending_and_cycle() {
        assert_eq!(step(2, &def(1, false)), Err(E::SequenceLimit));
        assert_eq!(step(-2, &def(-1, false)), Err(E::SequenceLimit));
        assert_eq!(step(2, &def(1, true)), Ok(-2));
        assert_eq!(step(-2, &def(-1, true)), Ok(2));
        assert_eq!(step(i64::MAX, &def(i64::MAX, false)), Err(E::SequenceLimit));
    }
    #[test]
    fn single_counter_matches_state_operations() {
        for increment in [1, -1] {
            let mut state = empty(17);
            let ddl = format!(
                "CREATE SEQUENCE s INCREMENT {increment} START 2 MINVALUE -2 MAXVALUE 2 CYCLE"
            );
            let prepared = relational::Prepared::parse(&ddl, 17, 0).unwrap();
            state.sql.execute(&prepared, &[], None).unwrap();
            let (&id, def) = state.sql.catalog_ext().sequences.iter().next().unwrap();
            let def = def.clone();
            let mut prior = None;
            for operation in [None, None, Some((0, false)), None, None,
                Some((2, true)), None, None, None, None, None, None] {
                let actual = match operation {
                    None => next_on(&def, prior.take()),
                    Some((value, called)) => set_on(&def, prior.take(), value, called),
                }.unwrap();
                let expected = match operation {
                    None => next(&mut state, id),
                    Some((value, called)) => setval(&mut state, id, value, called),
                }.unwrap();
                assert_eq!(actual.0, expected);
                // Eq covers incarnation, last_value, is_called, log_hw and stamp.
                assert_eq!(&actual.1, &state.seq[&id]);
                prior = Some(actual.1);
            }
            // A stale incarnation still contributes its stamp before reset.
            let mut stale = prior.unwrap();
            stale.incarnation = def.incarnation.wrapping_add(1);
            state.seq.insert(id, stale.clone());
            let actual = next_on(&def, Some(stale)).unwrap();
            assert_eq!(actual.0, next(&mut state, id).unwrap());
            assert_eq!(actual.1, state.seq[&id]);
            let mut exhausted = actual.1;
            exhausted.stamp = u64::MAX;
            state.seq.insert(id, exhausted.clone());
            assert_eq!(next_on(&def, Some(exhausted.clone())), Err(E::Overflow));
            assert_eq!(next(&mut state, id), Err(E::Overflow));
            for value in [0, 3] {
                assert_eq!(set_on(&def, Some(exhausted.clone()), value, false), Err(E::Overflow));
                assert_eq!(setval(&mut state, id, value, false), Err(E::Overflow));
            }
        }
    }
    #[test]
    fn reference_names_preserve_quoted_case() {
        assert_eq!(sequence_name(" public . FOO "), Ok("public\0foo".into()));
        assert_eq!(
            sequence_name("\"public\".\"F\"\"oo\""),
            Ok("public\0F\"oo".into())
        );
        assert_eq!(sequence_name("other.s"), Ok("other\0s".into()));
        assert_eq!(sequence_name("public."), Err(E::InvalidTextRepresentation));
    }
}
