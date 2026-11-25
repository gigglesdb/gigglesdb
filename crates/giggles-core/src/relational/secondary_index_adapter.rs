//! Trusted bridge from the authoritative relational catalog and complete row maps.
//! Ordinary serialization refuses indexes; explicit persistence returns paired
//! namespace5 base bytes and namespace8 payload for the storage owner's gates.
use super::secondary_index::{self as si, BaseRow, Catalog, Descriptor, Kind, Sidecar, Tuple};
use super::*;
use serde::ser::SerializeStruct;

fn error(e: si::Error) -> SqlError {
    match e {
        si::Error::Limit => SqlError::Limit,
        si::Error::Cancelled => SqlError::Aborted,
        si::Error::Duplicate => SqlError::Duplicate,
        si::Error::ImmutablePk => SqlError::Unsupported,
        _ => SqlError::Type,
    }
}
fn tick(cancel: &mut impl FnMut() -> bool) -> R<()> {
    if cancel() {
        Err(SqlError::Aborted)
    } else {
        Ok(())
    }
}
fn cap(n: usize, max: usize) -> R<()> {
    if n > max {
        Err(SqlError::Limit)
    } else {
        Ok(())
    }
}
fn add(a: usize, b: usize) -> R<usize> {
    a.checked_add(b).ok_or(SqlError::Limit)
}
fn mul(a: usize, b: usize) -> R<usize> {
    a.checked_mul(b).ok_or(SqlError::Limit)
}
struct CatalogView<'a>(BTreeMap<u64, &'a TableDef>);
impl<'a> CatalogView<'a> {
    fn new(state: &'a Relational) -> Self {
        Self(state.tables.values().map(|table| (table.id, table)).collect())
    }
}
impl Catalog for CatalogView<'_> {
    fn table_exists(&self, table: u64) -> bool {
        self.0.contains_key(&table)
    }
    fn column_kind(&self, table: u64, column: u64) -> Option<Kind> {
        self.0.get(&table)?
            .columns
            .iter()
            .find(|c| c.id == column)
            .and_then(|c| Kind::for_type(&c.data_type))
    }
}
fn table(state: &Relational, id: u64) -> R<&TableDef> {
    state
        .tables
        .values()
        .find(|t| t.id == id)
        .ok_or(SqlError::UnknownTable)
}
fn column(t: &TableDef, id: u64) -> R<usize> {
    t.columns
        .iter()
        .position(|c| c.id == id)
        .ok_or(SqlError::UnknownColumn)
}

/// Exact legacy encoded size, without cloning or encoding a borrowed scalar.
pub(super) fn scalar_len(v: &Value) -> R<usize> {
    Ok(match v {
        Value::Null => 1,
        Value::Bool(_) => 2,
        Value::Int(_) => 9,
        Value::Text(s) => add(
            add(s.len(), s.as_bytes().iter().filter(|b| **b == 0).count())?,
            3,
        )?,
        Value::Bytes(s) => add(add(s.len(), s.iter().filter(|b| **b == 0).count())?, 3)?,
        _ => secondary_index_keys::temporal_len(v).ok_or(SqlError::Unsupported)?,
    })
}
pub(super) fn tuple_len<'a>(values: impl Iterator<Item = &'a Value>) -> R<usize> {
    let mut raw = 0;
    let mut encoded = 0;
    let mut arity = 0;
    for v in values {
        // Read lengths before scanning variable bytes. Numeric remains outside
        // this path because its display conversion would allocate.
        let n = match v {
            Value::Text(s) => s.len(),
            Value::Bytes(b) => b.len(),
            Value::Null | Value::Bool(_) | Value::Int(_) => 9,
            _ if v.is_temporal() => value_bytes(v),
            _ => return Err(SqlError::Unsupported),
        };
        raw = add(raw, n)?;
        cap(raw, crate::relational::max_value_bytes())?;
        arity += 1;
        cap(arity, MAX_COLUMNS)?;
        encoded = add(encoded, scalar_len(v)?)?;
        cap(encoded, si::MAX_COMPONENT)?;
    }
    Ok(encoded)
}
pub(super) fn encode_tuple<'a>(values: impl Iterator<Item = &'a Value>) -> R<Vec<u8>> {
    // Whole-tuple admission precedes this call.
    let mut out = Vec::new();
    for v in values {
        out.extend(secondary_index_keys::component(v)?);
    }
    Ok(out)
}

#[derive(Clone, Copy)]
struct Witness<'a> {
    table: u64,
    key: &'a [u8],
    row: &'a SqlRow,
}
#[derive(Clone, Copy)]
struct Overlay<'a> {
    old: Option<Witness<'a>>,
    new: Option<Witness<'a>>,
}
struct EncodedRow {
    table: u64,
    pk: Vec<u8>,
    tuples: Vec<(u64, Vec<u8>, bool)>,
}
struct Snapshot(Vec<EncodedRow>);
impl Snapshot {
    fn tuples(&self) -> Vec<Vec<Tuple<'_>>> {
        self.0
            .iter()
            .map(|r| {
                r.tuples
                    .iter()
                    .map(|(index, bytes, has_null)| Tuple {
                        index: *index,
                        bytes,
                        has_null: *has_null,
                    })
                    .collect()
            })
            .collect()
    }
    fn rows<'a>(&'a self, tuples: &'a [Vec<Tuple<'a>>]) -> Vec<BaseRow<'a>> {
        self.0
            .iter()
            .zip(tuples)
            .map(|(r, tuples)| BaseRow {
                table: r.table,
                pk: &r.pk,
                tuples,
            })
            .collect()
    }
}

/// Length-only shape admission before witness/key equality, catalog lookup,
/// cancellation callbacks or cloning. Rows may contain unsupported NON-key
/// scalars, which are checked normally by row_check after this bound.
fn shape(w: Witness<'_>) -> R<()> {
    cap(w.key.len(), si::MAX_COMPONENT)?;
    cap(w.row.values.len(), MAX_COLUMNS)?;
    let mut bytes = 0;
    for v in &w.row.values {
        let n = match v {
            Value::Text(s) => s.len(),
            Value::Bytes(b) => b.len(),
            // Typed non-key payloads are not copied by the snapshot bridge.
            _ => 9,
        };
        bytes = add(bytes, n)?;
        cap(bytes, crate::relational::max_value_bytes())?;
    }
    Ok(())
}
fn descriptors(state: &Relational, ds: &[Descriptor], cancel: &mut impl FnMut() -> bool) -> R<()> {
    // Before any name equality, admit ALL descriptor shapes.
    for d in ds {
        cap(d.name.len(), si::MAX_NAME)?;
        cap(d.columns.len(), si::MAX_ARITY)?;
    }
    let mut object_ids = BTreeSet::new();
    let catalog = CatalogView::new(state);
    for t in state.tables.values() {
        cap(t.columns.len(), MAX_COLUMNS)?;
        cap(t.primary_key.len(), MAX_COLUMNS)?;
        object_ids.insert(t.id);
        object_ids.extend(t.columns.iter().map(|c| c.id));
    }
    let mut names = BTreeSet::new();
    for d in ds {
        tick(cancel)?;
        if d.id == 0 || d.name.is_empty() || d.columns.is_empty() || d.key_encoding != 1 {
            return Err(SqlError::Type);
        }
        // Index IDs share the stable catalog allocator with tables/columns.
        if !object_ids.insert(d.id) || !names.insert(&d.name) {
            return Err(SqlError::Duplicate);
        }
        let t = catalog.0.get(&d.table).copied().ok_or(SqlError::UnknownTable)?;
        schema_binding::validate_key(&d.name)?;
        if split_key(&d.name).0 != state.table_schema(t.id) {
            return Err(SqlError::Type);
        }
        if t.primary_key.is_empty() {
            return Err(SqlError::Type);
        }
        for p in &t.primary_key {
            let c = t.columns.get(*p).ok_or(SqlError::Type)?;
            if !c.not_null || c.data_type.is_temporal() || Kind::for_type(&c.data_type).is_none() {
                return Err(SqlError::Unsupported);
            }
        }
        for (j, c) in d.columns.iter().enumerate() {
            if *c == 0 || d.columns[..j].contains(c) {
                return Err(SqlError::Type);
            }
            if Kind::for_type(&t.columns[column(t, *c)?].data_type).is_none() {
                return Err(SqlError::Unsupported);
            }
        }
    }
    Ok(())
}
fn row_size(t: &TableDef, key: &[u8], r: &SqlRow, ds: &[Descriptor]) -> R<(usize, usize)> {
    if r.values.len() != t.columns.len() {
        return Err(SqlError::Type);
    }
    // Scalar/null/range proofs do not require formatting Numeric non-key
    // payloads. Only the admitted legacy PK/index components are encoded.
    for (v, c) in r.values.iter().zip(&t.columns) {
        check(v, c)?;
    }
    let pk = tuple_len(t.primary_key.iter().map(|p| &r.values[*p]))?;
    if pk != key.len() {
        return Err(SqlError::Type);
    }
    let mut bytes = 0;
    let mut count = 0;
    for d in ds.iter().filter(|d| d.table == t.id) {
        let tuple = tuple_len(
            d.columns
                .iter()
                .map(|id| &r.values[column(t, *id).expect("admitted column")]),
        )?;
        let combined = add(tuple, pk)?;
        cap(combined, si::MAX_KEY)?;
        let entry = add(combined, pk)?;
        cap(entry, si::MAX_ENTRY)?;
        bytes = add(bytes, entry)?;
        count += 1;
    }
    Ok((count, bytes))
}
fn referenced(ds: &[Descriptor], id: u64) -> bool {
    ds.iter().any(|d| d.table == id)
}

/// Admit the entire borrowed snapshot before any encoding/allocation/equality.
/// Global row/candidate/byte/work quotas are the substrate quotas, not SQL LIMIT.
fn admit(
    state: &Relational,
    ds: &[Descriptor],
    overlay: Overlay<'_>,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    for w in overlay.old.iter().chain(overlay.new.iter()) {
        shape(*w)?;
    }
    descriptors(state, ds, cancel)?;
    let indexed_tables: BTreeSet<_> = ds.iter().map(|descriptor| descriptor.table).collect();
    let mut rows = 0;
    for (id, map) in &state.rows {
        if indexed_tables.contains(id) {
            rows = add(rows, map.len())?;
            cap(rows, si::MAX_ROWS)?;
        }
    }
    // The new row may replace an old row; reserve conservatively only on insert.
    if overlay.old.is_none() && overlay.new.is_some_and(|w| indexed_tables.contains(&w.table)) {
        cap(add(rows, 1)?, si::MAX_ROWS)?;
    }
    for (id, map) in &state.rows {
        if indexed_tables.contains(id) {
            for (key, row) in map {
                shape(Witness {
                    table: *id,
                    key,
                    row,
                })?;
            }
        }
    }
    let mut candidates = 0;
    let mut bytes = 0;
    let mut max_pk = 0;
    for d in ds {
        if !state.rows.contains_key(&d.table) {
            return Err(SqlError::Type);
        }
    }
    for (id, map) in &state.rows {
        if map.is_empty() || !indexed_tables.contains(id) {
            continue;
        }
        let t = table(state, *id)?;
        for (key, row) in map {
            tick(cancel)?;
            let (n, b) = row_size(t, key, row, ds)?;
            candidates = add(candidates, n)?;
            bytes = add(bytes, b)?;
            max_pk = max_pk.max(key.len());
        }
    }
    // Admit both witnesses independently before equality; before and after
    // have separate finite quotas, so a replacement at the cap remains legal.
    let mut old_size = (0, 0);
    let mut new_size = (0, 0);
    for (witness, size) in [(overlay.old, &mut old_size), (overlay.new, &mut new_size)] {
        if let Some(w) = witness {
            tick(cancel)?;
            if indexed_tables.contains(&w.table) {
                *size = row_size(table(state, w.table)?, w.key, w.row, ds)?;
                max_pk = max_pk.max(w.key.len());
            }
        }
    }
    cap(candidates, si::MAX_CANDIDATES)?;
    cap(bytes, si::MAX_BYTES)?;
    // Provisional subtraction is safe only because stage_change requires exact
    // old-row equality before any allocation; malformed witnesses still receive
    // their independent bounds and comparison work admission first.
    let after_candidates = candidates
        .saturating_sub(old_size.0)
        .checked_add(new_size.0)
        .ok_or(SqlError::Limit)?;
    let after_bytes = bytes
        .saturating_sub(old_size.1)
        .checked_add(new_size.1)
        .ok_or(SqlError::Limit)?;
    candidates = candidates.max(after_candidates);
    bytes = bytes.max(after_bytes);
    // Reserve comparison/copy work BEFORE constructing canonical byte vectors.
    // This includes quadratic PK identity searches and witness comparisons.
    let work = add(
        mul(bytes, 64)?,
        mul(mul(add(rows, 2)?, add(rows, 2)?)?, add(max_pk, 1)?)?,
    )?;
    cap(candidates, si::MAX_CANDIDATES)?;
    cap(bytes, si::MAX_BYTES)?;
    // Also reserve full borrowed row-witness equality (including typed NON-key
    // scalars) independently of index entry sizes. Numeric has private validated
    // MAX_TEXT/MAX_DIGITS bounds; neither display nor cloning is used here.
    let witness_work = mul(mul(2, MAX_COLUMNS)?, giggles_pg_types::MAX_TEXT)?;
    cap(
        add(
            add(work, witness_work)?,
            add(mul(rows, ds.len())?, mul(candidates, si::MAX_ARITY)?)?,
        )?,
        si::MAX_WORK,
    )?;
    tick(cancel)
}
fn bind_predicates(
    state: &Relational, ds: &[Descriptor],
) -> R<BTreeMap<u64, partial_index::BoundPredicate>> {
    ds.iter().filter_map(|d| d.predicate.as_ref().map(|p| {
        Ok((d.id, p.bind(state, table(state, d.table)?)?))
    })).collect()
}
fn encoded(
    t: &TableDef,
    key: &[u8],
    row: &SqlRow,
    ds: &[Descriptor],
    predicates: &BTreeMap<u64, partial_index::BoundPredicate>,
    cancel: &mut impl FnMut() -> bool,
) -> R<EncodedRow> {
    tick(cancel)?;
    let pk = pk(t, &row.values)?;
    if pk != key {
        return Err(SqlError::Type);
    }
    let mut tuples = Vec::new();
    for d in ds.iter().filter(|d| d.table == t.id) {
        tick(cancel)?;
        if predicates.get(&d.id).map(|p| p.matches(&row.values)).transpose()? == Some(false) {
            continue;
        }
        let tuple = encode_tuple(
            d.columns
                .iter()
                .map(|id| &row.values[column(t, *id).expect("admitted column")]),
        )?;
        let has_null = d.columns.iter().any(|id| {
            matches!(
                row.values[column(t, *id).expect("admitted column")],
                Value::Null
            )
        });
        tuples.push((d.id, tuple, has_null));
    }
    Ok(EncodedRow {
        table: t.id,
        pk,
        tuples,
    })
}
fn snapshot(
    state: &Relational,
    ds: &[Descriptor],
    overlay: Overlay<'_>,
    cancel: &mut impl FnMut() -> bool,
) -> R<Snapshot> {
    snapshot_with_bind(state, ds, overlay, cancel, bind_predicates)
}

fn snapshot_with_bind(
    state: &Relational,
    ds: &[Descriptor],
    overlay: Overlay<'_>,
    cancel: &mut impl FnMut() -> bool,
    bind: impl FnOnce(&Relational, &[Descriptor]) -> R<BTreeMap<u64, partial_index::BoundPredicate>>,
) -> R<Snapshot> {
    admit(state, ds, overlay, cancel)?;
    let indexed_tables: BTreeSet<_> = ds.iter().map(|descriptor| descriptor.table).collect();
    let predicates = bind(state, ds)?;
    let mut out = Vec::new();
    for (id, map) in &state.rows {
        if map.is_empty() || !indexed_tables.contains(id) {
            continue;
        }
        let t = table(state, *id)?;
        for (key, row) in map {
            tick(cancel)?;
            if overlay.old.is_some_and(|w| w.table == *id && w.key == key) {
                continue;
            }
            out.push(encoded(t, key, row, ds, &predicates, cancel)?);
        }
    }
    if let Some(w) = overlay.new.filter(|w| indexed_tables.contains(&w.table)) {
        out.push(encoded(table(state, w.table)?, w.key, w.row, ds, &predicates, cancel)?);
    }
    Ok(Snapshot(out))
}
const NONE: Overlay<'static> = Overlay {
    old: None,
    new: None,
};

/// A statement borrows exact old witnesses and complete proposed new rows.
/// No constraint-disabled Relational can be extracted from staging.
pub(super) struct RowChange<'a> {
    pub table: u64,
    pub old: Option<(&'a [u8], &'a SqlRow)>,
    pub new: Option<(&'a [u8], &'a SqlRow)>,
}
pub(super) struct StatementProposal<'a> {
    pub changes: &'a [RowChange<'a>],
    pub next_version: u64,
}
// A statement includes full base scalars, witnesses, serialization and all
// indexes; the sidecar retains its independent MAX_WORK. Numeric's private
// bounded representation may require quadratic display work. Reserve up to
// eight conversions per borrowed occurrence, before any formatting or copy.
const STATEMENT_MAX_WORK: usize = si::MAX_WORK;
fn exact_witness(a: &SqlRow, b: &SqlRow) -> R<bool> {
    let limit = crate::relational::max_value_bytes() + MAX_COLUMNS * 16 + 32;
    let a_size = bincode::serialized_size(a).map_err(|_| SqlError::Type)?;
    let b_size = bincode::serialized_size(b).map_err(|_| SqlError::Type)?;
    if a_size > limit as u64 || b_size > limit as u64 {
        return Err(SqlError::Limit);
    }
    Ok(bincode::serialize(a).map_err(|_| SqlError::Type)?
        == bincode::serialize(b).map_err(|_| SqlError::Type)?)
}
struct StagedStatement {
    rows: BTreeMap<u64, BTreeMap<Vec<u8>, SqlRow>>,
    secondary: Sidecar,
    next_version: u64,
    result: SqlResult,
}
struct StatementNamespace5<'a> {
    source: &'a Relational,
    replacements: &'a BTreeMap<u64, BTreeMap<Vec<u8>, SqlRow>>,
    next_version: u64,
}
struct StatementRows<'a>(&'a StatementNamespace5<'a>);
impl Serialize for StatementRows<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.source.rows.len()))?;
        for (id, rows) in &self.0.source.rows {
            map.serialize_entry(id, self.0.replacements.get(id).unwrap_or(rows))?;
        }
        map.end()
    }
}
impl Serialize for StatementNamespace5<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let extended = !self.source.ext.namespace5_ext_is_empty();
        let format = if extended { 2u32 } else { 1u32 };
        let mut s = serializer.serialize_struct("Relational", if extended { 8 } else { 7 })?;
        s.serialize_field("format", &format)?;
        s.serialize_field("generation", &self.source.generation)?;
        s.serialize_field("epoch", &self.source.epoch)?;
        s.serialize_field("next_id", &self.source.next_id)?;
        s.serialize_field("next_version", &self.next_version)?;
        s.serialize_field("tables", self.source.tables.as_ref())?;
        s.serialize_field("rows", &StatementRows(self))?;
        if extended {
            s.serialize_field("ext", self.source.ext.as_ref())?;
        }
        s.end()
    }
}
fn raw_size(v: &Value) -> usize {
    match v {
        Value::Text(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Numeric(n) => n.display_byte_bound(),
        Value::Interval(_) => 16,
        Value::TimeTz(_) => 12,
        _ => 9,
    }
}
fn row_bound(key: &[u8], row: &SqlRow, bytes: &mut usize, work: &mut usize) -> R<()> {
    cap(key.len(), si::MAX_COMPONENT)?;
    cap(row.values.len(), MAX_COLUMNS)?;
    let mut size = 0;
    let mut copy_bytes = key.len();
    for value in &row.values {
        size = add(
            size,
            if matches!(value, Value::Numeric(_)) {
                9
            } else {
                raw_size(value)
            },
        )?;
        cap(size, crate::relational::max_value_bytes())?;
        copy_bytes = add(copy_bytes, raw_size(value))?;
        if matches!(value, Value::Numeric(_)) {
            *work = add(*work, mul(8, mul(raw_size(value), raw_size(value))?)?)?;
        }
    }
    *bytes = add(*bytes, copy_bytes)?;
    *work = add(*work, mul(add(key.len(), size)?, 64)?)?;
    cap(*bytes, si::MAX_BYTES)?;
    cap(*work, STATEMENT_MAX_WORK)
}
/// Crate-visible borrowed batch change; same meaning as RowChange.
pub(crate) struct BatchRowChange<'a> {
    pub table: u64,
    pub old: Option<(&'a [u8], &'a SqlRow)>,
    pub new: Option<(&'a [u8], &'a SqlRow)>,
}
/// SQL immediate UNIQUE occupancy retains unvisited OLD and visited NEW tuples.
/// Callers have admitted the complete mutation snapshot and secondary budgets.
/// The publisher still validates the complete final batch atomically.
/// Built once per multi-row statement (searched UPDATE, ON CONFLICT); the O(rows) build matches
/// the legacy engine's existing unique lookups, which also scan (see stored_preflight).
/// Single-row point UPDATEs do not build it: per-row and final-batch checks coincide there.
pub(crate) struct UpdateUnique {
    indexes: Vec<UpdateUniqueIndex>,
    primary_order: u64,
    primary_name: String,
    detailed: bool,
}
struct UpdateUniqueIndex {
    order: u64,
    key: collation_keys::Key,
    name: String,
    generated_key: bool,
    occupied: BTreeMap<Vec<u8>, Vec<u8>>,
}
impl UpdateUnique {
    pub(crate) fn new(
        state: &Relational,
        table: &TableDef,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<Self> {
        let mut indexes = Vec::new();
        let primary_order = state.primary_key_order(table.id);
        let primary_name = state.primary_collision_name(table);
        let primary = table.primary_key.iter().map(|i| table.columns[*i].id).collect::<Vec<_>>();
        let primary = collation_keys::Key::new(state, table, &primary, None)?;
        let mut keys = Vec::new();
        if primary.nondeterministic() {
            keys.push((primary_order, primary, primary_name.clone()));
        }
        for d in state.secondary.descriptors().iter().filter(|d| d.table == table.id && d.unique) {
            keys.push((d.id, collation_keys::Key::for_index(state, table, d)?, d.name.clone()));
        }
        if state.v2_unique_reporting {
            keys.sort_unstable_by_key(|(order, ..)| *order);
        }
        for (order, key, name) in keys {
            let mut occupied = BTreeMap::new();
            for (owner, row) in state.rows.get(&table.id).ok_or(SqlError::UnknownTable)? {
                tick(cancel)?;
                if let Some(tuple) = key.tuple(&row.values)? { occupied.insert(tuple, owner.clone()); }
            }
            let generated_key = key.columns.iter().any(|i| state.generated_column(table.columns[*i].id).is_some());
            indexes.push(UpdateUniqueIndex { order, key, name, generated_key, occupied });
        }
        Ok(Self { indexes, primary_order, primary_name, detailed: state.v2_unique_reporting })
    }
    pub(crate) fn visit(
        &mut self,
        table: &TableDef,
        old: Option<(&[u8], &SqlRow)>,
        key: &[u8],
        values: &[Value],
        primary_taken: bool,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<()> {
        let primary_error = || {
            collation_keys::key_duplicate(table, &self.primary_name, &table.primary_key, values, false)
                .unwrap_or_else(|| collation_keys::duplicate(&self.primary_name))
        };
        for UpdateUniqueIndex { order, key: equality, name, generated_key, occupied } in &mut self.indexes {
            if primary_taken && *order >= self.primary_order {
                return Err(primary_error());
            }
            tick(cancel)?;
            let tuple = equality.tuple(values)?;
            if let Some(tuple) = &tuple
                && occupied
                    .get(tuple)
                    .is_some_and(|owner| old.is_none_or(|(old_key, _)| owner.as_slice() != old_key))
            {
                let detail = if self.detailed || *generated_key {
                    collation_keys::key_duplicate(table, name, &equality.columns, values, false)
                } else {
                    equality.duplicate_detail(table, name, values, false, self.detailed)
                };
                return Err(detail
                    .unwrap_or_else(|| if self.detailed || equality.nondeterministic() { collation_keys::duplicate(name) } else { SqlError::Duplicate }));
            }
            if let Some((_, old)) = old
                && let Some(tuple) = equality.tuple(&old.values)?
            {
                occupied.remove(&tuple);
            }
            if let Some(tuple) = tuple {
                occupied.insert(tuple, key.to_vec());
            }
        }
        if primary_taken {
            return Err(primary_error());
        }
        Ok(())
    }
}

impl Relational {
    /// Next unassigned row version of this exact relational state.
    pub(crate) fn next_version_engine(&self) -> u64 {
        self.next_version
    }

    // Private explicit full postimage: stage against this exact source and publish
    // immediately in the same call. There is no detached or rebindable stage API.
    pub(super) fn secondary_apply_statement_with(
        &mut self,
        proposal: StatementProposal<'_>,
        mut cancel: impl FnMut() -> bool,
    ) -> R<SqlResult> {
        let staged = self.stage_statement_with(proposal, &mut cancel)?;
        for (table, rows) in staged.rows {
            *self.rows.get_mut(&table).expect("admitted table") = rows;
        }
        self.secondary = staged.secondary;
        self.next_version = staged.next_version;
        Ok(staged.result)
    }
    /// Validated typed entry to the central statement publisher.
    pub(crate) fn secondary_apply_batch_engine(
        &mut self,
        epoch: u64,
        changes: &[BatchRowChange<'_>],
        next_version: u64,
        cancel: impl FnMut() -> bool,
    ) -> R<SqlResult> {
        if self.epoch != epoch {
            return Err(SqlError::StaleSchema);
        }
        cap(changes.len(), MAX_ROWS)?;
        let converted: Vec<RowChange<'_>> = changes
            .iter()
            .map(|c| RowChange {
                table: c.table,
                old: c.old,
                new: c.new,
            })
            .collect();
        self.secondary_apply_statement_with(
            StatementProposal {
                changes: &converted,
                next_version,
            },
            cancel,
        )
    }
    fn stage_statement_with(
        &self,
        proposal: StatementProposal<'_>,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<StagedStatement> {
        cap(proposal.changes.len(), MAX_ROWS)?;
        let mut bytes = 0;
        let mut work = 0;
        let mut max_pk = 0usize;
        // WHOLE borrowed proposal before any variable witness comparisons or copies.
        for change in proposal.changes {
            for (key, row) in change.old.iter().chain(change.new.iter()) {
                row_bound(key, row, &mut bytes, &mut work)?;
                max_pk = max_pk.max(key.len());
            }
        }
        let mut base_rows = 0;
        for rows in self.rows.values() {
            base_rows = add(base_rows, rows.len())?;
            for (key, row) in rows {
                row_bound(key, row, &mut bytes, &mut work)?;
                max_pk = max_pk.max(key.len());
            }
        }
        // Covers all witness comparisons, tree searches, tuple construction and
        // final-image pair work before the first comparison/copy.
        work = add(
            work,
            mul(
                mul(
                    add(base_rows, proposal.changes.len())?,
                    add(proposal.changes.len(), 1)?,
                )?,
                add(max_pk, 1)?,
            )?,
        )?;
        cap(work, STATEMENT_MAX_WORK)?;
        tick(cancel)?;
        let ds = self.secondary.descriptors();
        admit(self, ds, NONE, cancel)?;
        let mut tuple_bytes = 0;
        let mut tuple_count = 0;
        for change in proposal.changes {
            for (key, row) in change.old.iter().chain(change.new.iter()) {
                tick(cancel)?;
                if referenced(ds, change.table) {
                    let (n, b) = row_size(table(self, change.table)?, key, row, ds)?;
                    tuple_count = add(tuple_count, n)?;
                    tuple_bytes = add(tuple_bytes, b)?;
                }
            }
        }
        cap(tuple_count, mul(si::MAX_CANDIDATES, 2)?)?;
        cap(tuple_bytes, mul(si::MAX_BYTES, 2)?)?;
        work = add(work, mul(tuple_bytes, 64)?)?;
        cap(work, STATEMENT_MAX_WORK)?;
        self.secondary_validate_engine(&mut *cancel)?;
        if proposal.next_version < self.next_version {
            return Err(SqlError::Version);
        }
        for (i, change) in proposal.changes.iter().enumerate() {
            tick(cancel)?;
            let rows = self.rows.get(&change.table).ok_or(SqlError::UnknownTable)?;
            let identity = change.old.or(change.new).map(|(k, _)| k);
            if proposal.changes[..i].iter().any(|prior| {
                prior.table == change.table
                    && identity.is_some()
                    && prior
                        .old
                        .or(prior.new)
                        .is_some_and(|(k, _)| Some(k) == identity)
            }) {
                let row = change.new.or(change.old).expect("matching row identity").1;
                return Err(self.primary_duplicate(table(self, change.table)?, &row.values));
            }
            if let Some((key, old)) = change.old {
                if !rows
                    .get(key)
                    .map(|actual| exact_witness(actual, old))
                    .transpose()?
                    .unwrap_or(false)
                {
                    return Err(SqlError::Conflict);
                }
            } else if let Some((key, row)) = change.new
                && rows.contains_key(key)
            {
                return Err(self.primary_duplicate(table(self, change.table)?, &row.values));
            }
            if let (Some((old, _)), Some((new, _))) = (change.old, change.new)
                && old != new
            {
                return Err(SqlError::Unsupported);
            }
            if let Some((key, row)) = change.new {
                let t = table(self, change.table)?;
                row_check(t, &row.values)?;
                if pk(t, &row.values)? != key {
                    return Err(SqlError::Type);
                }
                if row.version == 0 || row.version >= proposal.next_version {
                    return Err(SqlError::Version);
                }
            }
        }
        let mut final_rows = 0usize;
        let mut final_candidates = 0usize;
        let mut final_bytes = 0usize;
        for (id, map) in &self.rows {
            if referenced(ds, *id) {
                final_rows = add(final_rows, map.len())?;
                for (key, row) in map {
                    let (n, b) = row_size(table(self, *id)?, key, row, ds)?;
                    final_candidates = add(final_candidates, n)?;
                    final_bytes = add(final_bytes, b)?;
                }
            }
        }
        for change in proposal.changes {
            if referenced(ds, change.table) {
                for (key, row) in change.old.iter() {
                    let (n, b) = row_size(table(self, change.table)?, key, row, ds)?;
                    final_rows = final_rows.checked_sub(1).ok_or(SqlError::Conflict)?;
                    final_candidates = final_candidates.checked_sub(n).ok_or(SqlError::Conflict)?;
                    final_bytes = final_bytes.checked_sub(b).ok_or(SqlError::Conflict)?;
                }
            }
        }
        for change in proposal.changes {
            if referenced(ds, change.table)
                && let Some((key, row)) = change.new
            {
                let (n, b) = row_size(table(self, change.table)?, key, row, ds)?;
                final_rows = add(final_rows, 1)?;
                final_candidates = add(final_candidates, n)?;
                final_bytes = add(final_bytes, b)?;
            }
        }
        cap(final_rows, si::MAX_ROWS)?;
        cap(final_candidates, si::MAX_CANDIDATES)?;
        cap(final_bytes, si::MAX_BYTES)?;
        cap(add(work, mul(final_bytes, 64)?)?, STATEMENT_MAX_WORK)?;
        let mut versions = BTreeSet::new();
        for (id, map) in &self.rows {
            for (key, row) in map {
                tick(cancel)?;
                if !proposal
                    .changes
                    .iter()
                    .any(|c| c.table == *id && c.old.is_some_and(|(k, _)| k == key))
                    && (row.version == 0
                        || row.version >= proposal.next_version
                        || !versions.insert(row.version))
                {
                    return Err(SqlError::Version);
                }
            }
        }
        for change in proposal.changes {
            if let Some((_, row)) = change.new
                && !versions.insert(row.version)
            {
                return Err(SqlError::Version);
            }
        }
        let mut rows = BTreeMap::new();
        for change in proposal.changes {
            tick(cancel)?;
            rows.entry(change.table)
                .or_insert_with(|| self.rows[&change.table].clone());
        }
        // Remove ALL old memberships before adding ANY new row. Uniqueness is
        // recomputed once over untouched survivors plus the complete new batch.
        for change in proposal.changes {
            tick(cancel)?;
            if let Some((key, _)) = change.old {
                rows.get_mut(&change.table)
                    .expect("staged table")
                    .remove(key);
            }
        }
        for change in proposal.changes {
            tick(cancel)?;
            if let Some((key, row)) = change.new {
                rows.get_mut(&change.table)
                    .expect("staged table")
                    .insert(key.to_vec(), row.clone());
            }
        }
        for (id, map) in &rows {
            self.validate_collated_unique(table(self, *id)?, map, ds, cancel)?;
        }
        let mut count = 0;
        let mut candidates = 0;
        let mut index_bytes = 0;
        // Admit the complete actual postimage before any encoded tuple copies.
        for (id, original) in &self.rows {
            let map = rows.get(id).unwrap_or(original);
            if referenced(ds, *id) {
                count = add(count, map.len())?;
                cap(count, si::MAX_ROWS)?;
                for (key, row) in map {
                    let (n, b) = row_size(table(self, *id)?, key, row, ds)?;
                    candidates = add(candidates, n)?;
                    index_bytes = add(index_bytes, b)?;
                }
            }
        }
        cap(candidates, si::MAX_CANDIDATES)?;
        cap(index_bytes, si::MAX_BYTES)?;
        cap(add(work, mul(index_bytes, 64)?)?, STATEMENT_MAX_WORK)?;
        let predicates = bind_predicates(self, ds)?;
        let mut projected = Vec::new();
        for (id, original) in &self.rows {
            if referenced(ds, *id) {
                for (key, row) in rows.get(id).unwrap_or(original) {
                    projected.push(encoded(table(self, *id)?, key, row, ds, &predicates, cancel)?);
                }
            }
        }
        let snap = Snapshot(projected);
        let tuples = snap.tuples();
        let mut secondary = Sidecar::default();
        secondary
            .build(ds, &snap.rows(&tuples), &CatalogView::new(self), &mut *cancel)
            .map_err(|failure| {
                let failure = error(failure);
                if failure == SqlError::Duplicate {
                    for (id, map) in &rows {
                        let table = table(self, *id).expect("projected table");
                        if self.validate_index_details(table, map, ds, false, |pk| {
                            proposal.changes.iter().any(|change| change.table == *id
                                && change.new.is_some_and(|(key, _)| key == pk))
                        }, cancel) == Err(SqlError::Duplicate)
                        {
                            break;
                        }
                    }
                }
                failure
            })?;
        let result = SqlResult {
            affected: proposal
                .changes
                .iter()
                .filter(|c| c.old.is_some() || c.new.is_some())
                .count() as u64,
            ..SqlResult::default()
        };
        let image = StatementNamespace5 {
            source: self,
            replacements: &rows,
            next_version: proposal.next_version,
        };
        let base_size = bincode::serialized_size(&image).map_err(|_| SqlError::Type)?;
        let base_size = usize::try_from(base_size).map_err(|_| SqlError::Limit)?;
        cap(base_size, si::MAX_BYTES)?;
        cap(add(work, mul(base_size, 64)?)?, STATEMENT_MAX_WORK)?;
        let payload = si::codec::encode(&secondary, &mut *cancel).map_err(error)?;
        cap(add(add(base_size, payload.len())?, 42)?, si::MAX_BYTES)?;
        tick(cancel)?;
        let _base = bincode::serialize(&image).map_err(|_| SqlError::Type)?;
        tick(cancel)?; // Last fallible operation before publication of every field.
        Ok(StagedStatement {
            rows,
            secondary,
            next_version: proposal.next_version,
            result,
        })
    }
    pub(super) fn secondary_apply_statement(
        &mut self,
        table: u64,
        old: Option<(&[u8], &SqlRow)>,
        new: Option<(&[u8], &SqlRow)>,
        next_version: u64,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<SqlResult> {
        self.secondary_apply_statement_with(
            StatementProposal {
                changes: &[RowChange { table, old, new }],
                next_version,
            },
            cancel,
        )
    }
}

impl Relational {
    pub(crate) fn has_secondary_indexes_engine(&self) -> bool {
        !self.secondary.descriptors().is_empty()
    }

    /// Storage must first validate the decoded legacy base and its outer feature
    /// gates. Namespace8 descriptors are the only index definitions; project ALL
    /// referenced authoritative row maps, never a SELECT page or a second catalog.
    /// On every error (including the last cancellation fence) self is unchanged.
    pub(crate) fn secondary_restore_engine(
        &mut self,
        mut pending: si::codec::PendingDecode,
        mut cancel: impl FnMut() -> bool,
    ) -> R<()> {
        pending.normalize_r1_names(|id| self.table_schema(id));
        let ds = pending.descriptors();
        // A namespace8 record must contain an index, including on empty tables.
        // IDs belong to the persisted shared allocator, not a new registry.
        if ds.is_empty() || ds.iter().any(|d| d.id >= self.next_id) {
            return Err(SqlError::Type);
        }
        let snap = snapshot_with_bind(self, ds, NONE, &mut cancel, |state, descriptors| {
            // An over-limit expression in a stored descriptor is corrupt. Keep
            // ordinary sidecar resource limits and cancellation distinct.
            bind_predicates(state, descriptors).map_err(|error| match error {
                SqlError::Limit => SqlError::Type,
                error => error,
            })
        })?;
        let tuples = snap.tuples();
        let staged = pending
            .validate_complete(&snap.rows(&tuples), &CatalogView::new(self), &mut cancel)
            .map_err(error)?;
        super::catalog_ext::validate_unique_indexes(
            self.ext.constraints.iter().flat_map(|(t, cs)| cs.iter().map(move |c| (*t, c))),
            staged.descriptors().iter(),
        )?;
        super::catalog_ext::validate_key_targets(self.ext.as_ref(), self.tables.values(), staged.descriptors().iter(), self.next_id)?;
        tick(&mut cancel)?;
        self.secondary = staged;
        Ok(())
    }

    /// ONLY for the coordinated namespace5 + namespace8 record path. The caller
    /// must store both returned records under the indexed outer format, rejecting
    /// combined application/index state before calling. No base-only bypass is
    /// exposed. Direct Serialize(indexed) remains fail closed.
    pub(crate) fn secondary_persistence_engine(
        &self,
        mut cancel: impl FnMut() -> bool,
    ) -> R<(Vec<u8>, Vec<u8>)> {
        if !self.has_secondary_indexes_engine() {
            return Err(SqlError::Unsupported);
        }
        self.validate_unique_indexes()?;
        self.secondary_validate_engine(&mut cancel)?;
        let payload = si::codec::encode(&self.secondary, &mut cancel).map_err(error)?;
        tick(&mut cancel)?;
        // The paired storage path must bound the exact legacy base before its
        // allocation, including both record headers and the admitted ns8 bytes.
        let base_size =
            bincode::serialized_size(&LegacyNamespace5(self)).map_err(|_| SqlError::Type)?;
        if base_size
            .checked_add(payload.len() as u64)
            .and_then(|n| n.checked_add(42))
            .is_none_or(|n| n > si::MAX_BYTES as u64)
        {
            return Err(SqlError::Limit);
        }
        tick(&mut cancel)?;
        let base = bincode::serialize(&LegacyNamespace5(self)).map_err(|_| SqlError::Type)?;
        tick(&mut cancel)?;
        Ok((base, payload))
    }

    /// Read-only persistence handoff: a validated runtime sidecar, including
    /// descriptors on empty tables. Does not serialize or admit a new index.
    #[allow(dead_code)]
    pub(crate) fn secondary_state_engine(&self, cancel: impl FnMut() -> bool) -> R<&Sidecar> {
        self.secondary_validate_engine(cancel)?;
        Ok(&self.secondary)
    }

    /// Stage2 access bridge: exact schema fence and complete authoritative
    /// recomputation, then borrowed sidecar and base row map. Bounds/projection,
    /// residual evaluation, output budgets and truthful explain belong to caller.
    #[allow(dead_code)] // Stage2 internal read handoff; no planner is enabled.
    pub(crate) fn secondary_access_engine(
        &self,
        table: &TableDef,
        epoch: u64,
        mut cancel: impl FnMut() -> bool,
    ) -> R<(&Sidecar, &BTreeMap<Vec<u8>, SqlRow>)> {
        cap(table.name.len(), 63)?;
        cap(table.columns.len(), MAX_COLUMNS)?;
        cap(table.primary_key.len(), MAX_COLUMNS)?;
        for c in &table.columns {
            cap(c.name.len(), 63)?;
        }
        if epoch != self.epoch {
            return Err(SqlError::StaleSchema);
        }
        tick(&mut cancel)?;
        Prepared::engine_table(table, self)?;
        self.secondary_validate_engine(&mut cancel)?;
        Ok((
            &self.secondary,
            self.rows.get(&table.id).ok_or(SqlError::UnknownTable)?,
        ))
    }
    pub(crate) fn secondary_validate_engine(&self, mut cancel: impl FnMut() -> bool) -> R<()> {
        let ds = self.secondary.descriptors();
        let snap = snapshot(self, ds, NONE, &mut cancel)?;
        let tuples = snap.tuples();
        self.secondary
            .validate(&snap.rows(&tuples), &CatalogView::new(self), &mut cancel)
            .map_err(error)?;
        Ok(())
    }
    /// Complete-row sidecar build for `ds` over this exact state; self is unchanged.
    pub(super) fn secondary_build_complete_engine(
        &self,
        ds: &[Descriptor],
        mut cancel: impl FnMut() -> bool,
    ) -> R<Sidecar> {
        let check_collations = self.needs_collated_unique_validation(ds);
        if check_collations {
            self.validate_catalog_collated_unique(ds, &mut cancel)?;
        }
        let snap = snapshot(self, ds, NONE, &mut cancel).or_else(|error| {
            if !check_collations {
                // Admission checks descriptor columns. On failure, recover
                // the collation pass's UnknownColumn precedence, including
                // over cancellation and malformed descriptor shape.
                for d in ds.iter().filter(|d| d.unique) {
                    if let Ok(t) = table(self, d.table) {
                        for id in &d.columns {
                            column(t, *id)?;
                        }
                    }
                }
            }
            Err(error)
        })?;
        let tuples = snap.tuples();
        let mut staged = Sidecar::default();
        staged
            .build(ds, &snap.rows(&tuples), &CatalogView::new(self), &mut cancel)
            .map_err(error)?;
        Ok(staged)
    }
    fn validate_catalog_collated_unique(
        &self,
        ds: &[Descriptor],
        cancel: &mut impl FnMut() -> bool,
    ) -> R<()> {
        for table in self.tables.values() {
            self.validate_collated_unique(table, &self.rows[&table.id], ds, cancel)?;
        }
        Ok(())
    }
    pub(super) fn secondary_admit_values<'a>(values: impl Iterator<Item = &'a Value>) -> R<()> {
        let mut bytes = 0;
        let mut work = 0;
        for value in values {
            bytes = add(
                bytes,
                if matches!(value, Value::Numeric(_)) {
                    9
                } else {
                    raw_size(value)
                },
            )?;
            cap(bytes, crate::relational::max_value_bytes())?;
            work = add(work, raw_size(value))?;
            if matches!(value, Value::Numeric(_)) {
                work = add(work, mul(8, mul(raw_size(value), raw_size(value))?)?)?;
            }
            cap(work, STATEMENT_MAX_WORK)?;
        }
        Ok(())
    }
    /// Allocation-free admission before central mutation expression/row copies.
    pub(super) fn secondary_admit_mutation(&self, cancel: &mut impl FnMut() -> bool) -> R<()> {
        let mut bytes = 0;
        let mut work = 0;
        for map in self.rows.values() {
            for (key, row) in map {
                row_bound(key, row, &mut bytes, &mut work)?;
            }
        }
        if self.has_secondary_indexes_engine() {
            admit(self, self.secondary.descriptors(), NONE, cancel)?;
        }
        Ok(())
    }
    /// Test-only legacy sidecar handoff delegates to the same checked batch stage.
    #[cfg(test)]
    fn secondary_stage_change_with(
        &self,
        table: u64,
        old: Option<(&[u8], &SqlRow)>,
        new: Option<(&[u8], &SqlRow)>,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<Option<Sidecar>> {
        let staged = self.stage_statement_with(
            StatementProposal {
                changes: &[RowChange { table, old, new }],
                next_version: self
                    .next_version
                    .max(new.map_or(0, |(_, r)| r.version.saturating_add(1))),
            },
            cancel,
        )?;
        Ok(self
            .has_secondary_indexes_engine()
            .then_some(staged.secondary))
    }

    /// Deliberately test-only: no public/internal CREATE admission is released.
    #[cfg(test)]
    pub(crate) fn secondary_configure_test(
        &mut self,
        table_id: u64,
        name: &str,
        columns: &[u64],
        mut cancel: impl FnMut() -> bool,
    ) -> R<u64> {
        cap(name.len(), si::MAX_NAME)?;
        cap(columns.len(), si::MAX_ARITY)?;
        tick(&mut cancel)?;
        let mut next = self.next_id;
        let id = allocate(&mut next)?;
        let epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        let mut ds = self.secondary.descriptors().to_vec();
        ds.push(Descriptor {
            id,
            table: table_id,
            name: name.to_owned(),
            columns: columns.to_vec(),
            key_encoding: 1,
            unique: false,
            predicate: None,
        });
        let snap = snapshot(self, &ds, NONE, &mut cancel)?;
        let tuples = snap.tuples();
        let mut staged = Sidecar::default();
        staged
            .build(&ds, &snap.rows(&tuples), &CatalogView::new(self), &mut cancel)
            .map_err(error)?;
        tick(&mut cancel)?;
        self.secondary = staged;
        self.next_id = next;
        self.epoch = epoch;
        Ok(id)
    }
}

// Exact legacy field order and bincode shape; serde skip/default sidecar is
// never silently dropped from an indexed state, including direct encode calls.
impl Serialize for Relational {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.has_secondary_indexes_engine() || !self.view_names.is_empty() {
            return Err(serde::ser::Error::custom(
                "runtime secondary indexes require explicit persistence handoff",
            ));
        }
        LegacyNamespace5(self).serialize(serializer)
    }
}

// Private wrapper: the only indexed caller is the paired record encoder above.
// TableDef is serialized directly with its EXACT existing fields and order.
struct LegacyNamespace5<'a>(&'a Relational);
impl Serialize for LegacyNamespace5<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let extended = !self.0.ext.namespace5_ext_is_empty();
        let format = if extended { 2u32 } else { 1u32 };
        let mut s = serializer.serialize_struct("Relational", if extended { 8 } else { 7 })?;
        s.serialize_field("format", &format)?;
        s.serialize_field("generation", &self.0.generation)?;
        s.serialize_field("epoch", &self.0.epoch)?;
        s.serialize_field("next_id", &self.0.next_id)?;
        s.serialize_field("next_version", &self.0.next_version)?;
        s.serialize_field("tables", self.0.tables.as_ref())?;
        s.serialize_field("rows", &self.0.rows)?;
        if extended {
            s.serialize_field("ext", self.0.ext.as_ref())?;
        }
        s.end()
    }
}

// Bincode structs have no field count on the wire. Read the discriminator before
// choosing whether the eighth field exists, preserving every format-1 byte.
impl<'de> Deserialize<'de> for Relational {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RelationalVisitor;
        impl<'de> serde::de::Visitor<'de> for RelationalVisitor {
            type Value = Relational;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a versioned relational catalog")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error;
                let format: u32 = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("missing relational format"))?;
                if !matches!(format, 1 | 2) {
                    return Err(A::Error::custom(catalog_ext::UNSUPPORTED_CATALOG_EXT));
                }
                macro_rules! field {
                    ($name:literal) => {
                        seq.next_element()?.ok_or_else(|| {
                            A::Error::custom(concat!("missing relational ", $name))
                        })?
                    };
                }
                let mut value = Relational {
                    format: 1,
                    v2_unique_reporting: false,
                    generation: field!("generation"),
                    epoch: field!("epoch"),
                    next_id: field!("next_id"),
                    next_version: field!("next_version"),
                    tables: Arc::new(field!("tables")),
                    rows: field!("rows"),
                    ext: Arc::new(CatalogExt::default()),
                    secondary: Sidecar::default(),
                    view_names: BTreeMap::new(),
                };
                if format == 2 {
                    value.ext = Arc::new(field!("ext"));
                    if value.ext.namespace5_ext_is_empty() {
                        return Err(A::Error::custom("empty format-2 catalog extension"));
                    }
                    for table in std::sync::Arc::make_mut(&mut value.tables).values_mut() {
                        for column in &mut table.columns {
                            if let Some(kind) =
                                value.ext.columns.get(&column.id).and_then(|c| c.text_kind)
                            {
                                if column.data_type != DataType::Text {
                                    return Err(A::Error::custom("text kind on non-text column"));
                                }
                                column.data_type = match kind {
                                    TextKind::Varchar => DataType::Varchar,
                                    TextKind::Bpchar => DataType::Bpchar,
                                };
                            }
                        }
                    }
                    for table in std::sync::Arc::make_mut(&mut value.tables).values_mut() {
                        for column in &mut table.columns {
                            if let Some(kind) = value.ext.logical_kinds.get(&column.id) {
                                let physical = match kind {
                                    catalog_ext::LogicalKind::TsVector | catalog_ext::LogicalKind::TsQuery => DataType::Bytes,
                                    _ => DataType::Text,
                                };
                                if column.data_type != physical {
                                    return Err(A::Error::custom("logical kind on wrong physical column type"));
                                }
                                column.data_type = match kind {
                                    catalog_ext::LogicalKind::Uuid => DataType::Uuid,
                                    catalog_ext::LogicalKind::Jsonb => DataType::Jsonb,
                                    catalog_ext::LogicalKind::IntArray => DataType::IntArray,
                                    catalog_ext::LogicalKind::BigIntArray => DataType::BigIntArray,
                                    catalog_ext::LogicalKind::TextArray => DataType::TextArray,
                                    catalog_ext::LogicalKind::UuidArray => DataType::UuidArray,
                                    catalog_ext::LogicalKind::TsVector => DataType::TsVector,
                                    catalog_ext::LogicalKind::TsQuery => DataType::TsQuery,
                                };
                            }
                        }
                    }
                    value
                        .normalize_r1_relation_names()
                        .map_err(A::Error::custom)?;
                    value.validate_ext().map_err(|error| {
                        A::Error::custom(format!("relational catalog invariant: {error:?}"))
                    })?;
                }
                Ok(value)
            }
        }
        deserializer.deserialize_struct(
            "Relational",
            &[
                "format",
                "generation",
                "epoch",
                "next_id",
                "next_version",
                "tables",
                "rows",
                "ext",
            ],
            RelationalVisitor,
        )
    }
}

#[cfg(test)]
#[path = "secondary_index_adapter_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "secondary_index_persistence_tests.rs"]
mod persistence_tests;
