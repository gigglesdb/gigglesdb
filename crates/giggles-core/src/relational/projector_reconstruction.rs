//! Scratch-only reconstruction, never a publication or authority interface.
//!
//! The caller MUST pair counters, descriptor and rows from the SAME canonical
//! validated source/projection. Epoch/generation and opaque allocator counters
//! are consistency fences, not authenticated provenance. This module cannot
//! prove that pairing and deliberately invents no fingerprint or authority.
use super::*;
use std::mem::size_of;

const MAX_RECONSTRUCTION_ROWS: usize = MAX_ROWS;
const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const MAX_SCRATCH_BYTES: usize = 128 * 1024 * 1024;
const MAX_WORK: usize = 64 * 1024 * 1024;
// Conservative per-entry slack covers partially occupied standard B-tree nodes,
// including the one-entry table/row-map roots and validation's sets.
const TREE_ENTRY_BYTES: usize = 4096;
// Numeric is opaque and bounded by its owner. Do not allocate to_string merely
// to admit its formatting/key work. Charge coefficient clones, radix/text,
// padding/growth, digits and key-body scratch conservatively. These cumulative
// accounting bounds deliberately reject some otherwise representable inputs.
const NUMERIC_HEAP: usize = giggles_pg_types::MAX_TEXT * 2;
const NUMERIC_SCRATCH: usize = 16 * (giggles_pg_types::MAX_TEXT + giggles_pg_types::MAX_DIGITS);

/// Original allocator state. No serde or external constructor.
pub(crate) struct ProjectionCounters {
    next_id: u64,
    next_version: u64,
    epoch: u64,
    generation: u64,
}

/// Trusted caller bounds, each checked against a hard finite maximum.
pub(crate) struct ReconstructionLimits {
    pub(crate) rows: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) scratch_bytes: usize,
    pub(crate) work: usize,
}

impl Default for ReconstructionLimits {
    fn default() -> Self {
        Self {
            rows: MAX_RECONSTRUCTION_ROWS,
            retained_bytes: MAX_RETAINED_BYTES,
            scratch_bytes: MAX_SCRATCH_BYTES,
            work: MAX_WORK,
        }
    }
}

/// Owns only an isolated relation; no raw, serde, State, export or publication
/// conversions, Deref, AsRef, Borrow or extraction interface are provided.
pub(crate) struct ScratchRelation {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "GD53 T2a scratch-only boundary; production consumer requires separately authorized T2b"
        )
    )]
    relation: Relational,
}

fn tick(cancel: &mut impl FnMut() -> bool) -> R<()> {
    if cancel() {
        Err(SqlError::Aborted)
    } else {
        Ok(())
    }
}

fn allocator_bounds(next_id: u64, next_version: u64) -> R<()> {
    if next_id == 0 || next_version == 0 {
        return Err(SqlError::Version);
    }
    if next_id == u64::MAX || next_version == u64::MAX {
        return Err(SqlError::Overflow);
    }
    Ok(())
}

impl Relational {
    /// The relation must already have canonical validated-source provenance.
    /// This scalar-only capture does not scan or allocate to validate a source.
    pub(crate) fn projector_counters_engine(
        &self,
        expected_epoch: u64,
        expected_generation: u64,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<ProjectionCounters> {
        tick(cancel)?;
        if self.format != 1 || self.has_secondary_indexes_engine() {
            return Err(SqlError::Unsupported);
        }
        if self.epoch != expected_epoch || self.generation != expected_generation {
            return Err(SqlError::StaleSchema);
        }
        allocator_bounds(self.next_id, self.next_version)?;
        let counters = ProjectionCounters {
            next_id: self.next_id,
            next_version: self.next_version,
            epoch: self.epoch,
            generation: self.generation,
        };
        tick(cancel)?;
        Ok(counters)
    }
}

#[derive(Default)]
struct AllocationBound {
    retained: usize,
    scratch: usize,
    work: usize,
}

fn add(total: &mut usize, amount: usize, maximum: usize) -> R<()> {
    *total = total.checked_add(amount).ok_or(SqlError::Limit)?;
    if *total > maximum {
        return Err(SqlError::Limit);
    }
    Ok(())
}

fn product(left: usize, right: usize) -> R<usize> {
    left.checked_mul(right).ok_or(SqlError::Limit)
}

// No allocation: heap/scanning upper bounds use borrowed lengths or owner caps.
fn value_bound(value: &Value) -> R<(usize, usize, usize)> {
    match value {
        Value::Text(text) => Ok((text.len(), 0, text.len())),
        Value::Bytes(bytes) => Ok((bytes.len(), 0, bytes.len())),
        Value::Numeric(_) => Ok((NUMERIC_HEAP, NUMERIC_SCRATCH, giggles_pg_types::MAX_TEXT)),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float4(_) | Value::Float8(_) => {
            Ok((0, 0, size_of::<Value>()))
        }
        // Temporal values and future variants require separately audited bounds.
        _ => Err(SqlError::Unsupported),
    }
}

fn key_bound(value: &Value) -> R<usize> {
    match value {
        Value::Null => Ok(1),
        Value::Bool(_) => Ok(2),
        Value::Int(_) | Value::Float4(_) | Value::Float8(_) => Ok(9),
        Value::Text(text) => product(text.len(), 2)?
            .checked_add(3)
            .ok_or(SqlError::Limit),
        Value::Bytes(bytes) => product(bytes.len(), 2)?
            .checked_add(3)
            .ok_or(SqlError::Limit),
        Value::Numeric(_) => Ok(giggles_pg_types::MAX_DIGITS + 8),
        _ => Err(SqlError::Unsupported),
    }
}

/// Complete allocation-free preflight. Every subsequent variable allocation,
/// including existing row_check/pk/validate internals, is covered before entry.
fn preflight(
    table: &TableDef,
    rows: &BTreeMap<Vec<u8>, SqlRow>,
    limits: &ReconstructionLimits,
    cancel: &mut impl FnMut() -> bool,
) -> R<AllocationBound> {
    if limits.rows == 0
        || limits.rows > MAX_RECONSTRUCTION_ROWS
        || limits.retained_bytes == 0
        || limits.retained_bytes > MAX_RETAINED_BYTES
        || limits.scratch_bytes == 0
        || limits.scratch_bytes > MAX_SCRATCH_BYTES
        || limits.work == 0
        || limits.work > MAX_WORK
    {
        return Err(SqlError::Limit);
    }
    if rows.len() > limits.rows
        || table.columns.is_empty()
        || table.columns.len() > MAX_COLUMNS
        || table.primary_key.is_empty()
        || table.primary_key.len() > table.columns.len()
    {
        return Err(SqlError::Limit);
    }
    // Bound identifier scans before validate; retain its exact semantic checks.
    if table.name.len() > 63 || table.columns.iter().any(|column| column.name.len() > 63) {
        return Err(SqlError::Syntax);
    }
    for index in &table.primary_key {
        tick(cancel)?;
        let column = table.columns.get(*index).ok_or(SqlError::Type)?;
        if !matches!(
            column.data_type,
            DataType::Boolean
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Text
                | DataType::Bytes
                | DataType::Numeric
                | DataType::Real
                | DataType::DoublePrecision
        ) {
            return Err(SqlError::Unsupported);
        }
    }
    let mut bound = AllocationBound::default();
    add(
        &mut bound.retained,
        2 * TREE_ENTRY_BYTES + size_of::<Relational>(),
        limits.retained_bytes,
    )?;
    add(
        &mut bound.retained,
        table.name.len() * 2,
        limits.retained_bytes,
    )?;
    add(
        &mut bound.retained,
        product(table.columns.len(), size_of::<ColumnDef>())?,
        limits.retained_bytes,
    )?;
    add(
        &mut bound.retained,
        product(table.primary_key.len(), size_of::<usize>())?,
        limits.retained_bytes,
    )?;
    // validate's ID/name/PK sets and row-version set. Charge all entries before
    // it can allocate any tree nodes, not after discovering a duplicate.
    let entries = (table.columns.len() * 2)
        .checked_add(table.primary_key.len())
        .and_then(|count| count.checked_add(rows.len()))
        .and_then(|count| count.checked_add(1))
        .ok_or(SqlError::Limit)?;
    add(
        &mut bound.scratch,
        product(entries, TREE_ENTRY_BYTES)?,
        limits.scratch_bytes,
    )?;
    for column in &table.columns {
        tick(cancel)?;
        add(
            &mut bound.retained,
            column.name.len(),
            limits.retained_bytes,
        )?;
        add(&mut bound.work, column.name.len() + 1, limits.work)?;
    }
    for (key, row) in rows {
        tick(cancel)?;
        if row.values.len() != table.columns.len() {
            return Err(SqlError::Type);
        }
        add(&mut bound.retained, TREE_ENTRY_BYTES, limits.retained_bytes)?;
        add(&mut bound.retained, key.len(), limits.retained_bytes)?;
        add(
            &mut bound.retained,
            product(row.values.len(), size_of::<Value>())?,
            limits.retained_bytes,
        )?;
        add(&mut bound.work, key.len(), limits.work)?;
        for value in &row.values {
            tick(cancel)?;
            let (heap, scratch, work) = value_bound(value)?;
            add(&mut bound.retained, heap, limits.retained_bytes)?;
            // row_check is invoked explicitly and again by final validate.
            add(
                &mut bound.scratch,
                product(scratch, 2)?,
                limits.scratch_bytes,
            )?;
            add(&mut bound.work, product(work, 2)?, limits.work)?;
        }
        add(
            &mut bound.scratch,
            product(table.primary_key.len(), 2 * size_of::<Value>())?,
            limits.scratch_bytes,
        )?;
        for index in &table.primary_key {
            tick(cancel)?;
            let value = &row.values[*index];
            let (heap, scratch, work) = value_bound(value)?;
            // Both pk calls clone PK values. encode_key also calls payload_bound
            // then numeric::key, which formats and constructs digits/body.
            add(&mut bound.scratch, product(heap, 2)?, limits.scratch_bytes)?;
            add(
                &mut bound.scratch,
                product(scratch, 4)?,
                limits.scratch_bytes,
            )?;
            add(
                &mut bound.scratch,
                product(key_bound(value)?, 8)?,
                limits.scratch_bytes,
            )?;
            add(&mut bound.work, product(work, 4)?, limits.work)?;
        }
    }
    tick(cancel)?;
    Ok(bound)
}

impl ScratchRelation {
    /// Reconstruct only scratch state using ORIGINAL counters. The caller must
    /// establish same-source pairing; consistency validation is not provenance.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "GD53 T2a scratch-only boundary; production consumer requires separately authorized T2b"
        )
    )]
    pub(crate) fn reconstruct(
        table: &TableDef,
        rows: &BTreeMap<Vec<u8>, SqlRow>,
        counters: &ProjectionCounters,
        limits: &ReconstructionLimits,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<Self> {
        tick(cancel)?;
        allocator_bounds(counters.next_id, counters.next_version)?;
        preflight(table, rows, limits, cancel)?;
        // All allocations within these existing semantic helpers were admitted
        // above, including Numeric formatting and pk's temporary cloned tuple.
        for (key, row) in rows {
            tick(cancel)?;
            row_check(table, &row.values)?;
            if pk(table, &row.values)? != *key
                || row.version == 0
                || row.version >= counters.next_version
            {
                return Err(SqlError::Version);
            }
        }
        tick(cancel)?;
        let mut copied_rows = BTreeMap::new();
        for (key, row) in rows {
            tick(cancel)?;
            copied_rows.insert(key.clone(), row.clone());
        }
        tick(cancel)?;
        let relation = Relational {
            format: 1,
            generation: counters.generation,
            epoch: counters.epoch,
            next_id: counters.next_id,
            next_version: counters.next_version,
            tables: BTreeMap::from([(table.name.clone(), table.clone())]),
            rows: BTreeMap::from([(table.id, copied_rows)]),
            secondary: Default::default(),
        };
        // No indexes were reconstructed, so validate's ambient index-only branch
        // is unreachable. Reuse all existing schema/row/key/ID/version semantics.
        relation.validate()?;
        tick(cancel)?;
        Ok(Self { relation })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Relational, TableDef, BTreeMap<Vec<u8>, SqlRow>) {
        let mut source = Relational::default();
        let create = Prepared::create_engine(
            "items".into(),
            vec![
                ColumnDef {
                    id: 0,
                    name: "id".into(),
                    data_type: DataType::BigInt,
                    not_null: true,
                },
                ColumnDef {
                    id: 0,
                    name: "amount".into(),
                    data_type: DataType::Numeric,
                    not_null: true,
                },
                ColumnDef {
                    id: 0,
                    name: "bits".into(),
                    data_type: DataType::DoublePrecision,
                    not_null: true,
                },
            ],
            vec!["id".into()],
            123,
            &source,
        )
        .unwrap();
        source.execute(&create, &[], None).unwrap();
        let table = source.tables["items"].clone();
        for id in 1..=4 {
            let values = vec![
                Value::Int(id),
                Value::Numeric(Numeric::parse(&format!("{id}.00")).unwrap()),
                Value::Float8(Float8::from_bits(if id % 2 == 0 {
                    0x8000000000000000
                } else {
                    0x7ff8000000001234
                })),
            ];
            let insert = Prepared::insert_engine(&table, values, 123, &source).unwrap();
            source.execute(&insert, &[], None).unwrap();
        }
        let other = Prepared::create_engine(
            "unrelated".into(),
            vec![ColumnDef {
                id: 0,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            vec!["id".into()],
            123,
            &source,
        )
        .unwrap();
        source.execute(&other, &[], None).unwrap();
        source.validate().unwrap();
        let rows = source.rows[&table.id]
            .iter()
            .filter(|(_, row)| row.version <= 2)
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect();
        (source, table, rows)
    }

    fn counters(source: &Relational) -> ProjectionCounters {
        source
            .projector_counters_engine(source.epoch, source.generation, &mut || false)
            .unwrap()
    }

    fn bytes<T: Serialize>(value: &T) -> Vec<u8> {
        bincode::serialize(value).unwrap()
    }

    #[test]
    fn preserves_original_counters_ids_versions_and_raw_numeric_float_values() {
        let (source, table, rows) = fixture();
        let before = bytes(&source);
        let counters = counters(&source);
        let scratch = ScratchRelation::reconstruct(
            &table,
            &rows,
            &counters,
            &ReconstructionLimits::default(),
            &mut || false,
        )
        .unwrap();
        assert_eq!(
            (
                scratch.relation.next_id,
                scratch.relation.next_version,
                scratch.relation.epoch,
                scratch.relation.generation
            ),
            (
                source.next_id,
                source.next_version,
                source.epoch,
                source.generation
            )
        );
        assert!(
            scratch.relation.next_id
                > table.columns.iter().map(|column| column.id).max().unwrap() + 1
        );
        assert!(
            scratch.relation.next_version > rows.values().map(|row| row.version).max().unwrap() + 1
        );
        assert_eq!(scratch.relation.tables.len(), 1);
        assert_eq!(scratch.relation.tables["items"], table);
        assert_eq!(bytes(&scratch.relation.rows[&table.id]), bytes(&rows));
        assert!(!scratch.relation.has_secondary_indexes_engine());
        for row in scratch.relation.rows[&table.id].values() {
            let Value::Numeric(amount) = &row.values[1] else {
                panic!("numeric")
            };
            assert!(amount.to_string().ends_with(".00"));
            let Value::Float8(bits) = row.values[2] else {
                panic!("float")
            };
            let Value::Int(id) = row.values[0] else {
                panic!("integer key")
            };
            assert_eq!(
                bits.to_bits(),
                if id % 2 == 0 {
                    0x8000000000000000
                } else {
                    0x7ff8000000001234
                }
            );
        }
        assert_eq!(bytes(&source), before);
    }

    #[test]
    fn consistency_fences_are_not_authenticated_same_source_provenance() {
        let (source, table, rows) = fixture();
        assert!(matches!(
            source.projector_counters_engine(source.epoch + 1, source.generation, &mut || false),
            Err(SqlError::StaleSchema)
        ));
        assert!(matches!(
            source.projector_counters_engine(source.epoch, source.generation + 1, &mut || false),
            Err(SqlError::StaleSchema)
        ));
        let counters = counters(&source);
        let mut alternate = rows.clone();
        alternate.values_mut().next().unwrap().values[1] =
            Value::Numeric(Numeric::parse("999.000").unwrap());
        // This is structurally consistent but violates the CALLER's same-source
        // obligation. The opaque token cannot and must not authenticate content.
        let scratch = ScratchRelation::reconstruct(
            &table,
            &alternate,
            &counters,
            &ReconstructionLimits::default(),
            &mut || false,
        )
        .unwrap();
        assert_eq!(bytes(&scratch.relation.rows[&table.id]), bytes(&alternate));
        assert_ne!(bytes(&alternate), bytes(&rows));
    }

    #[test]
    fn rejects_malformed_schema_keys_types_duplicate_ids_and_versions() {
        let (source, table, rows) = fixture();
        let counters = counters(&source);
        let original_table = bytes(&table);
        let original_rows = bytes(&rows);
        for mutation in 0..12 {
            let mut bad_table = table.clone();
            let mut bad_rows = rows.clone();
            match mutation {
                0 => bad_table.id = 0,
                1 => bad_table.columns[1].id = bad_table.columns[0].id,
                2 => bad_table.columns[0].id = bad_table.id,
                3 => bad_table.primary_key = vec![usize::MAX],
                4 => bad_table.primary_key = vec![0, 0],
                5 => bad_table.columns[0].not_null = false,
                6 => bad_table.name = "bad-name".into(),
                7 => {
                    let (mut key, row) = bad_rows.pop_first().unwrap();
                    key.push(0);
                    bad_rows.insert(key, row);
                }
                8 => {
                    bad_rows.values_mut().next().unwrap().values[1] =
                        Value::Text("wrong type".into())
                }
                9 => {
                    let version = bad_rows.values().next().unwrap().version;
                    for row in bad_rows.values_mut() {
                        row.version = version;
                    }
                }
                10 => bad_rows.values_mut().next().unwrap().version = 0,
                11 => bad_rows.values_mut().next().unwrap().version = counters.next_version,
                _ => unreachable!(),
            }
            assert!(
                ScratchRelation::reconstruct(
                    &bad_table,
                    &bad_rows,
                    &counters,
                    &ReconstructionLimits::default(),
                    &mut || false
                )
                .is_err(),
                "mutation {mutation}"
            );
        }
        assert_eq!(bytes(&table), original_table);
        assert_eq!(bytes(&rows), original_rows);
    }

    #[test]
    fn rejects_zero_exhausted_allocators_and_any_index_presence_without_stripping() {
        let (source, table, rows) = fixture();
        for (next_id, next_version) in [(0, 1), (1, 0), (u64::MAX, 1), (1, u64::MAX)] {
            let mut bad = source.clone();
            bad.next_id = next_id;
            bad.next_version = next_version;
            let before = bytes(&bad);
            assert!(
                bad.projector_counters_engine(bad.epoch, bad.generation, &mut || false)
                    .is_err()
            );
            let counters = ProjectionCounters {
                next_id,
                next_version,
                epoch: source.epoch,
                generation: source.generation,
            };
            assert!(
                ScratchRelation::reconstruct(
                    &table,
                    &rows,
                    &counters,
                    &ReconstructionLimits::default(),
                    &mut || false
                )
                .is_err()
            );
            assert_eq!(bytes(&bad), before);
        }
        let mut indexed = source.clone();
        indexed
            .secondary_configure_test(table.id, "by_id", &[table.columns[0].id], || false)
            .unwrap();
        let before = indexed.clone();
        assert!(matches!(
            indexed.projector_counters_engine(indexed.epoch, indexed.generation, &mut || false),
            Err(SqlError::Unsupported)
        ));
        assert_eq!(indexed, before);
    }

    #[test]
    fn exact_conservative_budget_boundaries_extreme_limits_and_checked_overflow() {
        let (source, table, rows) = fixture();
        let counters = counters(&source);
        let bound = preflight(&table, &rows, &ReconstructionLimits::default(), &mut || {
            false
        })
        .unwrap();
        let exact = ReconstructionLimits {
            rows: rows.len(),
            retained_bytes: bound.retained,
            scratch_bytes: bound.scratch,
            work: bound.work,
        };
        ScratchRelation::reconstruct(&table, &rows, &counters, &exact, &mut || false).unwrap();
        for dimension in 0..4 {
            let mut limits = ReconstructionLimits {
                rows: exact.rows,
                retained_bytes: exact.retained_bytes,
                scratch_bytes: exact.scratch_bytes,
                work: exact.work,
            };
            match dimension {
                0 => limits.rows -= 1,
                1 => limits.retained_bytes -= 1,
                2 => limits.scratch_bytes -= 1,
                3 => limits.work -= 1,
                _ => unreachable!(),
            }
            assert!(matches!(
                ScratchRelation::reconstruct(&table, &rows, &counters, &limits, &mut || false),
                Err(SqlError::Limit)
            ));
        }
        for extreme in [0, usize::MAX] {
            let limits = ReconstructionLimits {
                rows: extreme,
                retained_bytes: extreme,
                scratch_bytes: extreme,
                work: extreme,
            };
            assert!(matches!(
                ScratchRelation::reconstruct(&table, &rows, &counters, &limits, &mut || false),
                Err(SqlError::Limit)
            ));
        }
        let mut total = usize::MAX;
        assert_eq!(add(&mut total, 1, usize::MAX), Err(SqlError::Limit));
        assert_eq!(product(usize::MAX, 2), Err(SqlError::Limit));
    }

    #[test]
    fn pk_temporary_clones_encoding_and_numeric_formatting_are_admitted_first() {
        let mut source = Relational::default();
        let create = Prepared::create_engine(
            "wide".into(),
            vec![ColumnDef {
                id: 0,
                name: "id".into(),
                data_type: DataType::Text,
                not_null: true,
            }],
            vec!["id".into()],
            1,
            &source,
        )
        .unwrap();
        source.execute(&create, &[], None).unwrap();
        let table = source.tables["wide"].clone();
        let insert = Prepared::insert_engine(
            &table,
            vec![Value::Text("\0".repeat(MAX_ROW_BYTES))],
            1,
            &source,
        )
        .unwrap();
        source.execute(&insert, &[], None).unwrap();
        source.validate().unwrap();
        let rows = &source.rows[&table.id];
        let token = counters(&source);
        let bound = preflight(&table, rows, &ReconstructionLimits::default(), &mut || {
            false
        })
        .unwrap();
        assert!(bound.scratch >= rows.keys().next().unwrap().len() * 8);
        let limits = ReconstructionLimits {
            scratch_bytes: bound.scratch - 1,
            ..Default::default()
        };
        assert!(matches!(
            ScratchRelation::reconstruct(&table, rows, &token, &limits, &mut || false),
            Err(SqlError::Limit)
        ));
        let (numeric_source, table, rows) = fixture();
        let before = bytes(&rows);
        let limits = ReconstructionLimits {
            scratch_bytes: NUMERIC_SCRATCH - 1,
            ..Default::default()
        };
        assert!(matches!(
            ScratchRelation::reconstruct(
                &table,
                &rows,
                &counters(&numeric_source),
                &limits,
                &mut || false
            ),
            Err(SqlError::Limit)
        ));
        assert_eq!(bytes(&rows), before);
    }

    #[test]
    fn explicit_cancellation_is_cooperative_and_independent_of_ambient_scope() {
        let (source, table, rows) = fixture();
        let original = bytes(&source);
        let original_rows = bytes(&rows);
        let cancel = crate::Cancellation::default();
        assert!(cancel.cancel());
        cancel.scope(|| {
            let counters = counters(&source);
            let mut total = 0;
            let scratch = ScratchRelation::reconstruct(
                &table,
                &rows,
                &counters,
                &ReconstructionLimits::default(),
                &mut || {
                    total += 1;
                    false
                },
            )
            .unwrap();
            assert_eq!(bytes(&scratch.relation.rows[&table.id]), original_rows);
            assert!(total > rows.len() * 2);
            for stop in [1, total / 2, total - 2, total] {
                let mut current = 0;
                assert!(matches!(
                    ScratchRelation::reconstruct(
                        &table,
                        &rows,
                        &counters,
                        &ReconstructionLimits::default(),
                        &mut || {
                            current += 1;
                            current == stop
                        }
                    ),
                    Err(SqlError::Aborted)
                ));
            }
            assert!(matches!(
                source.projector_counters_engine(source.epoch, source.generation, &mut || true),
                Err(SqlError::Aborted)
            ));
            assert_eq!(
                crate::cancellation::check(),
                Err(crate::CoreError::Cancelled)
            );
        });
        assert_eq!(bytes(&source), original);
        assert_eq!(bytes(&rows), original_rows);
        assert_eq!(cancel.check(), Err(crate::CoreError::Cancelled));
    }

    #[test]
    fn numeric_primary_keys_reuse_canonical_order_without_rewriting_scale() {
        let mut source = Relational::default();
        let create = Prepared::create_engine(
            "numeric_keys".into(),
            vec![ColumnDef {
                id: 0,
                name: "id".into(),
                data_type: DataType::Numeric,
                not_null: true,
            }],
            vec!["id".into()],
            1,
            &source,
        )
        .unwrap();
        source.execute(&create, &[], None).unwrap();
        let table = source.tables["numeric_keys"].clone();
        for text in ["-10.00", "0.000", "1.20"] {
            let insert = Prepared::insert_engine(
                &table,
                vec![Value::Numeric(Numeric::parse(text).unwrap())],
                1,
                &source,
            )
            .unwrap();
            source.execute(&insert, &[], None).unwrap();
        }
        source.validate().unwrap();
        let rows = &source.rows[&table.id];
        let scratch = ScratchRelation::reconstruct(
            &table,
            rows,
            &counters(&source),
            &ReconstructionLimits::default(),
            &mut || false,
        )
        .unwrap();
        assert_eq!(bytes(&scratch.relation.rows[&table.id]), bytes(rows));
        let equivalent = encode_key(&[Value::Numeric(Numeric::parse("1.2").unwrap())]).unwrap();
        let Value::Numeric(original) = &scratch.relation.rows[&table.id][&equivalent].values[0]
        else {
            panic!("numeric")
        };
        assert_eq!(original.to_string(), "1.20");
    }

    #[test]
    fn hard_row_limit_accepts_n_and_rejects_n_plus_one_before_copy() {
        let mut source = Relational::default();
        let create = Prepared::create_engine(
            "bounded".into(),
            vec![ColumnDef {
                id: 0,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            vec!["id".into()],
            1,
            &source,
        )
        .unwrap();
        source.execute(&create, &[], None).unwrap();
        let table = source.tables["bounded"].clone();
        for id in 0..MAX_RECONSTRUCTION_ROWS {
            let insert =
                Prepared::insert_engine(&table, vec![Value::Int(id as i64)], 1, &source).unwrap();
            source.execute(&insert, &[], None).unwrap();
        }
        source.validate().unwrap();
        let scratch = ScratchRelation::reconstruct(
            &table,
            &source.rows[&table.id],
            &counters(&source),
            &ReconstructionLimits::default(),
            &mut || false,
        )
        .unwrap();
        assert_eq!(
            scratch.relation.rows[&table.id].len(),
            MAX_RECONSTRUCTION_ROWS
        );
        let insert = Prepared::insert_engine(
            &table,
            vec![Value::Int(MAX_RECONSTRUCTION_ROWS as i64)],
            1,
            &source,
        )
        .unwrap();
        source.execute(&insert, &[], None).unwrap();
        source.validate().unwrap();
        let before = bytes(&source);
        assert!(matches!(
            ScratchRelation::reconstruct(
                &table,
                &source.rows[&table.id],
                &counters(&source),
                &ReconstructionLimits::default(),
                &mut || false
            ),
            Err(SqlError::Limit)
        ));
        assert_eq!(bytes(&source), before);
    }

    #[test]
    fn rejects_every_unqualified_temporal_pk_and_value_before_allocation() {
        let (source, table, rows) = fixture();
        let counters = counters(&source);
        let time = PgTime::from_microseconds(0).unwrap();
        let temporal = [
            (DataType::Date, Value::Date(Date::from_days(0).unwrap())),
            (DataType::Time, Value::Time(time)),
            (
                DataType::TimeTz,
                Value::TimeTz(TimeTz::new(time, 0).unwrap()),
            ),
            (
                DataType::Timestamp,
                Value::Timestamp(Timestamp::from_microseconds(0).unwrap()),
            ),
            (
                DataType::TimestampTz,
                Value::TimestampTz(TimestampTz::from_microseconds(0).unwrap()),
            ),
            (DataType::Interval, Value::Interval(Interval::new(0, 0, 0))),
        ];
        for (data_type, value) in temporal {
            let mut bad_pk = table.clone();
            bad_pk.columns[0].data_type = data_type.clone();
            assert!(matches!(
                ScratchRelation::reconstruct(
                    &bad_pk,
                    &rows,
                    &counters,
                    &ReconstructionLimits::default(),
                    &mut || false
                ),
                Err(SqlError::Unsupported)
            ));
            let mut bad_table = table.clone();
            bad_table.columns[1].data_type = data_type;
            let mut bad_rows = rows.clone();
            bad_rows.values_mut().next().unwrap().values[1] = value;
            assert!(matches!(
                ScratchRelation::reconstruct(
                    &bad_table,
                    &bad_rows,
                    &counters,
                    &ReconstructionLimits::default(),
                    &mut || false
                ),
                Err(SqlError::Unsupported)
            ));
        }
    }
}
