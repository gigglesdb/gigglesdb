//! Trusted, read-only projection input. This is neither authority nor a projector.
use super::*;

fn tick(cancel: &mut impl FnMut() -> bool) -> R<()> {
    if cancel() {
        Err(SqlError::Aborted)
    } else {
        Ok(())
    }
}

impl Relational {
    // Admit bounded catalog metadata before indexing it. Never trust a supplied
    // descriptor, name, or PK offset. No row traversal is needed here.
    fn projector_table(&self, table_id: u64, cancel: &mut impl FnMut() -> bool) -> R<&TableDef> {
        tick(cancel)?;
        if self.format != 1 {
            return Err(SqlError::Unsupported);
        }
        if self.tables.len() > MAX_TABLES || self.rows.len() != self.tables.len() {
            return Err(SqlError::Type);
        }
        let mut found = None;
        for (name, table) in &self.tables {
            tick(cancel)?;
            if table.id != table_id {
                continue;
            }
            if found.is_some() {
                return Err(SqlError::StaleSchema);
            }
            ident(name)?;
            if name != &table.name || table.id == 0 || table.id >= self.next_id {
                return Err(SqlError::StaleSchema);
            }
            if table.columns.is_empty()
                || table.columns.len() > MAX_COLUMNS
                || table.primary_key.is_empty()
                || table.primary_key.len() > MAX_COLUMNS
            {
                return Err(SqlError::Type);
            }
            unique(table.primary_key.iter())?;
            let mut ids = BTreeSet::from([table.id]);
            for column in &table.columns {
                tick(cancel)?;
                ident(&column.name)?;
                if column.id == 0 || column.id >= self.next_id || !ids.insert(column.id) {
                    return Err(SqlError::Type);
                }
            }
            for index in &table.primary_key {
                if table.columns.get(*index).is_none_or(|c| !c.not_null) {
                    return Err(SqlError::Null);
                }
            }
            unique(table.columns.iter().map(|c| &c.name))?;
            if !self.rows.contains_key(&table.id) {
                return Err(SqlError::Type);
            }
            found = Some(table);
        }
        found.ok_or(SqlError::UnknownTable)
    }

    /// Borrow the authoritative current descriptor and COMPLETE actual row map.
    ///
    /// Caller proof: this relation came from validated State decoding/construction,
    /// and subsequent changes preserved core invariants. This method checks the
    /// selected catalog binding, not all rows or unrelated tables. It must never
    /// be used to bless unchecked deserialized State. No SELECT/page cap applies.
    /// Before copying, the callback must admit map/row/value lengths, output bytes
    /// and work to its own finite budget, and cooperatively check cancellation.
    /// Only bounded catalog work occurs before entry. Cancellation is checked
    /// before disclosure and after successful callback completion; callback errors
    /// propagate unchanged. Nothing can mutate this relation through these borrows.
    /// Only the explicit `cancel` callback is consulted; ambient cancellation is
    /// not read. Callers wanting it must pass `|| crate::cancellation::check().is_err()`.
    /// Cancellation cannot undo external callback side effects: keep it pure.
    #[allow(dead_code)] // Released internal seam; runtime integration is separate.
    pub(crate) fn with_complete_table_engine<T>(
        &self,
        table_id: u64,
        mut cancel: impl FnMut() -> bool,
        visit: impl FnOnce(&TableDef, &BTreeMap<Vec<u8>, SqlRow>) -> R<T>,
    ) -> R<T> {
        let table = self.projector_table(table_id, &mut cancel)?;
        let rows = self.rows.get(&table.id).ok_or(SqlError::Type)?;
        tick(&mut cancel)?;
        let result = visit(table, rows)?;
        tick(&mut cancel)?;
        Ok(result)
    }

    /// Bind borrowed typed PK values against the current stable-ID descriptor.
    /// Uses ordinary core arity/type/null/payload and canonical encoding semantics,
    /// including Numeric and floats; no secondary-index support whitelist.
    /// Same validated-State caller proof and explicit-callback-only cancellation
    /// contract as `with_complete_table_engine`.
    #[allow(dead_code)]
    pub(crate) fn canonical_pk_engine(
        &self,
        table_id: u64,
        values: &[Value],
        mut cancel: impl FnMut() -> bool,
    ) -> R<Vec<u8>> {
        let table = self.projector_table(table_id, &mut cancel)?;
        if values.len() != table.primary_key.len() {
            return Err(SqlError::Unbounded);
        }
        payload_bound(values.iter())?;
        for (value, index) in values.iter().zip(&table.primary_key) {
            tick(&mut cancel)?;
            check(value, &table.columns[*index])?;
        }
        tick(&mut cancel)?;
        let key = encode_key(values)?;
        tick(&mut cancel)?;
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(data_type: DataType) -> Relational {
        let table = TableDef {
            id: 1,
            name: "items".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "id".into(),
                data_type,
                not_null: true,
            }],
            primary_key: vec![0],
        };
        Relational {
            next_id: 3,
            tables: BTreeMap::from([("items".into(), table)]),
            rows: BTreeMap::from([(1, BTreeMap::new())]),
            ..Relational::default()
        }
    }

    #[test]
    fn complete_borrow_preserves_identity_versions_and_state() {
        let mut state = fixture(DataType::BigInt);
        for n in 0..1025 {
            let values = vec![Value::Int(n)];
            let key = pk(&state.tables["items"], &values).unwrap();
            state.rows.get_mut(&1).unwrap().insert(
                key,
                SqlRow {
                    values,
                    version: n as u64 + 1,
                },
            );
        }
        state.next_version = 1026;
        state.validate().unwrap();
        let before = state.clone();
        state
            .with_complete_table_engine(
                1,
                || false,
                |table, rows| {
                    assert!(std::ptr::eq(table, &state.tables["items"]));
                    assert!(std::ptr::eq(rows, &state.rows[&1]));
                    assert_eq!(rows.len(), MAX_ROWS + 1);
                    assert_eq!(rows.values().map(|r| r.version).max(), Some(1025));
                    assert_eq!(table.id, 1);
                    Ok(())
                },
            )
            .unwrap();
        let page = Prepared::parse("SELECT id FROM items LIMIT 1024", 1, state.epoch).unwrap();
        assert_eq!(
            state.execute(&page, &[], None).unwrap().rows.len(),
            MAX_ROWS
        );
        assert_eq!(state, before);
        assert_eq!(
            state.with_complete_table_engine(1, || false, |_, _| Err::<(), _>(SqlError::Conflict)),
            Err(SqlError::Conflict)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn rejects_before_disclosure() {
        let mut state = fixture(DataType::BigInt);
        let never = |_: &TableDef, _: &BTreeMap<Vec<u8>, SqlRow>| -> R<()> { panic!("disclosed") };
        assert_eq!(
            state.with_complete_table_engine(0, || false, never),
            Err(SqlError::UnknownTable)
        );
        assert_eq!(
            state.with_complete_table_engine(999, || false, never),
            Err(SqlError::UnknownTable)
        );
        assert_eq!(
            state.with_complete_table_engine(1, || true, never),
            Err(SqlError::Aborted)
        );
        let cancellation = crate::cancellation::Cancellation::default();
        cancellation.cancel();
        assert_eq!(
            cancellation.scope(|| {
                state.with_complete_table_engine(1, || crate::cancellation::check().is_err(), never)
            }),
            Err(SqlError::Aborted)
        );
        state.tables.get_mut("items").unwrap().primary_key = vec![99];
        assert_eq!(
            state.with_complete_table_engine(1, || false, never),
            Err(SqlError::Null)
        );
        state.tables.get_mut("items").unwrap().primary_key = vec![0];
        state.tables.get_mut("items").unwrap().name = "other".into();
        assert_eq!(
            state.with_complete_table_engine(1, || false, never),
            Err(SqlError::StaleSchema)
        );
    }

    #[test]
    fn explicit_cancellation_is_independent_of_ambient_scope() {
        let mut state = fixture(DataType::BigInt);
        let values = vec![Value::Int(7)];
        let expected_key = pk(&state.tables["items"], &values).unwrap();
        state.rows.get_mut(&1).unwrap().insert(
            expected_key.clone(),
            SqlRow {
                values: values.clone(),
                version: 1,
            },
        );
        state.next_version = 2;
        state.validate().unwrap();
        let before = state.clone();
        let cancellation = crate::cancellation::Cancellation::default();
        assert!(cancellation.cancel());
        cancellation.scope(|| {
            assert!(crate::cancellation::check().is_err());
            assert_eq!(
                state.with_complete_table_engine(
                    1,
                    || false,
                    |table, rows| {
                        assert!(std::ptr::eq(table, &state.tables["items"]));
                        assert!(std::ptr::eq(rows, &state.rows[&1]));
                        assert_eq!(rows[&expected_key].values, values);
                        assert_eq!(rows[&expected_key].version, 1);
                        Ok(rows.len())
                    }
                ),
                Ok(1)
            );
            assert_eq!(
                state.canonical_pk_engine(1, &values, || false),
                Ok(expected_key.clone())
            );
            assert_eq!(state, before);
            assert_eq!(
                state.with_complete_table_engine(1, || true, |_, _| panic!("disclosed")),
                Err::<(), _>(SqlError::Aborted)
            );
            // Cancellation wins even over invalid PK input, before key disclosure.
            assert_eq!(
                state.canonical_pk_engine(1, &[], || true),
                Err(SqlError::Aborted)
            );
            assert_eq!(
                state.canonical_pk_engine(1, &values, || true),
                Err(SqlError::Aborted)
            );
            assert_eq!(
                state.with_complete_table_engine(
                    1,
                    || crate::cancellation::check().is_err(),
                    |_, _| panic!("disclosed")
                ),
                Err::<(), _>(SqlError::Aborted)
            );
            assert_eq!(
                state.canonical_pk_engine(1, &values, || crate::cancellation::check().is_err()),
                Err(SqlError::Aborted)
            );
            assert_eq!(state, before);
            // The seam never resets or suppresses the ambient context.
            assert!(crate::cancellation::check().is_err());
        });
        assert_eq!(state, before);
    }

    #[test]
    fn pk_admission_and_existing_canonical_equality() {
        let state = fixture(DataType::BigInt);
        assert_eq!(
            state.canonical_pk_engine(1, &[], || false),
            Err(SqlError::Unbounded)
        );
        assert_eq!(
            state.canonical_pk_engine(1, &[Value::Null], || false),
            Err(SqlError::Null)
        );
        assert_eq!(
            state.canonical_pk_engine(1, &[Value::Bool(true)], || false),
            Err(SqlError::Type)
        );
        let text = fixture(DataType::Text);
        assert_eq!(
            text.canonical_pk_engine(1, &[Value::Text("x".repeat(MAX_ROW_BYTES + 1))], || false),
            Err(SqlError::Limit)
        );
        let cases = [
            (
                DataType::Numeric,
                Value::Numeric(Numeric::parse("1.00").unwrap()),
                Value::Numeric(Numeric::parse("1").unwrap()),
            ),
            (
                DataType::Real,
                Value::Float4(Float4::from_bits((-0.0f32).to_bits())),
                Value::Float4(Float4::from_bits(0.0f32.to_bits())),
            ),
            (
                DataType::DoublePrecision,
                Value::Float8(Float8::from_bits(f64::NAN.to_bits())),
                Value::Float8(Float8::from_bits((-f64::NAN).to_bits())),
            ),
        ];
        for (ty, a, b) in cases {
            let mut state = fixture(ty);
            let table = &state.tables["items"];
            let ka = state
                .canonical_pk_engine(1, std::slice::from_ref(&a), || false)
                .unwrap();
            assert_eq!(ka, pk(table, std::slice::from_ref(&a)).unwrap());
            let insert = Prepared::insert_engine(table, vec![a], 1, &state).unwrap();
            state.execute(&insert, &[], None).unwrap();
            assert!(state.rows[&1].contains_key(&ka));
            assert_eq!(ka, state.canonical_pk_engine(1, &[b], || false).unwrap());
        }
    }

    #[test]
    fn current_binding_and_post_callback_cancellation() {
        let mut state = fixture(DataType::BigInt);
        assert!(
            state
                .canonical_pk_engine(1, &[Value::Int(7)], || false)
                .is_ok()
        );
        state.tables.get_mut("items").unwrap().columns[0].data_type = DataType::Text;
        assert_eq!(
            state.canonical_pk_engine(1, &[Value::Int(7)], || false),
            Err(SqlError::Type)
        );
        let before = state.clone();
        let cancelled = std::cell::Cell::new(false);
        assert_eq!(
            state.with_complete_table_engine(
                1,
                || cancelled.get(),
                |_, _| {
                    cancelled.set(true);
                    Ok(7)
                }
            ),
            Err(SqlError::Aborted)
        );
        assert_eq!(state, before);
        let duplicate = state.tables["items"].clone();
        state.tables.insert("duplicate".into(), duplicate);
        state.rows.insert(99, BTreeMap::new());
        assert_eq!(
            state.with_complete_table_engine(1, || false, |_, _| panic!("disclosed")),
            Err::<(), _>(SqlError::StaleSchema)
        );
        let mut missing = fixture(DataType::BigInt);
        missing.rows.clear();
        missing.rows.insert(99, BTreeMap::new());
        assert_eq!(
            missing.with_complete_table_engine(1, || false, |_, _| panic!("disclosed")),
            Err::<(), _>(SqlError::Type)
        );
    }
}
