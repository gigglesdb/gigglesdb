//! Transient equality keys. Persisted row and secondary-index codecs stay byte based.
use super::*;
use std::sync::Arc;

pub(crate) struct Key {
    pub(crate) columns: Vec<usize>,
    collators: Vec<Option<Arc<giggles_icu::Collator>>>,
}
impl Key {
    pub(crate) fn new(
        state: &Relational,
        table: &TableDef,
        ids: &[u64],
        index: Option<u64>,
    ) -> R<Self> {
        let explicit = index.and_then(|id| match state.ext.column_collations.get(&id) {
            Some(column_collations::Binding::Index(rs)) => Some(rs),
            _ => None,
        });
        if explicit.is_some_and(|rs| rs.len() != ids.len()) {
            return Err(SqlError::Type);
        }
        let mut columns = Vec::new();
        let mut collators = Vec::new();
        for (at, id) in ids.iter().enumerate() {
            columns.push(
                table
                    .columns
                    .iter()
                    .position(|c| c.id == *id)
                    .ok_or(SqlError::UnknownColumn)?,
            );
            let reference =
                explicit.map_or_else(|| state.column_collation(*id), |rs| rs[at].as_ref());
            let collator = match reference {
                Some(column_collations::Reference::User(id)) => {
                    let c = state.ext.collations.get(id).ok_or(SqlError::Type)?;
                    if c.deterministic {
                        None
                    } else {
                        Some(Arc::new(
                            giggles_icu::Collator::open(
                                c.locale.as_deref().or(c.collate.as_deref()).unwrap_or(""),
                                c.rules.as_deref(),
                            )
                            .map_err(|_| SqlError::Unsupported)?,
                        ))
                    }
                }
                _ => None,
            };
            collators.push(collator);
        }
        Ok(Self { columns, collators })
    }
    pub(crate) fn nondeterministic(&self) -> bool {
        self.collators.iter().any(Option::is_some)
    }
    pub(crate) fn tuple(&self, row: &[Value]) -> R<Option<Vec<u8>>> {
        self.values(
            &self
                .columns
                .iter()
                .map(|i| row[*i].clone())
                .collect::<Vec<_>>(),
        )
    }
    pub(crate) fn values(&self, values: &[Value]) -> R<Option<Vec<u8>>> {
        if values.len() != self.columns.len() {
            return Err(SqlError::Type);
        }
        if values.contains(&Value::Null) {
            return Ok(None);
        }
        let mut transformed = Vec::with_capacity(values.len());
        let mut bytes = 0usize;
        for (value, collator) in values.iter().zip(&self.collators) {
            let value = match (value, collator) {
                (Value::Text(text), Some(c)) => Value::Bytes(c.sort_key(text.as_bytes())),
                _ => value.clone(),
            };
            bytes = bytes
                .checked_add(value_bytes(&value))
                .ok_or(SqlError::Limit)?;
            if bytes > MAX_ROW_BYTES {
                return Err(SqlError::Limit);
            }
            transformed.push(value);
        }
        encode_key(&transformed).map(Some)
    }
}
pub(crate) fn duplicate(name: &str) -> SqlError {
    user_types::view_replace_error(format!(
        "duplicate key value violates unique constraint \"{}\"",
        split_key(name).1
    ));
    SqlError::Duplicate
}
impl Relational {
    pub(crate) fn primary_duplicate(&self, table: &TableDef) -> SqlError {
        if table
            .primary_key
            .iter()
            .any(|i| !self.deterministic_column(table.columns[*i].id))
        {
            duplicate(&self.primary_key_constraint_name(table).unwrap_or_default())
        } else {
            SqlError::Duplicate
        }
    }
    pub(crate) fn has_nondeterministic_keys(&self, table: &TableDef) -> bool {
        table.columns.iter().any(|c| !self.deterministic_column(c.id)) || self.secondary.descriptors().iter()
            .filter(|d| d.table == table.id).any(|d| {
                matches!(self.ext.column_collations.get(&d.id), Some(column_collations::Binding::Index(rs))
                    if rs.iter().flatten().any(|r| matches!(r,column_collations::Reference::User(id) if self.ext.collations.get(id).is_some_and(|c|!c.deterministic))))
            })
    }
    pub(crate) fn validate_collated_unique(
        &self,
        table: &TableDef,
        rows: &BTreeMap<Vec<u8>, SqlRow>,
        descriptors: &[secondary_index::Descriptor],
        cancel: &mut impl FnMut() -> bool,
    ) -> R<()> {
        for (key, name) in self.collated_unique_keys(table, descriptors)? {
            let mut occupied = BTreeSet::new();
            let mut bytes = 0usize;
            for row in rows.values() {
                if cancel() {
                    return Err(SqlError::Aborted);
                }
                if let Some(tuple) = key.tuple(&row.values)? {
                    bytes = bytes.checked_add(tuple.len()).ok_or(SqlError::Limit)?;
                    if bytes > MAX_RESULT_BYTES {
                        return Err(SqlError::Limit);
                    }
                    if !occupied.insert(tuple) {
                        return Err(duplicate(&name));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn collated_unique_keys(
        &self,
        table: &TableDef,
        descriptors: &[secondary_index::Descriptor],
    ) -> R<Vec<(Key, String)>> {
        let primary = table
            .primary_key
            .iter()
            .map(|i| table.columns[*i].id)
            .collect::<Vec<_>>();
        let mut keys = vec![(
            Key::new(self, table, &primary, None)?,
            self.primary_key_constraint_name(table).unwrap_or_default(),
        )];
        for d in descriptors
            .iter()
            .filter(|d| d.table == table.id && d.unique)
        {
            keys.push((
                Key::new(self, table, &d.columns, Some(d.id))?,
                d.name.clone(),
            ));
        }
        Ok(keys.into_iter().filter(|(key, _)| key.nondeterministic()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sql(s: &mut Relational, text: &str) {
        let p = Prepared::parse(text, 1, s.epoch).unwrap();
        s.execute(&p, &[], None).unwrap();
    }
    #[test]
    fn equality_keys_do_not_change_primary_or_secondary_wire_bytes() {
        let mut state = Relational::default();
        sql(
            &mut state,
            "CREATE COLLATION ci(provider=icu,locale='und-u-ks-level2',deterministic=false)",
        );
        sql(
            &mut state,
            "CREATE TABLE t(k text COLLATE ci PRIMARY KEY,v text COLLATE ci UNIQUE)",
        );
        sql(&mut state, "INSERT INTO t VALUES('abc','xyz')");
        let table = &state.tables["t"];
        let primary = encode_key(&[Value::Text("abc".into())]).unwrap();
        assert!(state.rows[&table.id].contains_key(&primary));
        let before = state.secondary_persistence_engine(|| false).unwrap();
        let d = &state.secondary.descriptors()[0];
        let key = Key::new(&state, table, &d.columns, Some(d.id)).unwrap();
        assert_eq!(
            key.values(&[Value::Text("xyz".into())]).unwrap(),
            key.values(&[Value::Text("XYZ".into())]).unwrap()
        );
        assert_eq!(
            state.secondary_persistence_engine(|| false).unwrap(),
            before
        );
        let mut byte_catalog = state.clone();
        byte_catalog.ext.column_collations.clear();
        // Namespace 8 contains only the unchanged byte tuples. No new codec revision.
        let raw_sidecar = byte_catalog
            .secondary_build_complete_engine(byte_catalog.secondary.descriptors(), || false)
            .unwrap();
        let raw = secondary_index::codec::encode(&raw_sidecar, || false).unwrap();
        assert_eq!(before.1, raw);
        assert_eq!(u32::from_le_bytes(raw[8..12].try_into().unwrap()), 2);
        let mut restored: Relational = bincode::deserialize(&before.0).unwrap();
        restored
            .secondary_restore_engine(
                secondary_index::codec::decode_pending(&before.1, || false).unwrap(),
                || false,
            )
            .unwrap();
        assert_eq!(restored, state);
        let d = &restored.secondary.descriptors()[0];
        let key = Key::new(&restored, &restored.tables["t"], &d.columns, Some(d.id)).unwrap();
        assert_eq!(
            key.values(&[Value::Text("xyz".into())]).unwrap(),
            key.values(&[Value::Text("XYZ".into())]).unwrap()
        );
    }
    #[test]
    fn deterministic_equality_and_preexisting_serialization_are_unchanged() {
        let mut state = Relational::default();
        sql(
            &mut state,
            "CREATE TABLE t(k text PRIMARY KEY,v text UNIQUE)",
        );
        sql(&mut state, "INSERT INTO t VALUES('abc','xyz')");
        let before = state.secondary_persistence_engine(|| false).unwrap();
        let t = &state.tables["t"];
        let key = Key::new(&state, t, &[t.columns[0].id], None).unwrap();
        assert!(!key.nondeterministic());
        assert_eq!(
            key.values(&[Value::Text("abc".into())]).unwrap(),
            Some(encode_key(&[Value::Text("abc".into())]).unwrap())
        );
        state
            .validate_collated_unique(
                t,
                &state.rows[&t.id],
                state.secondary.descriptors(),
                &mut || false,
            )
            .unwrap();
        assert_eq!(
            state.secondary_persistence_engine(|| false).unwrap(),
            before
        );
    }
}
