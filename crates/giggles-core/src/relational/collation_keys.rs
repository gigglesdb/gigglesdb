//! Transient equality keys. Persisted row and secondary-index codecs stay byte based.
use super::*;
use std::sync::Arc;
use giggles_pg_types::temporal::{DateStyleSettings, SessionZone};

thread_local! {
    static DETAIL_PRESENTATION: std::cell::Cell<Option<(DateStyleSettings, SessionZone)>> =
        const { std::cell::Cell::new(None) };
}

/// Session settings for diagnostics from the synchronous DDL and DML helpers.
pub(crate) struct DetailPresentation(Option<(DateStyleSettings, SessionZone)>);

impl DetailPresentation {
    pub(crate) fn enter(style: DateStyleSettings, zone: Option<SessionZone>) -> Self {
        Self(DETAIL_PRESENTATION.replace(Some((style, zone.unwrap_or(SessionZone::Fixed(0))))))
    }
}

impl Drop for DetailPresentation {
    fn drop(&mut self) {
        DETAIL_PRESENTATION.set(self.0);
    }
}

pub(super) fn detail_text(value: &Value) -> Option<String> {
    let (style, zone) = DETAIL_PRESENTATION.get()
        .unwrap_or_else(|| (DateStyleSettings::default(), SessionZone::Fixed(0)));
    match value {
        Value::Date(date) => Some(date.format_with_style(style.style, style.order)),
        Value::Timestamp(time) => Some(time.format_with_style(style.style, style.order)),
        Value::TimestampTz(time) => time.format_with_style(zone, style.style, style.order).ok(),
        _ => query::output_text(value).ok().flatten(),
    }
}

pub(crate) struct Key {
    pub(crate) columns: Vec<usize>,
    collators: Vec<Option<Arc<giggles_icu::Collator>>>,
    arrays: Vec<bool>,
    predicate: Option<partial_index::BoundPredicate>,
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
        let arrays = columns.iter().map(|i| table.columns[*i].data_type == DataType::TextArray).collect();
        Ok(Self { columns, collators, arrays, predicate: None })
    }
    pub(crate) fn for_index(
        state: &Relational, table: &TableDef, index: &secondary_index::Descriptor,
    ) -> R<Self> {
        let mut key = Self::new(state, table, &index.columns, Some(index.id))?;
        key.predicate = index.predicate.as_ref().map(|p| p.bind(state, table)).transpose()?;
        Ok(key)
    }
    pub(crate) fn partial(&self) -> bool { self.predicate.is_some() }
    pub(crate) fn duplicate_detail(
        &self, table: &TableDef, name: &str, values: &[Value], build: bool,
    ) -> Option<SqlError> {
        detailed_duplicate(table, name, &self.columns, values, build, self.partial())
    }
    pub(crate) fn nondeterministic(&self) -> bool {
        self.collators.iter().any(Option::is_some)
    }
    pub(crate) fn tuple(&self, row: &[Value]) -> R<Option<Vec<u8>>> {
        if self.predicate.as_ref().map(|p| p.matches(row)).transpose()? == Some(false) {
            return Ok(None);
        }
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
        for ((value, collator), array) in values.iter().zip(&self.collators).zip(&self.arrays) {
            let value = equality_value(value, collator.as_deref(), *array)?;
            bytes = bytes
                .checked_add(value_bytes(&value))
                .ok_or(SqlError::Limit)?;
            if bytes > crate::relational::max_value_bytes() {
                return Err(SqlError::Limit);
            }
            transformed.push(value);
        }
        if transformed.iter().any(Value::is_temporal) {
            return secondary_index_adapter::encode_tuple(transformed.iter()).map(Some);
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
pub(crate) fn primary_has_uuid(table: &TableDef) -> bool {
    has_uuid_columns(table, &table.primary_key)
}

fn has_uuid_columns(table: &TableDef, positions: &[usize]) -> bool {
    positions.iter().any(|i| table.columns[*i].data_type == DataType::Uuid)
}

fn detailed_duplicate(
    table: &TableDef,
    name: &str,
    positions: &[usize],
    values: &[Value],
    build: bool,
    partial: bool,
) -> Option<SqlError> {
    if !build && !partial && !primary_has_uuid(table) && !has_uuid_columns(table, positions)
        && !positions.iter().any(|i| table.columns[*i].data_type.is_temporal())
    {
        return None;
    }
    key_duplicate(table, name, positions, values, build)
}

fn key_duplicate(
    table: &TableDef, name: &str, positions: &[usize], values: &[Value], build: bool,
) -> Option<SqlError> {
    let columns = positions.iter().map(|i| query::quote_identifier(&table.columns[*i].name))
        .collect::<Vec<_>>().join(", ");
    let values = positions.iter()
        .map(|i| detail_text(values.get(*i)?))
        .collect::<Option<Vec<_>>>()?.join(", ");
    let (schema, name) = split_key(name);
    let outcome = if build {
        user_types::view_replace_error(format!("could not create unique index \"{name}\""));
        "is duplicated."
    } else {
        duplicate(name);
        "already exists."
    };
    user_types::sql_duplicate_key_detail(DuplicateKeyDetail {
        table_id: table.id,
        schema: schema.to_owned(),
        table: table.name.clone(),
        constraint: name.to_owned(),
        values: format!("Key ({columns})=({values}) {outcome}"),
    });
    Some(SqlError::Duplicate)
}

pub(crate) fn index_duplicate(
    table: &TableDef,
    index: &secondary_index::Descriptor,
    values: &[Value],
    build: bool,
) -> SqlError {
    let positions = index.columns.iter().filter_map(|id| table.columns.iter()
        .position(|column| column.id == *id)).collect::<Vec<_>>();
    detailed_duplicate(table, &index.name, &positions, values, build, index.predicate.is_some())
        .unwrap_or(SqlError::Duplicate)
}

impl Relational {
    /// Check the bindings that could require equality keys or make Key::new
    /// fail before the sidecar validates descriptor columns.
    pub(super) fn needs_collated_unique_validation(
        &self,
        descriptors: &[secondary_index::Descriptor],
    ) -> bool {
        use column_collations::{Binding, Reference};
        let needs_key = |reference: &Reference| {
            matches!(reference, Reference::User(id)
                if self.ext.collations.get(id).is_none_or(|c| !c.deterministic))
        };
        let mut has_explicit_index_binding = false;
        for binding in self.ext.column_collations.values() {
            let required = match binding {
                Binding::Column(reference) => needs_key(reference),
                Binding::Index(references) => {
                    has_explicit_index_binding = true;
                    references.iter().flatten().any(needs_key)
                }
                Binding::EmptyTable => false,
            };
            if required {
                return true;
            }
        }
        has_explicit_index_binding
            && descriptors.iter().filter(|d| d.unique).any(|d| {
                matches!(self.ext.column_collations.get(&d.id), Some(Binding::Index(references))
                    if references.len() != d.columns.len())
            })
    }

    pub(super) fn primary_collision_name(&self, table: &TableDef) -> String {
        let name = self.primary_key_constraint_name(table).unwrap_or_default();
        if primary_has_uuid(table) {
            schema_binding::relation_key(self.table_schema(table.id), &name)
        } else {
            name
        }
    }

    pub(crate) fn primary_duplicate(&self, table: &TableDef, row: &[Value]) -> SqlError {
        if primary_has_uuid(table) {
            return key_duplicate(table, &self.primary_collision_name(table), &table.primary_key, row, false)
                .unwrap_or(SqlError::Duplicate);
        }
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
                        return Err(key.duplicate_detail(table, &name, &row.values, false)
                            .unwrap_or_else(|| duplicate(&name)));
                    }
                }
            }
        }
        Ok(())
    }

    // Recover key details when the legacy sidecar reports an unadorned collision.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn validate_index_details(
        &self,
        table: &TableDef,
        rows: &BTreeMap<Vec<u8>, SqlRow>,
        descriptors: &[secondary_index::Descriptor],
        build: bool,
        proposed: impl Fn(&[u8]) -> bool,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<()> {
        for index in descriptors.iter().filter(|d| d.unique && d.table == table.id) {
            if !build && index.predicate.is_none() && !primary_has_uuid(table)
                && !index.columns.iter().any(|id| {
                    table.columns.iter().any(|column| {
                        column.id == *id
                            && (column.data_type == DataType::Uuid || column.data_type.is_temporal())
                    })
                })
            {
                continue;
            }
            let key = Key::for_index(self, table, index)?;
            let mut occupied = BTreeSet::new();
            let mut bytes = 0usize;
            // Check surviving rows before proposals so DETAIL describes the
            // attempted value even if a proposed PK sorts before its witness.
            let candidates = rows.iter().filter(|(pk, _)| !proposed(pk))
                .chain(rows.iter().filter(|(pk, _)| proposed(pk)));
            for (_, row) in candidates {
                if cancel() {
                    return Err(SqlError::Aborted);
                }
                if let Some(tuple) = key.tuple(&row.values)? {
                    bytes = bytes.checked_add(tuple.len()).ok_or(SqlError::Limit)?;
                    if bytes > secondary_index::MAX_BYTES {
                        return Err(SqlError::Limit);
                    }
                    if !occupied.insert(tuple) {
                        return Err(key.duplicate_detail(table, &index.name, &row.values, build)
                            .unwrap_or(SqlError::Duplicate));
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
        let mut keys = Vec::new();
        let key = Key::new(self, table, &primary, None)?;
        if key.nondeterministic() {
            keys.push((
                key,
                self.primary_collision_name(table),
            ));
        }
        for d in descriptors
            .iter()
            .filter(|d| d.table == table.id && d.unique)
        {
            let key = Key::for_index(self, table, d)?;
            if key.nondeterministic() {
                keys.push((key, d.name.clone()));
            }
        }
        Ok(keys)
    }
}

/// Transient hash equality representation; deterministic collations use bytes.
pub(crate) fn equality_value(value: &Value, collator: Option<&giggles_icu::Collator>, array: bool) -> R<Value> {
    if array { logical_types::validate_text_array(value)?; }
    if array && let (Value::Text(text), Some(c)) = (value, collator) {
        // Length-delimited element keys preserve NULLs and array boundaries.
        let values = logical_types::array_values(text)?;
        let mut key = Vec::new();
        for value in values {
            match value {
                None => key.push(0),
                Some(v) => {
                    key.push(1);
                    let bytes = c.sort_key(v.as_bytes());
                    key.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
                    key.extend(bytes);
                }
            }
        }
        return Ok(Value::Bytes(key));
    }
    Ok(match (value,collator) {
        (Value::Text(text),Some(c)) => Value::Bytes(c.sort_key(text.as_bytes())),
        _ => value.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sql(s: &mut Relational, text: &str) {
        let p = Prepared::parse(text, 1, s.epoch).unwrap();
        s.execute(&p, &[], None).unwrap();
    }
    #[test]
    fn malformed_array_key_is_an_error() {
        let c = giggles_icu::Collator::open("und", None).unwrap();
        let error = equality_value(&Value::Text("{a,b".into()), Some(&c), true).unwrap_err();
        assert_eq!(crate::sqlstate(&crate::CoreError::Sql(error)), "22P02");
        assert_eq!(take_sql_error_message().as_deref(), Some("malformed array literal: \"{a,b\""));
        assert_eq!(equality_value(&Value::Int(1), Some(&c), true), Err(SqlError::InternalError));
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
        std::sync::Arc::make_mut(&mut byte_catalog.ext).column_collations.clear();
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
