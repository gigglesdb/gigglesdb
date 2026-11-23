//! Primary-key installation uses the ordinary row-key codec and index rebuild.
use super::*;

impl Relational {
    pub(crate) fn primary_key_order(&self, table_id: u64) -> u64 {
        self.ext.primary_index_birth.get(&table_id).copied().unwrap_or(0)
    }
}

pub(super) fn primary_key_name(state: &Relational, table: &TableDef) -> Option<String> {
    if table.hidden_rowid().is_some() {
        return None;
    }
    Some(
        state
            .table_constraints(table.id)
            .iter()
            .find(|c| matches!(c.kind, ConstraintKind::PrimaryKey))
            .map(|c| c.name.clone())
            .unwrap_or_else(|| format!("{}_pkey", table.name)),
    )
}

pub(super) fn record_name(state: &mut Relational, table: &TableDef, name: &str) -> R<()> {
    if name == format!("{}_pkey", table.name) {
        return Ok(());
    }
    store_name(state, table, name)
}
fn store_name(state: &mut Relational, table: &TableDef, name: &str) -> R<()> {
    // The synthesized name is already logically occupied by this table.
    state.mutate_ext(|ext, next_id| {
        let id = allocate(next_id)?;
        ext.constraints
            .entry(table.id)
            .or_default()
            .push(Constraint {
                id,
                name: name.to_owned(),
                kind: ConstraintKind::PrimaryKey,
            });
        Ok(())
    })
}
pub(super) fn retain_name_before_rename(state: &mut Relational, table: &TableDef) -> R<()> {
    if table.hidden_rowid().is_none()
        && !state
            .table_constraints(table.id)
            .iter()
            .any(|c| matches!(c.kind, ConstraintKind::PrimaryKey))
    {
        store_name(state, table, &format!("{}_pkey", table.name))?;
    }
    Ok(())
}
pub(super) fn add(
    state: &mut Relational,
    table: &TableDef,
    explicit: Option<&str>,
    columns: &[String],
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let hidden = table
        .hidden_rowid()
        .ok_or(SqlError::InvalidTableDefinition)?;
    if columns.is_empty() {
        return Err(SqlError::Syntax);
    }
    let mut offsets = Vec::new();
    for name in columns {
        column_ident(name)?;
        let offset = col(table, name)?;
        if offset == hidden {
            return Err(SqlError::Reserved);
        }
        if offsets.contains(&offset) {
            return Err(SqlError::DuplicateColumn);
        }
        offsets.push(offset);
    }
    let name = match explicit {
        Some(name) => {
            ident::ident(name)?;
            name.to_owned()
        }
        None => state.choose_constraint_name(&state.table_key(table), &[], AutoName::Pkey),
    };
    if state.relation_name_taken(&relation_key(state.table_schema(table.id), &name)) {
        return Err(SqlError::DuplicateTable);
    }
    if state.constraint_name_taken(table.id, &name) {
        return Err(SqlError::DuplicateObject);
    }
    let mut candidate = state.clone();
    let mut keyed = table.clone();
    keyed.columns.remove(hidden);
    keyed.primary_key = offsets
        .iter()
        .map(|i| if *i > hidden { i - 1 } else { *i })
        .collect();
    for offset in &keyed.primary_key {
        keyed.columns[*offset].not_null = true;
    }
    let mut rows = BTreeMap::new();
    for row in state.rows.get(&table.id).ok_or(SqlError::Type)?.values() {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        let mut row = row.clone();
        row.values.remove(hidden);
        row_check(&keyed, &row.values)?;
        let key = pk(&keyed, &row.values)?;
        if rows.insert(key, row).is_some() {
            return Err(SqlError::Duplicate);
        }
    }
    let key = candidate.table_key(&keyed);
    std::sync::Arc::make_mut(&mut candidate.tables).insert(key, keyed.clone());
    candidate.rows.insert(keyed.id, rows);
    // Missing births mean the PK precedes every secondary descriptor.
    if candidate.secondary_descriptors_engine().iter()
        .any(|d| d.table == table.id && d.unique)
    {
        candidate.mutate_ext(|ext, next_id| {
            ext.primary_index_birth.insert(table.id, allocate(next_id)?);
            Ok(())
        })?;
    }
    record_name(&mut candidate, &keyed, &name)?;
    let descriptors = candidate.secondary_descriptors_engine().to_vec();
    candidate.secondary = candidate.secondary_build_complete_engine(&descriptors, &mut *cancel)?;
    if cancel() {
        return Err(SqlError::Aborted);
    }
    *state = candidate;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::Options;
    fn run(state: &mut Relational, sql: &str) -> R<SqlResult> {
        let p = Prepared::parse(sql, 1, state.epoch)?;
        state.execute(&p, &[], None)
    }
    #[test]
    fn birth_free_creates_preserve_catalog_bytes() {
        for sql in [
            "CREATE TABLE t(a int PRIMARY KEY)",
            "CREATE TABLE t(a int PRIMARY KEY,UNIQUE(a))",
            "CREATE TABLE t(a int PRIMARY KEY,b int UNIQUE)",
        ] {
            let mut state = Relational::default();
            run(&mut state, sql).unwrap();
            let before = bincode::serialize(state.ext.as_ref()).unwrap();
            let version = state.ext.wire_version();
            run(&mut state, "CREATE UNIQUE INDEX s ON t(a)").unwrap();
            assert_eq!(state.primary_key_order(state.tables["t"].id), 0);
            assert_eq!(bincode::serialize(state.ext.as_ref()).unwrap(), before);
            assert_eq!(state.ext.wire_version(), version);
            assert!(!state.ext.has_key_dependency_identity()); // feature bit 17
        }
    }

    #[test]
    fn late_cancel_does_not_publish_birth() {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE t(a int)").unwrap();
        run(&mut state, "CREATE UNIQUE INDEX s ON t(a)").unwrap();
        let before = state.clone();
        let table = state.tables["t"].clone();
        let mut polls = 0;
        assert_eq!(add(&mut state, &table, None, &["a".into()], &mut || {
            polls += 1;
            polls > 1
        }), Err(SqlError::Aborted));
        assert_eq!(state, before);
    }

    #[test]
    fn heap_rekey_rebuilds_indexes_and_reopens() {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE heap(a int,b int)").unwrap();
        run(&mut state, "INSERT INTO heap VALUES(2,20),(1,10)").unwrap();
        run(&mut state, "CREATE INDEX heap_b ON heap(b)").unwrap();
        let table = state.tables["heap"].clone();
        add(&mut state, &table, None, &["a".into()], &mut || false).unwrap();
        let table = &state.tables["heap"];
        assert_eq!(table.hidden_rowid(), None);
        assert_eq!(table.columns.len(), 2);
        assert!(table.columns[0].not_null);
        assert!(state.ext.constraints.is_empty());
        for (key, row) in &state.rows[&table.id] {
            assert_eq!(*key, pk(table, &row.values).unwrap());
        }
        let (base, indexes) = state.secondary_persistence_engine(|| false).unwrap();
        let mut reopened: Relational = bincode::deserialize(&base).unwrap();
        reopened
            .secondary_restore_engine(
                secondary_index::codec::decode_pending(&indexes, || false).unwrap(),
                || false,
            )
            .unwrap();
        assert_eq!(reopened, state);
        assert_eq!(
            run(&mut reopened, "SELECT a FROM heap WHERE b=20 LIMIT 10")
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(2)]
        );
        let table = reopened.tables["heap"].clone();
        assert_eq!(
            add(&mut reopened, &table, None, &["b".into()], &mut || false),
            Err(SqlError::InvalidTableDefinition)
        );
    }
    #[test]
    fn heap_rekey_failure_and_cancellation_are_atomic() {
        for (values, error) in [("(1),(1)", SqlError::Duplicate), ("(NULL)", SqlError::Null)] {
            let mut state = Relational::default();
            run(&mut state, "CREATE TABLE heap(a int)").unwrap();
            run(&mut state, &format!("INSERT INTO heap VALUES {values}")).unwrap();
            let table = state.tables["heap"].clone();
            let before = state.clone();
            assert_eq!(
                add(&mut state, &table, None, &["a".into()], &mut || false),
                Err(error)
            );
            assert_eq!(state, before);
            assert_eq!(
                add(&mut state, &table, None, &["a".into()], &mut || true),
                Err(SqlError::Aborted)
            );
            assert_eq!(state, before);
        }
    }
    #[test]
    fn primary_key_name_collision_and_rename_retention() {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE heap(a int)").unwrap();
        run(&mut state, "CREATE TABLE heap_pkey(a int PRIMARY KEY)").unwrap();
        let table = state.tables["heap"].clone();
        add(&mut state, &table, None, &["a".into()], &mut || false).unwrap();
        assert_eq!(
            primary_key_name(&state, &state.tables["heap"]),
            Some("heap_pkey1".into())
        );
        run(&mut state, "CREATE TABLE oldname(a int PRIMARY KEY)").unwrap();
        let mut table = state.tables["oldname"].clone();
        retain_name_before_rename(&mut state, &table).unwrap();
        std::sync::Arc::make_mut(&mut state.tables).remove("oldname");
        table.name = "newname".into();
        std::sync::Arc::make_mut(&mut state.tables).insert(table.name.clone(), table.clone());
        assert_eq!(
            primary_key_name(&state, &table),
            Some("oldname_pkey".into())
        );
    }

    #[test]
    fn plain_create_keeps_legacy_bytes_and_sql_rename_retains_name() {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE oldname(a int PRIMARY KEY)").unwrap();
        let codec = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian();
        // The seven-field pre-extension representation is frozen. In particular,
        // the ordinary synthesized PK name consumes neither an id nor ext bytes.
        let legacy = codec
            .serialize(&(
                1u32,
                state.generation,
                state.epoch,
                state.next_id,
                state.next_version,
                state.tables.as_ref(),
                &state.rows,
            ))
            .unwrap();
        assert_eq!(codec.serialize(&state).unwrap(), legacy);
        assert!(state.ext.is_empty());
        run(&mut state, "ALTER TABLE oldname RENAME TO newname").unwrap();
        assert_eq!(
            primary_key_name(&state, &state.tables["newname"]),
            Some("oldname_pkey".into())
        );
        let bytes = codec.serialize(&state).unwrap();
        let reopened: Relational = codec.deserialize(&bytes).unwrap();
        assert_eq!(reopened, state);
        assert_eq!(
            primary_key_name(&reopened, &reopened.tables["newname"]),
            Some("oldname_pkey".into())
        );
    }
}
