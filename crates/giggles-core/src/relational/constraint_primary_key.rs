//! Primary-key installation uses the ordinary row-key codec and index rebuild.
use super::*;

impl Relational {
    pub(crate) fn primary_key_order(&self, table_id: u64) -> u64 {
        self.ext
            .primary_index_birth
            .get(&table_id)
            .copied()
            .unwrap_or(0)
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
    let hidden = table.hidden_rowid().ok_or_else(|| {
        user_types::view_replace_error(format!(
            "multiple primary keys for table \"{}\" are not allowed",
            table.name
        ));
        SqlError::InvalidTableDefinition
    })?;
    if columns.is_empty() {
        return Err(SqlError::Syntax);
    }
    let mut offsets = Vec::new();
    for name in columns {
        column_ident(name)?;
        let offset = col(table, name)?;
        generated_columns::check_virtual_primary(state, &table.columns[offset])?;
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
    if sql_index::name_taken(state, &relation_key(state.table_schema(table.id), &name)) {
        return Err(sql_index::duplicate_relation(&name));
    }
    if state.constraint_name_taken(table.id, &name) {
        return Err(constraints::duplicate_constraint(table, &name));
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
        for &offset in &keyed.primary_key {
            if row.values[offset] == Value::Null {
                user_types::view_replace_error(format!(
                    "column \"{}\" of relation \"{}\" contains null values",
                    keyed.columns[offset].name, table.name
                ));
                return Err(SqlError::Null);
            }
        }
        row_check(&keyed, &row.values)?;
        let key = pk(&keyed, &row.values)?;
        if let std::collections::btree_map::Entry::Vacant(slot) = rows.entry(key) {
            slot.insert(row);
        } else {
            user_types::view_replace_error(format!("could not create unique index \"{name}\""));
            let detail = key_detail(&keyed, &row.values, "is duplicated")?;
            user_types::sql_error_detail(detail);
            return Err(SqlError::Duplicate);
        }
    }
    let key = candidate.table_key(&keyed);
    std::sync::Arc::make_mut(&mut candidate.tables).insert(key, keyed.clone());
    candidate.rows.insert(keyed.id, rows);
    // Missing births mean the PK precedes every secondary descriptor.
    if candidate
        .secondary_descriptors_engine()
        .iter()
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

pub(super) fn key_detail(table: &TableDef, row: &[Value], message: &str) -> R<String> {
    let columns = table
        .primary_key
        .iter()
        .map(|&i| table.columns[i].name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let values = table
        .primary_key
        .iter()
        .map(|&i| query::output_text(&row[i]).map(|text| text.unwrap_or_else(|| "null".into())))
        .collect::<R<Vec<_>>>()?
        .join(", ");
    Ok(format!("Key ({columns})=({values}) {message}."))
}

pub(super) fn drop(
    state: &mut Relational,
    table: &TableDef,
    name: &str,
    cascade: bool,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    if table.columns.len() >= MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let columns: BTreeSet<_> = table
        .primary_key
        .iter()
        .map(|&i| table.columns[i].id)
        .collect();
    let mut dependents = Vec::new();
    for (&child, constraints) in &state.ext.constraints {
        for constraint in constraints {
            if let ConstraintKind::ForeignKey {
                ref_table,
                ref_columns,
                target,
                ..
            } = &constraint.kind
                && *ref_table == table.id
                && (*target == ReferencedKey::PrimaryKey
                    || *target == ReferencedKey::Unknown
                        && ref_columns.iter().copied().collect::<BTreeSet<_>>() == columns)
            {
                if cascade && *target == ReferencedKey::Unknown
                    && state.secondary_descriptors_engine().iter().any(|index| {
                        index.table == table.id && index.unique
                            && index.columns.iter().copied().collect::<BTreeSet<_>>() == columns
                    })
                {
                    user_types::view_replace_error(format!(
                        "cannot determine which index supports foreign key \"{}\"", constraint.name
                    ));
                    return Err(SqlError::Unsupported);
                }
                let child = state
                    .tables
                    .values()
                    .find(|t| t.id == child)
                    .ok_or(SqlError::Type)?;
                dependents.push((child, constraint));
            }
        }
    }
    dependents.sort_by_key(|(_, constraint)| constraint.id);
    if !dependents.is_empty() && !cascade {
        user_types::view_replace_error(format!(
            "cannot drop constraint {} on table \"{}\" because other objects depend on it",
            name, table.name
        ));
        user_types::sql_error_detail(
            dependents
                .iter()
                .map(|(child, constraint)| {
                    format!(
                        "constraint {} on table \"{}\" depends on index \"{name}\"",
                        constraint.name, child.name
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"),
        );
        user_types::sql_error_hint("Use DROP ... CASCADE to drop the dependent objects too.");
        return Err(SqlError::DependentObjects);
    }
    let notices = dependents
        .iter()
        .map(|(child, constraint)| {
            format!(
                "drop cascades to constraint {} on table \"{}\"",
                constraint.name, child.name
            )
        })
        .collect::<Vec<_>>();
    let ids = dependents
        .iter()
        .map(|(_, constraint)| constraint.id)
        .collect::<BTreeSet<_>>();
    let mut heap = table.clone();
    heap.primary_key = vec![heap.columns.len()];
    heap.columns.push(ColumnDef {
        id: allocate(&mut state.next_id)?,
        name: HIDDEN_ROWID.into(),
        data_type: DataType::BigInt,
        not_null: true,
    });
    let ext = Arc::make_mut(&mut state.ext);
    ext.primary_index_birth.remove(&table.id);
    for (&owner, constraints) in &mut ext.constraints {
        constraints.retain(|c| {
            !(owner == table.id && matches!(c.kind, ConstraintKind::PrimaryKey))
                && !ids.contains(&c.id)
        });
    }
    ext.constraints
        .retain(|_, constraints| !constraints.is_empty());
    Relational::normalize_ext(ext);
    let old = state.rows.remove(&table.id).ok_or(SqlError::Type)?;
    let mut rows = BTreeMap::new();
    for (i, mut row) in old.into_values().enumerate() {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        // Both SQL and native allocators assign positive IDs to future inserts.
        let rowid = -i64::try_from(i + 1).map_err(|_| SqlError::Limit)?;
        row.values.push(Value::Int(rowid));
        row_check(&heap, &row.values)?;
        rows.insert(pk(&heap, &row.values)?, row);
    }
    state.rows.insert(heap.id, rows);
    let key = state.table_key(&heap);
    Arc::make_mut(&mut state.tables).insert(key, heap);
    if notices.len() == 1 {
        user_types::sql_notice(notices.into_iter().next().unwrap(), "00000");
    } else if !notices.is_empty() {
        user_types::sql_notice_detail(
            format!("drop cascades to {} other objects", notices.len()),
            notices.join("\n"),
            "00000",
        );
    }
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
        assert_eq!(
            add(&mut state, &table, None, &["a".into()], &mut || {
                polls += 1;
                polls > 1
            }),
            Err(SqlError::Aborted)
        );
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
    #[test]
    fn drop_named_primary_key_preserves_rows_column_flags_and_secondary_indexes() {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE old_table (a int, b int, token text UNIQUE, CONSTRAINT original_key PRIMARY KEY(a,b))",
            "CREATE INDEX by_a ON old_table(a)",
            "INSERT INTO old_table VALUES (1,2,'t1'),(2,2,'t2')",
            "ALTER TABLE old_table RENAME TO tokens",
            "ALTER TABLE tokens DROP CONSTRAINT original_key",
            "ALTER TABLE tokens DROP CONSTRAINT IF EXISTS original_key",
            "INSERT INTO tokens VALUES (1,2,'t3')",
        ] {
            run(&mut state, sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        let table = &state.tables["tokens"];
        assert!(table.columns[0].not_null && table.columns[1].not_null);
        assert!(table.hidden_rowid().is_some());
        assert_eq!(primary_key_name(&state, table), None);
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
            reopened.secondary_persistence_engine(|| false).unwrap(),
            (base, indexes)
        );
        assert_eq!(
            run(
                &mut reopened,
                "SELECT token FROM tokens WHERE a=1 ORDER BY token"
            )
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
            [Value::Text("t1".into()), Value::Text("t3".into())]
        );
        assert_eq!(
            run(&mut reopened, "INSERT INTO tokens VALUES (1,2,'t2')"),
            Err(SqlError::Duplicate)
        );
        assert_eq!(
            run(&mut reopened, "INSERT INTO tokens VALUES (NULL,2,'t4')"),
            Err(SqlError::Null)
        );
        let before = reopened.clone();
        assert_eq!(
            run(
                &mut reopened,
                "ALTER TABLE tokens ADD CONSTRAINT next_pk PRIMARY KEY(a,b)"
            ),
            Err(SqlError::Duplicate)
        );
        assert_eq!(reopened, before);
        assert_eq!(
            user_types::take_sql_error_message().as_deref(),
            Some("could not create unique index \"next_pk\"")
        );
        assert_eq!(
            user_types::take_sql_error_detail().as_deref(),
            Some("Key (a, b)=(1, 2) is duplicated.")
        );
        run(
            &mut reopened,
            "ALTER TABLE tokens ADD CONSTRAINT next_pk PRIMARY KEY(token)",
        )
        .unwrap();
        assert_eq!(
            primary_key_name(&reopened, &reopened.tables["tokens"]),
            Some("next_pk".into())
        );
        assert_eq!(
            run(
                &mut reopened,
                "ALTER TABLE tokens ADD CONSTRAINT another_pk PRIMARY KEY(a)"
            ),
            Err(SqlError::InvalidTableDefinition)
        );
        assert_eq!(
            user_types::take_sql_error_message().as_deref(),
            Some("multiple primary keys for table \"tokens\" are not allowed")
        );
        assert_eq!(
            run(
                &mut reopened,
                "ALTER TABLE tokens DROP CONSTRAINT nope_pkey"
            ),
            Err(SqlError::UndefinedObject)
        );
        assert_eq!(
            user_types::take_sql_error_message().as_deref(),
            Some("constraint \"nope_pkey\" of relation \"tokens\" does not exist")
        );
    }

    #[test]
    fn drop_primary_restrict_and_cascade_keep_unrelated_foreign_keys() {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE \"P\" (id int, other int UNIQUE, CONSTRAINT \"P_pkey\" PRIMARY KEY(id))",
            "CREATE TABLE \"C\" (pid int REFERENCES \"P\"(id))",
            "CREATE TABLE secondary_child (pid int REFERENCES \"P\"(other) ON DELETE CASCADE)",
            "INSERT INTO \"P\" VALUES (1,10)",
            "INSERT INTO \"C\" VALUES (1)",
            "INSERT INTO secondary_child VALUES (10)",
        ] {
            run(&mut state, sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        let before = state.clone();
        assert_eq!(
            run(&mut state, "ALTER TABLE \"P\" DROP CONSTRAINT \"P_pkey\""),
            Err(SqlError::DependentObjects)
        );
        assert_eq!(
            user_types::take_sql_error_message().as_deref(),
            Some("cannot drop constraint P_pkey on table \"P\" because other objects depend on it")
        );
        assert_eq!(
            user_types::take_sql_error_detail().as_deref(),
            Some("constraint C_pid_fkey on table \"C\" depends on index \"P_pkey\"")
        );
        assert_eq!(
            user_types::take_sql_error_hint(),
            Some("Use DROP ... CASCADE to drop the dependent objects too.")
        );
        assert_eq!(state, before);
        run(
            &mut state,
            "ALTER TABLE \"P\" DROP CONSTRAINT \"P_pkey\" CASCADE",
        )
        .unwrap();
        assert_eq!(
            run(&mut state, "SELECT pid FROM secondary_child")
                .unwrap()
                .rows[0]
                .values,
            [Value::Int(10)]
        );
        run(&mut state, "INSERT INTO \"C\" VALUES (2)").unwrap();
        assert_eq!(
            run(&mut state, "INSERT INTO secondary_child VALUES (20)"),
            Err(SqlError::ForeignKeyViolation)
        );
        run(&mut state, "INSERT INTO \"P\" VALUES (1,20)").unwrap();

        run(&mut state, "CREATE TABLE \"N2\" (a int,b int)").unwrap();
        run(&mut state, "INSERT INTO \"N2\" VALUES(NULL,1)").unwrap();
        assert_eq!(
            run(
                &mut state,
                "ALTER TABLE \"N2\" ADD CONSTRAINT \"N2_pkey\" PRIMARY KEY(a)"
            ),
            Err(SqlError::Null)
        );
        assert_eq!(
            user_types::take_sql_error_message().as_deref(),
            Some("column \"a\" of relation \"N2\" contains null values")
        );
        assert!(!state.tables["N2"].columns[0].not_null);
    }
}
