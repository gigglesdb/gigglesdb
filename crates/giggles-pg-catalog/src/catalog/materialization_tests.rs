use super::*;

fn snapshot(tables: u64, explicit_indexes: u64) -> CatalogSnapshot {
    let mut snapshot = CatalogSnapshot {
        timezone_now: Some(0),
        user_types: vec![UserType {
            schema: "public".into(),
            oid: 900_000,
            name: "publication_status".into(),
            kind: 'e',
            base: 0,
            typmod: -1,
            not_null: false,
            labels: vec!["draft".into(), "published".into()],
        }],
        ..CatalogSnapshot::default()
    };
    let columns = [
        ("id", ScalarType::Uuid),
        ("workspace_id", ScalarType::Uuid),
        ("created_by_id", ScalarType::Uuid),
        ("status", ScalarType::Text),
        ("display_name", ScalarType::Text),
        ("created_at", ScalarType::TimestampTz),
        ("updated_at", ScalarType::TimestampTz),
        ("description", ScalarType::Text),
        ("source_url", ScalarType::Text),
        ("archived_at", ScalarType::TimestampTz),
    ];
    for table in 1..=tables {
        let name = format!(
            "{}_revision_{table}",
            ["document", "workspace", "attachment", "publication"][table as usize % 4]
        );
        snapshot.tables.push(TableDef {
            id: table,
            name: name.clone(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(i, &(name, data_type))| ColumnDef {
                    id: table * 10 + i as u64,
                    name: name.into(),
                    data_type,
                    not_null: i < 7,
                })
                .collect(),
            primary_key: vec![0],
        });
        snapshot.column_type_oids.insert(table * 10 + 3, 900_000);
        snapshot
            .column_defaults
            .insert(table * 10, "gen_random_uuid()".into());
        snapshot
            .column_defaults
            .insert(table * 10 + 5, "CURRENT_TIMESTAMP".into());
        for (i, columns) in [vec![table * 10 + 1], vec![table * 10 + 2, table * 10 + 5]]
            .into_iter()
            .enumerate()
        {
            if table + tables * i as u64 > explicit_indexes {
                continue;
            }
            snapshot.indexes.push(IndexDef {
                id: 10_000 + table * 2 + i as u64,
                table,
                name: format!(
                    "{name}_{}",
                    ["workspace_idx", "created_by_created_at_idx"][i]
                ),
                columns,
                unique: false,
            });
        }
        if table > 1 {
            for (i, column) in ["workspace_id", "created_by_id"].iter().enumerate() {
                snapshot.constraints.push(ConstraintDef {
                    id: 20_000 + table * 2 + i as u64,
                    table,
                    name: format!("{name}_{column}_fkey"),
                    kind: 'f',
                    columns: vec![table * 10 + i as u64 + 1],
                    foreign_table: table - 1,
                    foreign_columns: vec![(table - 1) * 10],
                    foreign_key_target: Some(ForeignKeyTarget::PrimaryKey),
                    update: 'a',
                    delete: 'a',
                    validated: true,
                    ..Default::default()
                });
            }
        }
    }
    snapshot
}

fn run(snapshot: &CatalogSnapshot, sql: &str) -> Vec<Vec<Value>> {
    let plan = plan(sql)
        .unwrap()
        .unwrap_or_else(|| panic!("no catalog plan for {sql}"));
    execute(&plan, snapshot, &[])
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .rows
}

fn count(snapshot: &CatalogSnapshot, sql: &str) -> i64 {
    let rows = run(snapshot, sql);
    let [row] = rows.as_slice() else {
        panic!("{sql}: {rows:?}")
    };
    let [I(count)] = row.as_slice() else {
        panic!("{sql}: {row:?}")
    };
    *count
}

const NAMESPACE_EXISTS: &str = "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='public')";
const ATTRIBUTE_COUNT: &str = "SELECT count(*) FROM pg_catalog.pg_attribute";
const COLUMNS_COUNT: &str =
    "SELECT count(*) FROM information_schema.columns WHERE table_schema='public'";
const INTROSPECTION_COUNT: &str = "SELECT count(*) FROM pg_catalog.pg_attribute a
     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
     WHERE n.nspname = 'public' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped";

#[test]
fn archdocs_shape_and_headroom_through_catalog_execution() {
    let base_attributes = count(&CatalogSnapshot::default(), ATTRIBUTE_COUNT);
    for (table_count, indexes) in [(195, 368), (2000, 4000)] {
        let snapshot = snapshot(table_count, indexes);
        let tables = table_count as i64;
        let index_attributes = snapshot
            .indexes
            .iter()
            .map(|index| index.columns.len() as i64)
            .sum::<i64>();
        assert_eq!(run(&snapshot, NAMESPACE_EXISTS), vec![vec![B(true)]]);
        assert_eq!(
            count(&snapshot, ATTRIBUTE_COUNT),
            base_attributes + tables * 17 + index_attributes
        );
        assert_eq!(count(&snapshot, COLUMNS_COUNT), tables * 10);
        assert_eq!(count(&snapshot, INTROSPECTION_COUNT), tables * 10);
        for requested in [
            &["pg_namespace"][..],
            &["pg_attribute"],
            &["columns"],
            &["pg_attribute", "pg_class", "pg_namespace", "pg_attrdef"],
        ] {
            let mut budget = Budget::default();
            let demand = requested.iter().map(|&name| name.to_owned()).collect();
            build_with_budget(&snapshot, &demand, &mut budget).unwrap();
            assert!(budget.bytes < budget.byte_limit * 3 / 4);
            assert!(budget.work < MAX_WORK / 2);
            eprintln!(
                "{tables} tables {requested:?}: {} bytes, {} work",
                budget.bytes, budget.work
            );
        }
        if table_count == 195 {
            let columns = run(
                &snapshot,
                "SELECT table_name, column_name, data_type, column_default
                FROM information_schema.columns WHERE table_schema='public' ORDER BY 1, 2",
            );
            assert_eq!(columns.len() as i64, tables * 10);
            assert_eq!(
                columns
                    .iter()
                    .filter(|row| row[3] == s("gen_random_uuid()"))
                    .count(),
                195
            );
            let constraints = run(
                &snapshot,
                "SELECT c.oid, pg_get_constraintdef(c.oid) FROM pg_constraint c",
            );
            assert_eq!(constraints.len(), 195 * 8 + snapshot.constraints.len());
            assert!(
                constraints
                    .iter()
                    .all(|row| matches!(&row[1], Value::Text(value) if !value.is_empty()))
            );
            let definitions = run(
                &snapshot,
                "SELECT pg_get_indexdef(indexrelid) FROM pg_index",
            );
            assert_eq!(definitions.len(), 195 + snapshot.indexes.len());
            assert!(
                definitions.iter().all(
                    |row| matches!(&row[0], Value::Text(value) if value.starts_with("CREATE "))
                )
            );
        }
    }
}

#[test]
fn every_relation_matches_full_materialization_in_row_order() {
    let mut snapshot = snapshot(24, 46);
    // An OID from a relation outside the selected catalog must still reserve
    // its slot in synthetic constraint and default OID allocation.
    snapshot.schemas.insert("archive".into(), (u32::MAX, 10));
    snapshot.table_schemas.insert(2, "archive".into());
    snapshot.comments.insert((1, 0), "document revision".into());
    snapshot.column_identities.insert(11, 'd');
    let names = known_names()
        .iter()
        .copied()
        .chain([
            "pg_statistic_ext",
            "pg_publication",
            "pg_publication_namespace",
            "pg_publication_rel",
        ])
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let mut full_budget = Budget::default();
    let full = build_with_budget(&snapshot, &names, &mut full_budget).unwrap();
    assert_eq!(
        full_budget.bytes,
        full.values()
            .flat_map(|relation| &relation.rows)
            .map(|row| row_bytes(row))
            .sum::<usize>()
    );
    assert_eq!(
        full["pg_constraint"]
            .rows
            .iter()
            .find(|row| row[3] == s("n"))
            .unwrap()[0],
        I(i64::from(u32::MAX) - 1)
    );
    for (name, relation) in full {
        let demand = BTreeSet::from([name.clone()]);
        let selected = build(&snapshot, &demand).unwrap();
        assert_eq!(selected.len(), 1, "{name}");
        assert_eq!(selected[&name].fields, relation.fields, "{name} fields");
        assert_eq!(selected[&name].rows, relation.rows, "{name} rows");
    }
}

#[test]
fn materialization_stops_early_when_requested_comments_exceed_the_byte_budget() {
    let mut snapshot = snapshot(195, 368);
    for table in &snapshot.tables {
        snapshot.comments.insert((table.id, 0), "x".repeat(12_000));
    }
    let mut budget = Budget {
        byte_limit: 128 * 1024,
        ..Budget::default()
    };
    let requested = BTreeSet::from(["pg_description".into(), "pg_attribute".into()]);
    assert!(matches!(
        build_with_budget(&snapshot, &requested, &mut budget),
        Err(Error::Limit)
    ));
    let mut complete = Budget::default();
    build_with_budget(&snapshot, &requested, &mut complete).unwrap();
    assert!(budget.work < complete.work / 2);
    let mut namespace_budget = Budget {
        byte_limit: budget.byte_limit,
        ..Budget::default()
    };
    let requested = BTreeSet::from(["pg_namespace".into()]);
    build_with_budget(&snapshot, &requested, &mut namespace_budget).unwrap();
    assert!(namespace_budget.bytes < 1024);
    assert_eq!(run(&snapshot, NAMESPACE_EXISTS), vec![vec![B(true)]]);
}
