use giggles_pg_catalog::*;
fn snapshot(target: Option<ForeignKeyTarget>) -> CatalogSnapshot {
    let mut s = CatalogSnapshot::default();
    for (id, name) in [(10, "p"), (20, "c")] {
        s.tables.push(TableDef { id, name: name.into(), primary_key: vec![0],
            columns: vec![ColumnDef { id, name: "id".into(), data_type: ScalarType::Integer, not_null: true }] });
    }
    s.indexes.push(IndexDef { id: 40, table: 10, name: "s".into(), columns: vec![10], unique: true });
    s.constraints.push(ConstraintDef { id: 30, table: 20, name: "fk".into(), kind: 'f',
        columns: vec![20], foreign_table: 10, foreign_columns: vec![10], foreign_key_target: target,
        update: 'a', delete: 'a', validated: true, ..Default::default() });
    s
}
fn run(s: &CatalogSnapshot, sql: &str) -> Result<Vec<Vec<Value>>, Error> {
    execute(&plan(sql).unwrap().unwrap(), s, &[]).map(|r| r.rows)
}
#[test]
fn saved_target_controls_conindid_and_dependency() {
    use Value::{Int as I, Text as S};
    for (target, oid) in [(Some(ForeignKeyTarget::Index(40)), 65696),
        (Some(ForeignKeyTarget::PrimaryKey), 65577), (None, 0)] {
        let s = snapshot(target);
        assert_eq!(run(&s, "SELECT conindid FROM pg_constraint WHERE contype='f'").unwrap(), vec![vec![I(oid)]]);
        let edges = run(&s, "SELECT refobjid,deptype FROM pg_depend WHERE classid=2606 AND objid=65656 AND refobjsubid=0").unwrap();
        assert_eq!(edges, if oid == 0 { vec![] } else { vec![vec![I(oid), S("n".into())]] });
        assert_eq!(run(&s, "SELECT refobjid,refobjsubid,deptype FROM pg_depend WHERE classid=2606 AND objid=65656 AND refobjsubid=1 ORDER BY refobjid").unwrap(),
            vec![vec![I(65576), I(1), S("n".into())], vec![I(65616), I(1), S("a".into())]]);
    }
}
#[test]
fn invalid_known_targets_are_rejected() {
    let base = snapshot(Some(ForeignKeyTarget::Index(40)));
    let mut invalid = Vec::new();
    let mut s = base.clone(); s.indexes.clear(); invalid.push(s);
    let mut s = base.clone(); s.indexes[0].table = 20; s.indexes[0].columns = vec![20]; invalid.push(s);
    let mut s = base.clone(); s.indexes[0].unique = false; invalid.push(s);
    let mut s = base; s.indexes[0].columns.clear(); invalid.push(s);
    let mut s = snapshot(Some(ForeignKeyTarget::PrimaryKey)); s.tables[0].primary_key.clear(); invalid.push(s);
    let mut s = snapshot(Some(ForeignKeyTarget::PrimaryKey));
    s.tables[0].columns.push(ColumnDef { id: 11, name: "other".into(), data_type: ScalarType::Integer, not_null: true });
    s.tables[0].primary_key = vec![1]; invalid.push(s);
    for s in invalid {
        assert_eq!(run(&s, "SELECT * FROM pg_constraint"), Err(Error::InvalidSnapshot));
    }
}
