//! Profile 0.21 CREATE TABLE foreign keys bind like SQL; native execution forbids the SQL parser.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession, relational::with_sql_parser_forbidden};
use giggles_language::{Bindings, CatalogStatement, ConstraintSpec, Engine, Error, ForeignKeyAction, LanguageVersion,
    TableConstraint, parse_catalog_with_version};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded021;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-create-fk21-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn storages() -> Vec<Storage> {
    vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2]
}
/// ID-free catalog and rows: on V2 a CREATE TABLE foreign key gets its object ID
/// the way ALTER TABLE ADD CONSTRAINT allocates it, which also shifts later IDs
/// (asserted exactly in exact_ids_match_sql_create_then_alter).
fn snapshot(session: &mut SqlSession) -> String {
    use std::collections::BTreeMap;
    let m = session.catalog_metadata().unwrap();
    let mut names = BTreeMap::new();
    for table in &m.tables {
        let qualified = format!("{}.{}", m.schemas.tables.get(&table.id).map(String::as_str).unwrap_or("public"), table.name);
        for column in &table.columns { names.insert(column.id, format!("{qualified}.{}", column.name)); }
        names.insert(table.id, qualified);
    }
    let name = |id: &u64| names.get(id).cloned().unwrap_or_else(|| format!("#{id}"));
    let mut tables = m.tables.iter().map(|t| (name(&t.id), t.columns.iter().map(|c| (c.name.clone(), format!("{:?}", c.data_type), c.not_null)).collect::<Vec<_>>(),
        t.primary_key.clone(), m.presentation.primary_names.get(&t.id).cloned())).collect::<Vec<_>>();
    tables.sort();
    let mut constraints = m.presentation.constraints.iter().map(|c| format!("{} {} {} {:?} {} {:?} {}{} {} {} {} {:?}",
        name(&c.table), c.name, c.kind, c.columns.iter().map(name).collect::<Vec<_>>(), if c.foreign_table == 0 { String::new() } else { name(&c.foreign_table) },
        c.foreign_columns.iter().map(name).collect::<Vec<_>>(), c.update, c.delete, c.deferrable, c.deferred, c.validated, c.expression)).collect::<Vec<_>>();
    constraints.sort();
    let mut indexes = m.indexes.iter().map(|i| (name(&i.table), i.name.clone(), i.columns.iter().map(name).collect::<Vec<_>>(), i.unique)).collect::<Vec<_>>();
    indexes.sort();
    let defaults = m.presentation.defaults.iter().map(|(k, v)| (name(k), v.clone())).collect::<BTreeMap<_, _>>();
    let identities = m.presentation.identities.iter().map(|(k, v)| (name(k), *v)).collect::<BTreeMap<_, _>>();
    let mut out = format!("{:?}", (&tables, &constraints, &indexes, &defaults, &identities, m.presentation.views.len()));
    for (table, ..) in tables {
        let rows = session.execute(&format!("select * from {table} order by 1"), &[], None)
            .unwrap_or_else(|e| panic!("snapshot table {table}: {e:?}")).rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
        out.push_str(&format!("{rows:?}"));
    }
    out
}
fn snapshot_ids(session: &mut SqlSession) -> String {
    let mut m = session.catalog_metadata().unwrap();
    m.presentation.epoch = 0; // V2 epochs are per-database HLCs, not logical catalog state.
    format!("{:?}", (&m.tables, &m.indexes, &m.presentation))
}
fn equal(left: &Admin, right: &Admin) -> String {
    let expected = snapshot(&mut left.sql_session());
    assert_eq!(snapshot(&mut right.sql_session()), expected);
    expected
}
fn create(left: &Admin, right: &Admin, native: &str, sql: &str, state: Option<&str>) {
    let before = equal(left, right);
    let expected = left.sql_session().execute(sql, &[], None);
    let actual = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION).execute_catalog_text(native));
    if let Some(state) = state {
        assert_eq!(giggles_core::sqlstate(&expected.unwrap_err()), state, "{sql}");
        assert_eq!(actual.unwrap_err(), Error::Sql { sqlstate: state.into() }, "{native}");
        // Atomic: a failing foreign key leaves no table behind.
        assert_eq!(equal(left, right), before);
    } else {
        assert_eq!(expected.unwrap().affected, 0);
        actual.unwrap_or_else(|e| panic!("{native}: {e:?}"));
        assert_ne!(equal(left, right), before);
    }
}
fn dml(left: &Admin, right: &Admin, native: &str, sql: &str, state: Option<&str>) {
    // Defaults (serial, identity, SET DEFAULT) need SQL's statement clock.
    let clock = giggles_core::relational::StatementClock { unavailable: None, transaction_us: 1_000_000, statement_us: 1_000_000, zone: None, wall: None };
    let l = left.sql_session().execute_at(sql, &[], None, &clock);
    let r = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION).execute_text(native, &Bindings::new()));
    match state {
        Some(state) => {
            assert_eq!(giggles_core::sqlstate(&l.unwrap_err()), state, "{sql}");
            assert_eq!(r.unwrap_err(), Error::Sql { sqlstate: state.into() }, "{native}");
        }
        None => assert_eq!(l.unwrap_or_else(|e| panic!("{sql}: {e:?}")).affected, r.unwrap_or_else(|e| panic!("{native}: {e:?}")).affected as u64, "{sql}"),
    }
    equal(left, right);
}
#[test]
fn create_table_foreign_keys_match_sql() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        for a in [&left, &right] {
            for sql in ["create table parent(id bigint primary key,code bigint not null)", "create schema app", "create table app.p(id bigint primary key)"] {
                a.sql_session().execute(sql, &[], None).unwrap();
            }
        }
        for (native, sql, state) in [
            // Column REFERENCES with an action; the generated name matches SQL (child_parent_id_fkey).
            ("create table child(id int64,parent_id nullable int64 references parent(id) on delete cascade,primary key(id))",
             "create table child(id bigint primary key,parent_id bigint references parent(id) on delete cascade)", None),
            // Table constraints, named and unnamed, referencing the primary key implicitly and explicitly.
            ("create table item(id int64,a nullable int64,b nullable int64,primary key(id),constraint item_a foreign key(a) references parent on update restrict,foreign key(b) references parent(id) on delete set null on update cascade)",
             "create table item(id bigint primary key,a bigint,b bigint,constraint item_a foreign key(a) references parent on update restrict,foreign key(b) references parent(id) on delete set null on update cascade)", None),
            // Self reference, and two keys on one column (second generated name gets a suffix).
            ("create table node(id int64,up nullable int64 references node(id) references node,primary key(id))",
             "create table node(id bigint primary key,up bigint references node(id) references node)", None),
            // Schema-qualified table and parent, named column constraint.
            ("create table app.c(id int64,p nullable int64 constraint c_p references app.p(id) on delete restrict,primary key(id))",
             "create table app.c(id bigint primary key,p bigint constraint c_p references app.p(id) on delete restrict)", None),
            // Defaults, serial and identity together with a SET DEFAULT foreign key.
            ("create table combo(id serial,p int64 default 1 references parent(id) on delete set default,n int64 generated by default as identity,primary key(id))",
             "create table combo(id serial primary key,p bigint not null default 1 references parent(id) on delete set default,n bigint generated by default as identity)", None),
            ("create table bad(id int64,p nullable int64 references absent(id),primary key(id))",
             "create table bad(id bigint primary key,p bigint references absent(id))", Some("42P01")),
            ("create table bad(id int64,p int64,primary key(id),foreign key(p) references parent(code))",
             "create table bad(id bigint primary key,p bigint not null,foreign key(p) references parent(code))", Some("42830")),
            ("create table bad(id int64,p int64,primary key(id),foreign key(nope) references parent(id))",
             "create table bad(id bigint primary key,p bigint not null,foreign key(nope) references parent(id))", Some("42703")),
            ("create table child(id int64,p nullable int64 references parent,primary key(id))",
             "create table child(id bigint primary key,p bigint references parent)", Some("42P07")),
        ] {
            create(&left, &right, native, sql, state);
        }
        for (native, sql, state) in [
            ("insert parent set id=1,code=10", "insert into parent values(1,10)", None),
            ("insert parent set id=2,code=20", "insert into parent values(2,20)", None),
            ("insert child set id=1,parent_id=1", "insert into child values(1,1)", None),
            ("insert child set id=2,parent_id=99", "insert into child values(2,99)", Some("23503")),
            ("insert item set id=1,a=2,b=1", "insert into item values(1,2,1)", None),
            ("insert node set id=1,up=null", "insert into node values(1,null)", None),
            ("insert node set id=2,up=1", "insert into node values(2,1)", None),
            ("insert node set id=3,up=7", "insert into node values(3,7)", Some("23503")),
            ("insert app.p set id=5", "insert into app.p values(5)", None),
            ("insert app.c set id=1,p=5", "insert into app.c values(1,5)", None),
            ("delete app.p where id=5", "delete from app.p where id=5", Some("23001")),
            ("insert parent set id=3,code=30", "insert into parent values(3,30)", None),
            ("insert combo set p=3", "insert into combo(p) values(3)", None),
            ("insert combo set p=4", "insert into combo(p) values(4)", Some("23503")),
            // SET DEFAULT moves combo.p from 3 to its default 1.
            ("delete parent where id=3", "delete from parent where id=3", None),
            ("delete combo where p=1", "delete from combo where p=1", None),
            // item_a is NO ACTION on delete.
            ("delete parent where id=2", "delete from parent where id=2", Some("23503")),
            ("delete parent where id=1", "delete from parent where id=1", None),
        ] {
            dml(&left, &right, native, sql, state);
        }
        // ON DELETE CASCADE removed the child row and SET NULL cleared item.b.
        assert!(right.sql_session().execute("select * from child", &[], None).unwrap().rows.is_empty());
        assert_eq!(right.sql_session().execute("select * from item where b is null", &[], None).unwrap().rows.len(), 1);
        assert!(right.sql_session().execute("select * from combo", &[], None).unwrap().rows.is_empty());
    }
}
#[test]
fn create_table_foreign_keys_are_opt_in_and_atomic() {
    let text = "create table c(id int64,p nullable int64 constraint fk references parent(id) on delete cascade,primary key(id),foreign key(p) references parent)";
    assert!(parse_catalog_with_version(text, LanguageVersion::Embedded020).is_err());
    let fk = |name: Option<&str>, ref_columns: Option<Vec<String>>, on_delete| TableConstraint {
        name: name.map(Into::into), constraint: ConstraintSpec::ForeignKey { columns: vec!["p".into()], ref_table: "parent".into(), ref_columns, on_delete, on_update: ForeignKeyAction::NoAction },
    };
    let CatalogStatement::CreateTableWith { constraints, primary_key, columns, .. } = parse_catalog_with_version(text, VERSION).unwrap() else { panic!() };
    assert_eq!(constraints, vec![fk(Some("fk"), Some(vec!["id".into()]), ForeignKeyAction::Cascade), fk(None, None, ForeignKeyAction::NoAction)]);
    // Column options and REFERENCES combine in one statement.
    let CatalogStatement::CreateTableWith { columns: combo, constraints, .. } = parse_catalog_with_version(
        "create table c(id serial,p int64 default 1 references parent,primary key(id))", VERSION).unwrap() else { panic!() };
    assert!(combo[0].generated.is_some() && combo[1].default.is_some());
    assert_eq!(constraints.len(), 1);

    assert_eq!((primary_key, columns.len()), (vec!["id".to_string()], 2));
    // Without foreign keys 0.21 parses exactly like 0.20.
    let plain = "create table c(id int64,p nullable int64,primary key(id))";
    assert_eq!(parse_catalog_with_version(plain, VERSION).unwrap(), parse_catalog_with_version(plain, LanguageVersion::Embedded020).unwrap());
    // ALTER TABLE ADD FOREIGN KEY keeps its 0.20 tree.
    let alter = "alter table c add constraint fk foreign key(p) references parent(id) on update set default";
    assert_eq!(parse_catalog_with_version(alter, VERSION).unwrap(), parse_catalog_with_version(alter, LanguageVersion::Embedded020).unwrap());
    // Columns named like the new keywords still parse.
    parse_catalog_with_version("create table c(id int64,foreign int64,constraint int64,primary key(id))", VERSION).unwrap();
    for bad in ["create table c(id int64,p int64 references,primary key(id))",
        "create table c(id int64,p int64 references parent on delete cascade on delete restrict,primary key(id))",
        "create table c(id int64,primary key(id),constraint fk foreign key() references parent)"] {
        assert!(parse_catalog_with_version(bad, VERSION).is_err(), "{bad}");
    }
    let (_db, admin) = Database::fresh(Storage::Legacy);
    admin.sql_session().execute("create table parent(id bigint primary key)", &[], None).unwrap();
    let engine = Engine::with_version(admin.clone(), VERSION);
    let statement = parse_catalog_with_version(text, VERSION).unwrap();
    // Other constraint kinds stay reserved in CREATE TABLE trees.
    let mut tree = parse_catalog_with_version("create table u(id serial,primary key(id))", VERSION).unwrap();
    let CatalogStatement::CreateTableWith { constraints, .. } = &mut tree else { panic!() };
    constraints.push(TableConstraint { name: None, constraint: ConstraintSpec::Unique { columns: vec!["id".into()] } });
    assert_eq!(engine.execute_catalog(&tree), Err(Error::Unsupported));
    // No single bound request carries the foreign keys.
    assert!(matches!(engine.prepare_catalog_bound(&statement), Err(Error::Unsupported)));
    assert_eq!(with_sql_parser_forbidden(|| engine.execute_catalog_optional(&statement)).unwrap().unwrap().table.name, "c");
    // A failing second key rolls back the table and the first key.
    let before = snapshot(&mut admin.sql_session());
    let failing = "create table d(id int64,p nullable int64 references parent,q nullable int64 references absent,primary key(id))";
    assert_eq!(with_sql_parser_forbidden(|| engine.execute_catalog_text(failing)), Err(Error::Sql { sqlstate: "42P01".into() }));
    assert_eq!(snapshot(&mut admin.sql_session()), before);
}
#[test]
fn exact_ids_match_sql_create_then_alter() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        for a in [&left, &right] { a.sql_session().execute("create table parent(id bigint primary key)", &[], None).unwrap(); }
        for sql in ["create table child(id bigint primary key,p bigint,q bigint)",
            "alter table child add constraint fk_p foreign key(p) references parent(id) on delete cascade",
            "alter table child add foreign key(q) references parent"] {
            left.sql_session().execute(sql, &[], None).unwrap();
        }
        with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION).execute_catalog_text(
            "create table child(id int64,p nullable int64 constraint fk_p references parent(id) on delete cascade,q nullable int64,primary key(id),foreign key(q) references parent)")).unwrap();
        assert_eq!(snapshot_ids(&mut right.sql_session()), snapshot_ids(&mut left.sql_session()));
    }
}
