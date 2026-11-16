mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::SqlError};

fn exercise(engine: Engine) {
    let path = common::fresh("fk-create-candidates");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    // Names/counts are the PostgreSQL 18.6 pg_constraint/pg_index oracle.
    for (table, body, primary, uniques) in [
        ("t1", "id int, CONSTRAINT u UNIQUE(id), CONSTRAINT p PRIMARY KEY(id)", Some("p"), vec![]),
        ("t2", "id int PRIMARY KEY, CONSTRAINT u2 UNIQUE(id)", Some("u2"), vec![]),
        ("t3", "a int,b int,UNIQUE(a,b),UNIQUE(a,b)", None, vec![("t3_a_b_key", vec!["a", "b"])]),
        ("t3_named", "a int,b int,CONSTRAINT first_u UNIQUE(a,b),CONSTRAINT second_u UNIQUE(a,b)", None, vec![("first_u", vec!["a", "b"])]),
        ("t3_transfer", "a int,b int,UNIQUE(a,b),CONSTRAINT transferred UNIQUE(a,b)", None, vec![("transferred", vec!["a", "b"])]),
        ("t4", "a int,b int,UNIQUE(b,a),PRIMARY KEY(a,b)", Some("t4_pkey"), vec![("t4_b_a_key", vec!["b", "a"])]),
        ("t6", "id int UNIQUE PRIMARY KEY", Some("t6_pkey"), vec![]),
        ("ordered", "a int,b int,c int,UNIQUE(c),UNIQUE(b),PRIMARY KEY(a),UNIQUE(b)", Some("ordered_pkey"), vec![("ordered_c_key", vec!["c"]), ("ordered_b_key", vec!["b"])]),
    ] {
        let sql = format!("CREATE TABLE {table}({body})");
        session.execute(&sql, &[], None).unwrap();
        let tables = admin.catalog().unwrap().1;
        let def = tables.iter().find(|t| t.name == table).unwrap();
        let presentation = admin.catalog_presentation().unwrap();
        assert_eq!(presentation.primary_names.get(&def.id).map(String::as_str), primary, "{sql}");
        // CREATE PK has no birth token, preceding every secondary descriptor.
        assert!(!session.primary_key_birth_metadata().unwrap().0.contains_key(&def.id));
        let indexes: Vec<_> = admin.secondary_index_catalog().unwrap().1.into_iter()
            .filter(|i| i.table == def.id).collect();
        assert_eq!(indexes.len(), uniques.len(), "{sql}");
        assert!(indexes.windows(2).all(|w| w[0].id < w[1].id));
        let constraints: Vec<_> = presentation.constraints.iter()
            .filter(|c| c.table == def.id && c.kind == 'u').collect();
        assert_eq!(constraints.len(), uniques.len(), "{sql}");
        for (index, (name, columns)) in indexes.iter().zip(&uniques) {
            assert!(index.unique);
            assert_eq!(&index.name, name);
            let ids: Vec<_> = columns.iter().map(|name|
                def.columns.iter().find(|c| &c.name == name).unwrap().id).collect();
            assert_eq!(index.columns, ids);
            let constraint = constraints.iter().find(|c| c.index == index.id).unwrap();
            assert_eq!(&constraint.name, name);
            assert_eq!(constraint.columns, ids);
        }
    }
    // Case e is accepted by PG18 but not by GigglesDB's key grammar.
    for sql in [
        "CREATE TABLE t5(id int,UNIQUE(id) DEFERRABLE,PRIMARY KEY(id))",
        "CREATE TABLE t5_null(id int,UNIQUE NULLS NOT DISTINCT(id),UNIQUE(id))",
    ] {
        assert_eq!(session.execute(sql, &[], None), Err(CoreError::Sql(SqlError::Syntax)));
    }
    // Keys (including a later UNIQUE) are canonicalized before self-FKs bind.
    session.execute("CREATE TABLE self_fk(id int PRIMARY KEY,parent int REFERENCES self_fk(id),UNIQUE(id))", &[], None).unwrap();
    session.execute("INSERT INTO self_fk VALUES(1,1)", &[], None).unwrap();
    let id = admin.catalog().unwrap().1.iter().find(|t| t.name == "self_fk").unwrap().id;
    assert!(!admin.secondary_index_catalog().unwrap().1.iter().any(|i| i.table == id));
    assert_eq!(admin.catalog_presentation().unwrap().constraints.iter()
        .filter(|c| c.table == id).map(|c| c.kind).collect::<Vec<_>>(), vec!['f']);
    session.execute("CREATE TABLE child(id int REFERENCES t1(id))", &[], None).unwrap();
    session.execute("INSERT INTO t1 VALUES(1)", &[], None).unwrap();
    session.execute("INSERT INTO child VALUES(1)", &[], None).unwrap();
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_create_candidates() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_create_candidates() { exercise(Engine::V2); }
