mod common;
use giggles_core::{Admin, Engine, Limits, relational::Value};

fn engines() -> Vec<Engine> {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")]
    engines.push(Engine::V2);
    engines
}

#[test]
fn qualified_targets_match_pg18_6() {
    // PG18.6 basis: explicit target schemas override search_path for every DML form.
    for engine in engines() {
        for (target, other, search_path) in [("public.dest", "app.dest", "app, public"),
            ("app.dest", "public.dest", "public")] {
            let path = common::fresh("qualified-dml-targets");
            let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = admin.sql_session();
            for sql in ["CREATE SCHEMA app", "CREATE TABLE public.dest(id INT PRIMARY KEY, v TEXT)",
                "CREATE TABLE app.dest(id INT PRIMARY KEY, v TEXT)", "CREATE TABLE public.src(id INT, v TEXT)",
                "INSERT INTO public.src VALUES(1,'a'),(2,'b')"] {
                s.execute(sql, &[], None).unwrap();
            }
            s.set_search_path(search_path, "postgres");
            for (sql, affected) in [
                (format!("INSERT INTO {target} SELECT id, v FROM public.src"), 2),
                (format!("INSERT INTO {target} VALUES (3,'c')"), 1),
                (format!("INSERT INTO {target} VALUES (3,'z') ON CONFLICT (id) DO UPDATE SET v = excluded.v"), 1),
                (format!("INSERT INTO {target} VALUES (4,'d') ON CONFLICT DO NOTHING"), 1),
                (format!("UPDATE {target} SET v = v || '!' WHERE id = 1"), 1),
                (format!("DELETE FROM {target} WHERE id = 2"), 1),
            ] {
                assert_eq!(s.execute(&sql, &[], None).unwrap().affected, affected, "{engine:?}: {sql}");
            }
            let rows = s.execute(&format!("SELECT * FROM {target} ORDER BY id"), &[], None).unwrap();
            assert_eq!(rows.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(), vec![
                vec![Value::Int(1), Value::Text("a!".into())],
                vec![Value::Int(3), Value::Text("z".into())],
                vec![Value::Int(4), Value::Text("d".into())],
            ], "{engine:?}: {target}");
            assert!(s.execute(&format!("SELECT * FROM {other}"), &[], None).unwrap().rows.is_empty());
            assert_eq!(s.execute("INSERT INTO dest VALUES (9,'app')", &[], None).unwrap().affected, 1);
            let bare_target = if search_path == "public" { "public.dest" } else { "app.dest" };
            let rows = s.execute(&format!("SELECT * FROM {bare_target}"), &[], None).unwrap();
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0].values, vec![Value::Int(9), Value::Text("app".into())]);
            drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
        }
    }
}

fn check_public_dml_target(sql: &str, affected: u64, expected: &str) {
    for engine in engines() {
        let path = common::fresh("each-qualified-dml-target");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for setup in ["CREATE SCHEMA app", "CREATE TABLE public.dest(id INT PRIMARY KEY, v TEXT)",
            "CREATE TABLE app.dest(id INT PRIMARY KEY, v TEXT)", "CREATE TABLE public.src(id INT, v TEXT)",
            "INSERT INTO public.src VALUES(1,'new')", "INSERT INTO app.dest VALUES(1,'shadow')"] {
            s.execute(setup, &[], None).unwrap();
        }
        if !sql.starts_with("INSERT") || sql.contains("ON CONFLICT") {
            s.execute("INSERT INTO public.dest VALUES(1,'old')", &[], None).unwrap();
        }
        s.set_search_path("app, public", "postgres");
        assert_eq!(s.execute(sql, &[], None).unwrap().affected, affected, "{engine:?}: {sql}");
        let rows = s.execute("SELECT v FROM public.dest", &[], None).unwrap();
        let actual = rows.rows.first().map(|r| r.values[0].clone());
        assert_eq!(actual, if expected.is_empty() { None } else { Some(Value::Text(expected.into())) }, "{engine:?}: {sql}");
        assert_eq!(s.execute("SELECT v FROM app.dest", &[], None).unwrap().rows[0].values,
            vec![Value::Text("shadow".into())], "{engine:?}: {sql}");
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn public_insert_select() {
    check_public_dml_target("INSERT INTO public.dest SELECT id, v FROM public.src", 1, "new");
}
#[test]
fn public_insert_values() {
    check_public_dml_target("INSERT INTO public.dest VALUES (1,'new')", 1, "new");
}
#[test]
fn public_upsert() {
    check_public_dml_target("INSERT INTO public.dest VALUES (1,'new') ON CONFLICT (id) DO UPDATE SET v = excluded.v", 1, "new");
}
#[test]
fn public_do_nothing() {
    check_public_dml_target("INSERT INTO public.dest VALUES (1,'new') ON CONFLICT DO NOTHING", 0, "old");
}
#[test]
fn public_update() {
    check_public_dml_target("UPDATE public.dest SET v = 'new' WHERE id = 1", 1, "new");
}
#[test]
fn public_delete() {
    check_public_dml_target("DELETE FROM public.dest WHERE id = 1", 1, "");
}
