mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, sqlstate};

fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn exercise(engine: Engine) {
    let path = common::fresh("fk-index-binding");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for (n, order) in ["PSU", "PUS", "SPU", "SUP", "UPS", "USP"].iter().enumerate() {
        let p = format!("p{n}");
        run(&mut s, &format!("CREATE TABLE {p}(id int NOT NULL)"));
        for key in order.chars() {
            run(&mut s, &match key {
                'P' => format!("ALTER TABLE {p} ADD CONSTRAINT {p}_pk PRIMARY KEY(id)"),
                'S' => format!("CREATE UNIQUE INDEX {p}_s ON {p}(id)"),
                _ => format!("ALTER TABLE {p} ADD CONSTRAINT {p}_u UNIQUE(id)"),
            });
        }
        for form in 0..6 {
            let c = format!("c{n}_{form}");
            let fk = format!("fk{n}_{form}");
            let refs = if form % 2 == 0 { p.clone() } else { format!("{p}(id)") };
            let sql = match form {
                0 | 1 => format!("CREATE TABLE {c}(id int CONSTRAINT {fk} REFERENCES {refs})"),
                2 | 3 => format!("CREATE TABLE {c}(id int, CONSTRAINT {fk} FOREIGN KEY(id) REFERENCES {refs})"),
                _ => {
                    run(&mut s, &format!("CREATE TABLE {c}(id int)"));
                    format!("ALTER TABLE {c} ADD CONSTRAINT {fk} FOREIGN KEY(id) REFERENCES {refs}")
                }
            };
            run(&mut s, &sql);
            let chosen = if form % 2 == 0 { 'P' } else { order.chars().next().unwrap() };
            let expected = match chosen {
                'P' => format!("pk:{p}_pk"),
                'S' => format!("index:{p}_s"),
                _ => format!("index:{p}_u"),
            };
            assert_eq!(s.foreign_key_targets().unwrap()[&fk], expected, "{order}/{form}");
        }
    }
    for sql in [
        "CREATE TABLE perm(a int,b int,PRIMARY KEY(a,b))",
        "CREATE TABLE perm_c(a int,b int,CONSTRAINT perm_fk FOREIGN KEY(a,b) REFERENCES perm(b,a))",
        "CREATE TABLE up(a int,b int)",
        "CREATE UNIQUE INDEX up_first ON up(b,a)",
        "CREATE UNIQUE INDEX up_second ON up(a,b)",
        "CREATE TABLE up_c(a int,b int,CONSTRAINT up_fk FOREIGN KEY(a,b) REFERENCES up(a,b))",
        "CREATE TABLE self_late(id int,parent int CONSTRAINT self_pk_fk REFERENCES self_late,CONSTRAINT self_late_pk PRIMARY KEY(id))",
        "CREATE TABLE self_unique(id int,parent int CONSTRAINT self_u_fk REFERENCES self_unique(id),CONSTRAINT self_unique_u UNIQUE(id))",
        "CREATE TABLE same_create(id int,CONSTRAINT same_create_u UNIQUE(id),CONSTRAINT same_create_pk PRIMARY KEY(id))",
        "CREATE TABLE same_c(id int CONSTRAINT same_fk REFERENCES same_create(id))",
        "CREATE TABLE same_perm(a int,b int,UNIQUE(b,a),CONSTRAINT same_perm_pk PRIMARY KEY(a,b))",
        "CREATE TABLE same_perm_c(a int,b int,CONSTRAINT same_perm_fk FOREIGN KEY(a,b) REFERENCES same_perm(b,a))",
        "CREATE TABLE same_u(a int,b int,CONSTRAINT same_ba UNIQUE(b,a),CONSTRAINT same_ab UNIQUE(a,b))",
        "CREATE TABLE same_u_c(a int,b int,CONSTRAINT same_u_fk FOREIGN KEY(a,b) REFERENCES same_u(a,b))",
    ] { run(&mut s, sql); }
    for (fk, target) in [
        ("perm_fk", "pk:perm_pkey"), ("up_fk", "index:up_first"),
        ("self_pk_fk", "pk:self_late_pk"), ("self_u_fk", "index:self_unique_u"),
        ("same_fk", "pk:same_create_pk"), ("same_perm_fk", "pk:same_perm_pk"),
        ("same_u_fk", "index:same_ba"),
    ] { assert_eq!(s.foreign_key_targets().unwrap()[fk], target); }
    run(&mut s, "CREATE TABLE no_key(a int,b int)");
    run(&mut s, "CREATE TABLE only_u(a int UNIQUE)");
    for (sql, state) in [
        ("CREATE TABLE bad(a int REFERENCES no_key)", "42704"),
        ("CREATE TABLE bad(a int REFERENCES only_u)", "42704"),
        ("CREATE TABLE bad(a int REFERENCES perm(a))", "42830"),
        ("CREATE TABLE bad(a int,b int,FOREIGN KEY(a,b) REFERENCES perm(a,a))", "42830"),
        ("CREATE TABLE bad(a int REFERENCES no_key(a))", "42830"),
    ] {
        let error = s.execute(sql, &[], None).unwrap_err();
        assert_eq!(sqlstate(&error), state, "{sql}: {error:?}");
    }
    let before = s.foreign_key_targets().unwrap();
    run(&mut s, "ALTER TABLE up ADD PRIMARY KEY(a,b)");
    run(&mut s, "CREATE UNIQUE INDEX up_third ON up(a,b)");
    assert_eq!(s.foreign_key_targets().unwrap(), before);
    // Index rename is not implemented; retain the PG probe as a known coverage gap.
    let error = s.execute("ALTER INDEX up_first RENAME TO up_renamed", &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), "42601");
    assert_eq!(s.foreign_key_targets().unwrap(), before);
    let before = s.foreign_key_targets().unwrap();
    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(admin.sql_session().foreign_key_targets().unwrap(), before);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_binding() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_binding() { exercise(Engine::V2); }
