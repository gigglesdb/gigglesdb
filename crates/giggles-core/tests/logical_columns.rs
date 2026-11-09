use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};

struct Db {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Db {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir()
            .join("logical-columns")
            .join(format!("{:032x}", u128::from_le_bytes(nonce)));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn reopen(&mut self) {
        drop(self.admin.take());
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
#[test]
fn logical_columns_reopen_and_operators() {
    let mut db = Db::new();
    {
        let mut s = db.admin().sql_session();
        run(
            &mut s,
            "CREATE TABLE logicals (id int PRIMARY KEY, u uuid, j jsonb, a int[], b bigint[], t text[])",
        );
        run(
            &mut s,
            r#"INSERT INTO logicals VALUES (1,'{A0EEBC999C0B4EF8BB6D6BB9BD380A11}','{"bb":2,"a":1,"list":[null,"x"]}','{1,NULL,3}','{9223372036854775807}','{"a,b",NULL,"NULL"}')"#,
        );
        assert_eq!(
            run(&mut s, "SELECT u FROM logicals"),
            vec![vec![Value::Text(
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into()
            )]]
        );
        for (col, bad) in [("u", "no"), ("j", "{bad}"), ("a", "{x}")] {
            assert_eq!(
                s.execute(
                    &format!("UPDATE logicals SET {col}='{bad}' WHERE id=1"),
                    &[],
                    None
                ),
                Err(CoreError::Sql(SqlError::InvalidTextRepresentation))
            );
        }
    }
    db.reopen();
    let mut s = db.admin().sql_session();
    assert_eq!(
        run(
            &mut s,
            "SELECT a[2],a[3],a[0],a[9],array_length(a,1),array_length(a,2) FROM logicals"
        ),
        vec![vec![
            Value::Null,
            Value::Int(3),
            Value::Null,
            Value::Null,
            Value::Int(3),
            Value::Null
        ]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT j->>'a',j->'a',j->'list'->>-1,j->>'missing' FROM logicals"
        ),
        vec![vec![
            Value::Text("1".into()),
            Value::Text("1".into()),
            Value::Text("x".into()),
            Value::Null
        ]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM logicals WHERE u='A0EEBC999C0B4EF8BB6D6BB9BD380A11'::uuid"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn logical_literal_casts() {
    let db = Db::new();
    let mut s = db.admin().sql_session();
    assert_eq!(
        run(
            &mut s,
            "SELECT '1'::jsonb->0, '1'::jsonb->>-1, array_length('{1,NULL,3}'::int[],1)"
        ),
        vec![vec![
            Value::Text("1".into()),
            Value::Text("1".into()),
            Value::Int(3)
        ]]
    );
    run(&mut s, "CREATE TABLE extra (id int PRIMARY KEY)");
    run(&mut s, "ALTER TABLE extra ADD COLUMN u uuid");
    run(
        &mut s,
        "INSERT INTO extra VALUES (1,'12345678123456781234567812345678')",
    );
    run(&mut s, "ALTER TABLE extra DROP COLUMN u");
    run(&mut s, "DROP TABLE extra");
}
