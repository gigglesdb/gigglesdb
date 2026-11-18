//! FK existence witnesses must not consume the child's version reservation.
use super::*;

fn run(session: &mut SqlSession, sql: &str) -> relational::SqlResult {
    let clock = relational::StatementClock {
        unavailable: None, transaction_us: 1, statement_us: 1, zone: None, wall: None,
    };
    session.execute_at(sql, &[], None, &clock)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn newer_parent_witness(isolation: SqlIsolation) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!("fk-version-{:032x}", u128::from_be_bytes(nonce)));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    {
        let mut session = admin.sql_session();
        run(&mut session, "CREATE TABLE p(id bigint PRIMARY KEY,v bigint)");
        run(&mut session, "CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id))");
        run(&mut session, "INSERT INTO p VALUES(1,0)");
        session.set_isolation(isolation);
        run(&mut session, "BEGIN");
        let writer = admin.clone();
        v2_sql::AFTER_DML_VERSION_RESERVATION.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                std::thread::spawn(move || {
                    run(&mut writer.sql_session(), "UPDATE p SET v=v+1 WHERE id=1");
                }).join().unwrap();
            }));
        });
        run(&mut session, "INSERT INTO c VALUES(1,1)");
        run(&mut session, "COMMIT");
        assert_eq!(run(&mut session, "SELECT * FROM c").rows.len(), 1);
        assert_eq!(run(&mut session, "SELECT v FROM p").rows[0].values, vec![relational::Value::Int(1)]);
    }
    drop(admin);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn newer_parent_witness_read_committed() {
    newer_parent_witness(SqlIsolation::ReadCommitted);
}

#[test]
fn newer_parent_witness_repeatable_read() {
    newer_parent_witness(SqlIsolation::RepeatableRead);
}
