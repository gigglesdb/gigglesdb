use super::*;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(10);

fn next(session: &mut SqlSession, sequence: &str) -> i64 {
    scalar(session, &format!("SELECT nextval('{sequence}')"))
}

fn sequence_slot(db: &EngineLease, sequence: &str) -> Arc<v2::SeqSlot> {
    let view = db.catalog().current_local_view().unwrap();
    let id = view
        .sequences
        .iter()
        .find(|(_, def)| def.name == sequence)
        .unwrap()
        .0;
    db.sql_counters.lock().unwrap()[id].clone()
}

fn before_settlement<T>(slot: &v2::SeqSlot, received: &mpsc::Receiver<T>) -> Option<T> {
    let deadline = Instant::now() + WAIT;
    loop {
        match received.try_recv() {
            Ok(value) => return Some(value),
            Err(mpsc::TryRecvError::Disconnected) => panic!("caller exited without reporting"),
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if slot.counter.lock().unwrap().waiting_returns != 0 {
            return received.try_recv().ok();
        }
        assert!(Instant::now() < deadline, "caller never drew or returned");
        thread::yield_now();
    }
}

#[test]
fn staged_rewrite_draw_cancels_while_root_ack_is_held() {
    let mut f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    assert_eq!(scalar(&mut session, "SELECT setval('s', 10)"), 10);
    run(&mut session, "BEGIN");
    run(&mut session, "ALTER SEQUENCE s CACHE 2");

    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_acks(false));
    let cancel = crate::Cancellation::new(None);
    let request = cancel.clone();
    let (send, recv) = mpsc::channel();
    let client = thread::spawn(move || {
        let result = request.scope(|| session.execute_at("SELECT nextval('s')", &[], None, &clock()));
        send.send(result).unwrap();
        session
    });
    let deadline = Instant::now() + WAIT;
    while db.sequencer.held_counter_acks() == 0 {
        assert!(Instant::now() < deadline, "staged draw did not submit its root write");
        thread::sleep(Duration::from_millis(1));
    }
    assert!(cancel.cancel());
    let outcome = recv.recv_timeout(Duration::from_secs(2));

    let mut caller = f.session();
    let competing = outcome.is_ok().then(|| {
        let deadline = Instant::now() + Duration::from_secs(2);
        crate::Cancellation::new(Some(deadline)).scope(|| {
            caller.execute_at("SELECT nextval('s')", &[], None, &clock())
        })
    });
    drop(gate);
    let mut session = client.join().unwrap();

    let error = outcome.expect("cancelled draw waited for the held root acknowledgement")
        .expect_err("a cancelled draw returned a value");
    assert_eq!(crate::sqlstate(&error), "57014");
    let error = competing.unwrap().expect_err("competing draw bypassed the rewrite lease");
    assert_eq!(crate::sqlstate(&error), "40001");
    run(&mut session, "ROLLBACK");
    assert_eq!(next(&mut caller, "s"), 12);

    drop(caller);
    drop(session);
    drop(db);
    f.reopen();
    assert_eq!(next(&mut f.session(), "s"), 44);
}

#[test]
fn staged_rewrite_draws_survive_rollback_and_rollback_to() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    assert_eq!(scalar(&mut session, "SELECT setval('s', 10)"), 10);
    run(&mut session, "BEGIN");
    run(&mut session, "SAVEPOINT before_alter");
    run(&mut session, "ALTER SEQUENCE s CACHE 2");
    assert_eq!(next(&mut session, "s"), 11);
    run(&mut session, "ROLLBACK TO before_alter");
    assert_eq!(next(&mut session, "s"), 12);
    run(&mut session, "ALTER SEQUENCE s CACHE 3");
    assert_eq!(next(&mut session, "s"), 13);
    run(&mut session, "ROLLBACK");
    assert_eq!(next(&mut session, "s"), 14);
}

#[test]
fn w4_v2_stride_change_draws_survive_rollback() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    for (start, old, new) in [(1, 10, 1), (-1, -10, -1), (1, 1, 10), (-1, -1, -10)] {
        run(&mut session, &format!("CREATE SEQUENCE s START {start} INCREMENT BY {old}"));
        assert_eq!(scalar(&mut session, &format!("SELECT setval('s', {start})")), start);
        run(&mut session, "BEGIN");
        let alter = format!("ALTER SEQUENCE s INCREMENT BY {new}");
        run(&mut session, &alter);
        assert_eq!(next(&mut session, "s"), start + new);
        run(&mut session, "ROLLBACK");
        run(&mut session, &alter);
        assert_eq!(next(&mut session, "s"), start + 2 * new, "{old} to {new}");
        run(&mut session, "DROP SEQUENCE s");
    }
}

#[test]
fn w4_v2_savepoint_restores_stride_change_draws() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    for (start, old, new) in [(1, 10, 1), (-1, -10, -1), (1, 1, 10), (-1, -1, -10)] {
        run(&mut session, &format!("CREATE SEQUENCE s START {start} INCREMENT BY {old}"));
        assert_eq!(scalar(&mut session, &format!("SELECT setval('s', {start})")), start);
        run(&mut session, "BEGIN");
        run(&mut session, "ALTER SEQUENCE s CACHE 2");
        run(&mut session, "SAVEPOINT first_rewrite");
        let alter = format!("ALTER SEQUENCE s INCREMENT BY {new}");
        run(&mut session, &alter);
        assert_eq!(next(&mut session, "s"), start + new);
        run(&mut session, "ROLLBACK TO first_rewrite");
        run(&mut session, &alter);
        assert_eq!(next(&mut session, "s"), start + 2 * new, "{old} to {new}");
        run(&mut session, "ROLLBACK");
        run(&mut session, "DROP SEQUENCE s");
    }
}

#[test]
fn staged_rewrite_crash_worker() {
    let Some(path) = std::env::var_os("STAGED_SEQUENCE_REWRITE_PATH") else {
        return;
    };
    let admin = Admin::open(PathBuf::from(path), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    assert_eq!(scalar(&mut session, "SELECT setval('s', 10)"), 10);
    run(&mut session, "BEGIN");
    run(&mut session, "ALTER SEQUENCE s CACHE 2");
    assert_eq!(next(&mut session, "s"), 11);
    crate::storage::crash_now();
}

#[test]
fn staged_rewrite_draw_remains_covered_after_process_stop_before_commit() {
    let mut f = Fixture::new(Engine::V2);
    run(&mut f.session(), "CREATE SEQUENCE s");
    f.admin.take();
    let output = Command::new(std::env::current_exe().unwrap()).args([
        "--exact",
        "storage::v2_sql::sql_sequences::reservation_tests::exposure_tests::staged_rewrite_crash_worker",
    ]).env("STAGED_SEQUENCE_REWRITE_PATH", &f.path).output().unwrap();
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    f.reopen();
    assert!(next(&mut f.session(), "s") > 11);
}

fn bounded_sequence(session: &mut SqlSession, ascending: bool) -> (&'static str, i64, i64) {
    let (increment, bound, alter) = if ascending {
        (1, 100, "ALTER SEQUENCE s MAXVALUE 200")
    } else {
        (-1, -100, "ALTER SEQUENCE s MINVALUE -200")
    };
    run(session, &format!("CREATE SEQUENCE s INCREMENT BY {increment} MINVALUE -100 MAXVALUE 100"));
    let start = bound - increment;
    assert_eq!(scalar(session, &format!("SELECT setval('s', {start})")), start);
    (alter, bound, increment)
}

fn staged_bound_draw(session: &mut SqlSession, alter: &str, bound: i64) -> Result<SqlResult, CoreError> {
    run(session, alter);
    assert_eq!(next(session, "s"), bound);
    session.execute_at("SELECT nextval('s')", &[], None, &clock())
}

#[test]
fn w5_widening_cannot_reissue_after_v2_rollback() {
    for ascending in [true, false] {
        let f = Fixture::new(Engine::V2);
        let mut session = f.session();
        let (alter, bound, increment) = bounded_sequence(&mut session, ascending);
        run(&mut session, "BEGIN");
        let extra = staged_bound_draw(&mut session, alter, bound);
        run(&mut session, "ROLLBACK");
        run(&mut session, alter);
        let next_value = next(&mut session, "s");
        assert_ne!(next_value, bound);
        assert_eq!(crate::sqlstate(&extra.unwrap_err()), "2200H");
        assert_eq!(next_value, bound + increment);
    }
}

#[test]
fn w5_v2_widening_cannot_reissue_after_savepoint() {
    for ascending in [true, false] {
        let f = Fixture::new(Engine::V2);
        let mut session = f.session();
        let (alter, bound, increment) = bounded_sequence(&mut session, ascending);
        run(&mut session, "BEGIN");
        run(&mut session, "ALTER SEQUENCE s CACHE 2");
        run(&mut session, "SAVEPOINT first_rewrite");
        let extra = staged_bound_draw(&mut session, alter, bound);
        run(&mut session, "ROLLBACK TO first_rewrite");
        // Commit the restored narrow definition, then widen it again.
        run(&mut session, "COMMIT");
        run(&mut session, alter);
        let next_value = next(&mut session, "s");
        assert_ne!(next_value, bound, "increment {increment}");
        assert_eq!(crate::sqlstate(&extra.unwrap_err()), "2200H", "increment {increment}");
        assert_eq!(next_value, bound + increment, "increment {increment}");
    }
}

#[test]
fn w5_widening_cannot_reissue_after_process_stop() {
    let test = "storage::v2_sql::sql_sequences::reservation_tests::exposure_tests::w5_widening_cannot_reissue_after_process_stop";
    if let Some(path) = std::env::var_os("STAGED_SEQUENCE_BOUNDS_PATH") {
        let admin = Admin::open(PathBuf::from(&path), Limits::default()).unwrap();
        let mut session = admin.sql_session();
        let ascending = std::env::var("STAGED_SEQUENCE_ASCENDING").unwrap() == "true";
        let (alter, bound, _) = bounded_sequence(&mut session, ascending);
        run(&mut session, "BEGIN");
        let extra = staged_bound_draw(&mut session, alter, bound);
        let seen = match extra {
            Ok(reply) => {
                let Value::Int(value) = reply.rows[0].values[0] else { panic!("nextval value") };
                value
            }
            Err(error) => {
                assert_eq!(crate::sqlstate(&error), "2200H");
                bound
            }
        };
        std::fs::write(PathBuf::from(path).join("client-seen"), seen.to_string()).unwrap();
        crate::storage::crash_now();
    }
    for ascending in [true, false] {
        let mut f = Fixture::new(Engine::V2);
        f.admin.take();
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env("STAGED_SEQUENCE_BOUNDS_PATH", &f.path)
            .env("STAGED_SEQUENCE_ASCENDING", ascending.to_string())
            .output().unwrap();
        assert_eq!(output.status.code(), Some(86), "{output:?}");
        let seen: i64 = std::fs::read_to_string(f.path.join("client-seen")).unwrap().parse().unwrap();
        f.reopen();
        let mut session = f.session();
        if ascending {
            run(&mut session, "ALTER SEQUENCE s MAXVALUE 200");
            assert!(next(&mut session, "s") > seen);
        } else {
            run(&mut session, "ALTER SEQUENCE s MINVALUE -200");
            assert!(next(&mut session, "s") < seen);
        }
    }
}

#[test]
fn w5_v2_rejects_reversed_draw_until_alter_commits() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s MINVALUE 1 MAXVALUE 100");
    assert_eq!(scalar(&mut session, "SELECT setval('s', 10)"), 10);
    run(&mut session, "BEGIN");
    run(&mut session, "ALTER SEQUENCE s INCREMENT BY -1");
    let staged = session.execute_at("SELECT nextval('s')", &[], None, &clock());
    run(&mut session, "ROLLBACK");
    run(&mut session, "ALTER SEQUENCE s INCREMENT BY -1");
    assert_eq!(next(&mut session, "s"), 9);
    assert_eq!(crate::sqlstate(&staged.unwrap_err()), "0A000");
}

#[test]
fn w6_staged_cycle_wrap_is_refused_before_rollback() {
    for rollback in ["ROLLBACK", "ROLLBACK TO before_alter"] {
        let f = Fixture::new(Engine::V2);
        let mut session = f.session();
        run(&mut session, "CREATE SEQUENCE s MAXVALUE 10 START WITH 10 NO CYCLE");
        assert_eq!(next(&mut session, "s"), 10);
        run(&mut session, "BEGIN");
        run(&mut session, "SAVEPOINT before_alter");
        run(&mut session, "ALTER SEQUENCE s CYCLE");
        let staged = session.execute_at("SELECT nextval('s')", &[], None, &clock());
        run(&mut session, rollback);
        if session.in_transaction() {
            run(&mut session, "COMMIT");
        }
        run(&mut session, "ALTER SEQUENCE s CYCLE");
        assert_eq!(next(&mut session, "s"), 1);
        assert_eq!(next(&mut session, "s"), 2);
        assert_eq!(crate::sqlstate(&staged.unwrap_err()), "0A000", "{rollback}");
    }
}

#[test]
fn w6_staged_cycle_wrap_is_refused_before_process_stop() {
    const TEST: &str = "storage::v2_sql::sql_sequences::reservation_tests::exposure_tests::w6_staged_cycle_wrap_is_refused_before_process_stop";
    if let Some(path) = std::env::var_os("STAGED_SEQUENCE_CYCLE_PATH") {
        let path = PathBuf::from(path);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut session = admin.sql_session();
        run(&mut session, "CREATE SEQUENCE s MINVALUE -10 MAXVALUE -1 START WITH -10 INCREMENT BY -1");
        assert_eq!(next(&mut session, "s"), -10);
        run(&mut session, "BEGIN");
        run(&mut session, "ALTER SEQUENCE s CYCLE");
        let draw = session.execute_at("SELECT nextval('s')", &[], None, &clock());
        let result = match draw {
            Ok(reply) => format!("{:?}", reply.rows[0].values[0]),
            Err(error) => crate::sqlstate(&error).to_owned(),
        };
        std::fs::write(path.join("cycle-draw"), result).unwrap();
        crate::storage::crash_now();
    }
    let mut f = Fixture::new(Engine::V2);
    f.admin.take();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env("STAGED_SEQUENCE_CYCLE_PATH", &f.path)
        .output().unwrap();
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    f.reopen();
    let mut session = f.session();
    run(&mut session, "ALTER SEQUENCE s CYCLE");
    assert_eq!(next(&mut session, "s"), -1);
    assert_eq!(std::fs::read_to_string(f.path.join("cycle-draw")).unwrap(), "0A000");
}

#[test]
fn w6_abandoned_and_new_sequence_definitions_do_not_constrain_draws() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s START WITH 10 MAXVALUE 100");
    assert_eq!(next(&mut session, "s"), 10);
    run(&mut session, "BEGIN");
    run(&mut session, "SAVEPOINT older");
    run(&mut session, "ALTER SEQUENCE s MAXVALUE 10");
    run(&mut session, "ALTER SEQUENCE s MAXVALUE 100");
    assert_eq!(next(&mut session, "s"), 11);
    run(&mut session, "ROLLBACK");
    assert_eq!(next(&mut session, "s"), 12, "the committed root still gets coverage");

    run(&mut session, "BEGIN");
    run(&mut session, "CREATE SEQUENCE private_s MAXVALUE 10 START WITH 10");
    run(&mut session, "ALTER SEQUENCE private_s MAXVALUE 100");
    assert_eq!(next(&mut session, "private_s"), 10);
    assert_eq!(next(&mut session, "private_s"), 11);
    run(&mut session, "ROLLBACK");
}

#[test]
fn w6_only_live_savepoints_constrain_draws() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s START WITH 10 MAXVALUE 100");
    assert_eq!(next(&mut session, "s"), 10);
    run(&mut session, "BEGIN");
    run(&mut session, "ALTER SEQUENCE s MAXVALUE 10");
    run(&mut session, "SAVEPOINT keep");
    run(&mut session, "SAVEPOINT inner_mark");
    run(&mut session, "ALTER SEQUENCE s MAXVALUE 100");
    let refused = session.execute_at("SELECT nextval('s')", &[], None, &clock());
    assert_eq!(crate::sqlstate(&refused.unwrap_err()), "2200H");
    run(&mut session, "ROLLBACK TO inner_mark");
    run(&mut session, "RELEASE inner_mark");
    run(&mut session, "ALTER SEQUENCE s MAXVALUE 100");
    let refused = session.execute_at("SELECT nextval('s')", &[], None, &clock());
    assert_eq!(crate::sqlstate(&refused.unwrap_err()), "2200H");
    run(&mut session, "ROLLBACK TO keep");
    run(&mut session, "RELEASE keep");
    run(&mut session, "ALTER SEQUENCE s MAXVALUE 100");
    assert_eq!(next(&mut session, "s"), 11);
}

#[test]
fn w4_stride_change_crash_worker() {
    let Some(path) = std::env::var_os("STAGED_SEQUENCE_STRIDE_PATH") else {
        return;
    };
    let admin = Admin::open(PathBuf::from(path), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    assert_eq!(scalar(&mut session, "SELECT setval('s', 1)"), 1);
    run(&mut session, "BEGIN");
    run(&mut session, "ALTER SEQUENCE s INCREMENT BY 1");
    assert_eq!(next(&mut session, "s"), 2);
    crate::storage::crash_now();
}

#[test]
fn w4_stride_change_draw_survives_process_stop() {
    let mut f = Fixture::new(Engine::V2);
    run(&mut f.session(), "CREATE SEQUENCE s START 1 INCREMENT BY 10");
    f.admin.take();
    let output = Command::new(std::env::current_exe().unwrap()).args([
        "--exact",
        "storage::v2_sql::sql_sequences::reservation_tests::exposure_tests::w4_stride_change_crash_worker",
    ]).env("STAGED_SEQUENCE_STRIDE_PATH", &f.path).output().unwrap();
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    f.reopen();
    let mut session = f.session();
    run(&mut session, "ALTER SEQUENCE s INCREMENT BY 1");
    assert!(next(&mut session, "s") > 2);
}

#[test]
fn v2_sequence_observations_follow_logical_id_across_alter() {
    let f = Fixture::new(Engine::V2);
    let mut ddl = f.session();
    let mut other = f.session();
    run(&mut ddl, "CREATE SEQUENCE s");
    assert_eq!(next(&mut ddl, "s"), 1);
    assert_eq!(next(&mut other, "s"), 2);
    run(&mut ddl, "ALTER SEQUENCE s CACHE 2");
    for (session, value) in [(&mut ddl, 1), (&mut other, 2)] {
        assert_eq!(scalar(session, "SELECT currval('s')"), value);
        assert_eq!(scalar(session, "SELECT lastval()"), value);
    }
    run(&mut ddl, "DROP SEQUENCE s");
    run(&mut ddl, "CREATE SEQUENCE s");
    for query in ["SELECT currval('s')", "SELECT lastval()"] {
        let error = other.execute_at(query, &[], None, &clock()).unwrap_err();
        assert_eq!(crate::sqlstate(&error), "55000");
    }
}

#[test]
fn w4_repeatable_read_sequence_observations_use_live_authority_after_alter() {
    let f = Fixture::new(Engine::V2);
    let mut reader = f.session();
    let mut ddl = f.session();
    let roles = f.admin.as_ref().unwrap().roles();
    for name in ["observer", "replacement_owner"] {
        roles.create(roles::NewRole { name: name.into(), ..Default::default() }).unwrap();
    }
    run(&mut ddl, "CREATE SEQUENCE s");
    roles.set_table_owner("s", "observer").unwrap();
    reader.set_sequence_authority(SequenceAuthority::Role("observer".into()));
    reader.set_isolation(crate::SqlIsolation::RepeatableRead);
    run(&mut reader, "BEGIN");
    assert_eq!(next(&mut reader, "s"), 1);
    run(&mut ddl, "ALTER SEQUENCE s CACHE 2");
    assert_eq!(scalar(&mut reader, "SELECT currval('s')"), 1);
    assert_eq!(scalar(&mut reader, "SELECT lastval()"), 1);

    roles.set_table_owner("s", "replacement_owner").unwrap();
    let error = reader.execute_at("SELECT currval('s')", &[], None, &clock()).unwrap_err();
    assert_eq!(crate::sqlstate(&error), "42501");
}

#[test]
fn autocommit_rewrite_retires_counter_while_old_write_is_held_before_submission() {
    let mut f = Fixture::new(Engine::V2);
    let mut alter = f.session();
    run(&mut alter, "CREATE SEQUENCE s");
    assert_eq!(scalar(&mut alter, "SELECT setval('s', 10)"), 10);
    let db = f.admin.as_ref().unwrap().test_v2();
    let view = db.catalog().current_local_view().unwrap();
    let (&id, old) = view.sequences.iter().next().unwrap();
    let key = engine::catalog_codec::counter_key(id, old.incarnation);
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut caller = f.session();
    let (send, recv) = mpsc::channel();
    let client = thread::spawn(move || send.send(next(&mut caller, "s")).unwrap());
    let early = before_settlement(&sequence_slot(&db, "s"), &recv);
    let rewrite = alter.execute_at("ALTER SEQUENCE s CACHE 2", &[], None, &clock());
    drop(gate);
    client.join().unwrap();
    assert_eq!(early, None);
    rewrite.unwrap();
    assert_eq!(recv.recv_timeout(WAIT).unwrap(), 11);
    assert!(db.kv.get(Keyspace::Seq, &key).unwrap().is_none());
    drop(db);
    drop(alter);
    f.reopen();
    assert_eq!(next(&mut f.session(), "s"), 12);
}

#[test]
fn v2_once_setup_leaves_legacy_sequence_baseline_empty() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    session.begin_once_engine(None).unwrap();
    assert!(session.sequence_baseline.is_empty());
}
