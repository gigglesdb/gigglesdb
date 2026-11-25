use super::*;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(10);

const DRAW: i64 = 54_321;
const BAD_DEFAULT: &str =
    "CREATE TABLE bad_default(id uuid PRIMARY KEY DEFAULT (nextval('s')::text)::uuid, payload bigint)";
const CHECKED: &str = "CREATE TABLE checked(id bigint PRIMARY KEY DEFAULT nextval('s'))";

fn prepare_sequence(session: &mut SqlSession, sequence: &str) {
    run(
        session,
        &format!("CREATE SEQUENCE {sequence} START {}", DRAW - 33),
    );
    assert_eq!(next(session, sequence), DRAW - 33);
}

fn exhaust_coverage(mut session: SqlSession) -> SqlSession {
    spawn(move || {
        for expected in DRAW - 32..DRAW {
            assert_eq!(next(&mut session, "s"), expected);
        }
        session
    })
    .recv_timeout(WAIT)
    .expect("covered sequence draws stalled behind a held acknowledgement")
}

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
        if slot.counter.try_observe(|state| state.waiting_returns != 0) == Some(true) {
            return received.try_recv().ok();
        }
        assert!(Instant::now() < deadline, "caller never drew or returned");
        thread::sleep(Duration::from_millis(1));
    }
}

fn spawn<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
    let (send, received) = mpsc::channel();
    thread::spawn(move || {
        let _ = send.send(work());
    });
    received
}

fn checked_insert(session: &mut SqlSession, report: impl FnOnce(i64)) -> Result<(), CoreError> {
    let prepared = session
        .prepare("INSERT INTO checked DEFAULT VALUES RETURNING id")
        .unwrap();
    let mut report = Some(report);
    let mut check = |result: crate::task::Released<&SqlResult>,
                     _: Option<&mut relational::spill::SpilledRows>| {
        let Value::Int(id) = result.get().rows[0].values[0] else {
            panic!("sequence result")
        };
        report.take().unwrap()(id);
        Err(CoreError::SqlReplyEncoding("test rejection"))
    };
    session
        .execute_prepared_at_checked_unreleased(&prepared, &[], None, &clock(), Some(&mut check))
        .map(drop)
}

fn normalize(session: &mut SqlSession, table: &str) -> Result<i64, CoreError> {
    let (_, prepared) = session
        .prepare_copy(&format!("COPY {table} (payload) FROM STDIN"))
        .unwrap();
    let (_, rows) =
        session.normalize_copy_rows_at(&prepared, vec![vec![Value::Int(1)]], &clock())?;
    let Value::Int(id) = rows[0][0] else {
        panic!("sequence default")
    };
    Ok(id)
}

#[test]
fn checked_reply_waits_for_reservation_even_if_it_rejects_the_write() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    run(&mut session, CHECKED);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_acks(false));
    let mut session = exhaust_coverage(session);

    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || checked_insert(&mut session, |id| send.send(id).unwrap()));
    let early = before_settlement(&slot, &recv);
    drop(gate);

    assert_eq!(
        client.recv_timeout(WAIT).unwrap(),
        Err(CoreError::SqlReplyEncoding("test rejection"))
    );
    assert_eq!(early, None);
    assert_eq!(recv.recv_timeout(WAIT).unwrap(), DRAW);
    assert_eq!(scalar(&mut f.session(), "SELECT count(*) FROM checked"), 0);
}

#[test]
fn copy_normalization_waits_for_reservation() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    run(
        &mut session,
        "CREATE TABLE copied(id bigint PRIMARY KEY DEFAULT nextval('s'), payload bigint)",
    );
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || send.send(normalize(&mut session, "copied")).unwrap());
    let early = before_settlement(&slot, &recv);
    drop(gate);
    client.recv_timeout(WAIT).unwrap();

    assert_eq!(early, None);
    assert_eq!(recv.recv_timeout(WAIT).unwrap(), Ok(DRAW));
}

#[test]
fn buffered_copy_page_does_not_expose_normalized_cells() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    run(
        &mut session,
        "CREATE TABLE buffered(id bigint PRIMARY KEY DEFAULT nextval('s'), payload bigint)",
    );
    let (_, prepared) = session.prepare_copy("COPY buffered (payload) FROM STDIN").unwrap();
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let client = spawn(move || {
        let input = relational::CopyInput {
            rows: vec![vec![Some(Value::Int(42))]],
            options: Default::default(),
            first_line: 1,
            skipped: 0,
        };
        let (_, rows, skipped) = session
            .normalize_buffered_copy_cells_at(&prepared, input, &clock())
            .unwrap();
        // This also compiles against the old API, whose concrete return type was Vec.
        let exposed = (&rows as &dyn std::any::Any)
            .downcast_ref::<Vec<Vec<Value>>>()
            .map(|rows| rows[0][0].clone());
        (skipped, !session.sequence_unsettled.is_empty(), exposed)
    });
    let early = client.recv_timeout(WAIT);
    drop(gate);
    assert_eq!(early.expect("buffering waited for sequence settlement"), (0, true, None));
}

#[test]
fn buffered_copy_ingest_waits_for_sequence_coverage() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    run(
        &mut session,
        "CREATE TABLE buffered(id bigint PRIMARY KEY DEFAULT nextval('s'), payload bigint)",
    );
    let (_, prepared) = session.prepare_copy("COPY buffered (payload) FROM STDIN").unwrap();
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let (buffered, buffered_received) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let reply = spawn(move || {
        let input = relational::CopyInput {
            rows: vec![vec![Some(Value::Int(42))]],
            options: Default::default(),
            first_line: 1,
            skipped: 0,
        };
        let (prepared, rows, skipped) = session
            .normalize_buffered_copy_cells_at(&prepared, input, &clock())
            .unwrap();
        assert_eq!(skipped, 0);
        buffered.send(()).unwrap();
        session.ingest_buffered_copy_at(&prepared, rows, &clock())
    });
    buffered_received.recv_timeout(WAIT).expect("COPY page did not buffer");
    let early = before_settlement(&slot, &reply);
    let mut reader = f.session();
    let count = spawn(move || scalar(&mut reader, "SELECT count(*) FROM buffered"));
    let count = count.recv_timeout(WAIT);
    drop(gate);

    assert_eq!(early, None, "COPY ingest replied while its reservation was held");
    assert_eq!(count.expect("COPY hid the table while settlement was pending"), 0);
    assert_eq!(reply.recv_timeout(WAIT).unwrap().unwrap().affected, 1);
    assert_eq!(
        run(&mut f.session(), "SELECT id, payload FROM buffered").rows[0].values,
        [Value::Int(DRAW), Value::Int(42)],
    );
}

#[test]
fn streaming_copy_skipped_count_waits_for_reservation() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    run(
        &mut session,
        "CREATE TABLE copied(id bigint PRIMARY KEY DEFAULT nextval('s'), payload bigint)",
    );
    let (_, prepared) = session.prepare_copy("COPY copied (payload) FROM STDIN").unwrap();
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        assert!(session.begin_copy_input().unwrap());
        let input = relational::CopyInput {
            rows: vec![vec![Some(Value::Text("bad integer".into()))]],
            options: relational::CopyOptions {
                on_error_ignore: true,
                log_verbosity: relational::CopyLogVerbosity::Silent,
                ..Default::default()
            },
            first_line: 1,
            skipped: 0,
        };
        session.normalize_and_feed_copy_input_at(&prepared, input, &clock())
            .map(|(_, skipped)| skipped)
    });
    let early = before_settlement(&slot, &client);
    drop(gate);
    assert_eq!(early, None, "streaming COPY exposed its skipped count before settlement");
    assert_eq!(client.recv_timeout(WAIT).unwrap(), Ok(1));
}

#[test]
fn privilege_inquiry_arguments_wait_for_reservation() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        let callback: PrivilegeCallback = Arc::new(move |name, args, _| {
            assert_eq!(name, "pg_has_role");
            send.send(args[0].clone()).unwrap();
            Ok(Some(true))
        });
        session.with_privilege_callback(callback, |session| {
            run(session, "SELECT pg_has_role(nextval('s')::text, 'MEMBER')")
        })
    });
    let early = before_settlement(&slot, &recv);
    drop(gate);

    assert_eq!(client.recv_timeout(WAIT).unwrap().rows[0].values, [Value::Bool(true)]);
    assert_eq!(early, None);
    assert_eq!(
        recv.recv_timeout(WAIT).unwrap(),
        Value::Text(DRAW.to_string())
    );
}

fn diagnostic_fields() -> DiagnosticFields {
    let (message, context) = relational::take_sql_error_message_with_context();
    (
        message,
        context,
        relational::take_sql_error_detail(),
        relational::take_sql_diagnostic_hint().map(|hint| hint.into_owned()),
    )
}

fn failed_settlement_diagnostics(
    ddl: Option<&str>,
    execute: impl FnOnce(&mut SqlSession) -> Result<(), String> + Send + 'static,
) -> (Result<(), String>, DiagnosticFields) {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    if let Some(ddl) = ddl {
        run(&mut session, ddl);
    }
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let cancellation = crate::Cancellation::default();
    let context = cancellation.clone();
    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        let reply = context.scope(|| execute(&mut session));
        let fields = diagnostic_fields();
        assert!(relational::take_sql_notices().is_empty());
        send.send((reply, fields)).unwrap();
    });
    let early = before_settlement(&slot, &recv);
    cancellation.cancel();
    let observed = early.unwrap_or_else(|| recv.recv_timeout(WAIT).unwrap());
    drop(gate);
    client.recv_timeout(WAIT).unwrap();
    observed
}

type DiagnosticFields = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[test]
fn failed_settlement_never_calls_the_reply_check() {
    let called = Arc::new(AtomicBool::new(false));
    let observed = called.clone();
    let (reply, fields) = failed_settlement_diagnostics(Some(CHECKED), move |session| {
        checked_insert(session, |_| observed.store(true, Ordering::Relaxed))
            .map_err(|error| crate::sqlstate(&error).to_owned())
    });
    assert!(!called.load(Ordering::Relaxed));
    assert_eq!(reply, Err("57014".to_owned()));
    assert_eq!(fields, (None, None, None, None));
}

#[test]
fn failed_settlement_discards_sql_error_diagnostics() {
    let (reply, fields) = failed_settlement_diagnostics(None, |session| {
        session
            .execute_at("SELECT nextval('s')::text::uuid", &[], None, &clock())
            .map(drop)
            .map_err(|error| crate::sqlstate(&error).to_owned())
    });
    assert_eq!(reply, Err("57014".to_owned()));
    assert_eq!(fields, (None, None, None, None));
}

#[test]
fn failed_settlement_discards_copy_error_diagnostics() {
    let (reply, fields) = failed_settlement_diagnostics(Some(BAD_DEFAULT), |session| {
        let (_, prepared) = session
            .prepare_copy("COPY bad_default (payload) FROM STDIN")
            .unwrap();
        session
            .normalize_copy_rows_at(&prepared, vec![vec![Value::Int(1)]], &clock())
            .map(drop)
            .map_err(|error| crate::sqlstate(&error).to_owned())
    });
    assert_eq!(reply, Err("57014".to_owned()));
    assert_eq!(fields, (None, None, None, None));
}

#[test]
fn failed_settlement_discards_native_default_error_diagnostics() {
    use crate::operations::{Request, ValueExpr};
    let (reply, fields) = failed_settlement_diagnostics(Some(BAD_DEFAULT), |session| {
        let request = Request::Insert {
            table: "bad_default".into(),
            values: vec![("payload".into(), ValueExpr::Literal(Value::Int(1)))],
        };
        session
            .execute_native(&request, &Default::default())
            .map(drop)
            .map_err(|error| format!("{error:?}"))
    });
    assert_eq!(fields, (None, None, None, None));
    assert!(matches!(&reply, Err(error) if error.contains("Cancelled")), "{reply:?}");
}

#[test]
fn failed_settlement_keeps_08007_without_the_original_error_text() {
    let (reply, fields) = failed_settlement_diagnostics(Some(CHECKED), |session| {
        // Inject inside execution, since an external callback now waits for coverage.
        super::super::AFTER_COLUMN_DEFAULT_DRAW.set(Some(Box::new(|value| {
            assert_eq!(value, DRAW);
            relational::prepend_sql_notices(vec![(format!("draw {value}"), None, Some("00000"))]);
            Err(CoreError::Storage(format!("after draw {value}")))
        })));
        session
            .execute_at("INSERT INTO checked DEFAULT VALUES", &[], None, &clock())
            .map(drop)
            .map_err(|error| format!("{} {error}", crate::sqlstate(&error)))
    });
    assert_eq!(fields, (None, None, None, None));
    let error = reply.unwrap_err();
    assert!(error.starts_with("08007 "), "{error}");
    assert!(!error.contains(&DRAW.to_string()), "{error}");
}

#[test]
fn failed_native_visits_and_scan_clear_pending_sequence_diagnostics() {
    for access in ["key", "range", "index", "scan"] {
        let (reply, fields) = failed_settlement_diagnostics(Some(CHECKED), move |session| {
            session.begin_block().unwrap();
            let db = session.native_sequence_lease().unwrap().unwrap();
            let (image, draw) = session.native_sequence_callback(Some(&db)).unwrap();
            let value = draw("nextval", "s", None, false, relational::clock::SequenceOrigin::Call).unwrap();
            assert_eq!(value, Value::Int(DRAW));
            drop(draw);
            assert!(!session.sequence_unsettled.is_empty());
            let table = session.admin.catalog().unwrap().1.remove(0);
            let mut column = table.columns[0].clone();
            column.data_type = relational::DataType::Uuid;
            assert!(relational::coerce_to_column(&image, &column, Value::Text(DRAW.to_string())).is_err());
            let mut visit = |_: &relational::SqlRow| -> Result<(), CoreError> {
                panic!("native visit exposed a row before coverage");
            };
            let result = match access {
                "key" => session.visit_native_key_v2(&db, &table, &[value], &mut visit, |e| e),
                "range" => session.visit_native_range_v2(&db, &table, std::ops::Bound::Unbounded,
                    std::ops::Bound::Unbounded, &mut visit, |e| e),
                "index" => session.visit_native_index_v2(&db, &table,
                    (&relational::secondary_index::Descriptor {
                        id: 1, table: table.id, name: "checked_id".into(),
                        columns: vec![table.columns[0].id], key_encoding: 1,
                        unique: false, predicate: None,
                    }, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
                    &mut visit, |e| e),
                "scan" => session.scan_v2_engine(&db, relational::Scan {
                    table: "checked".into(), projection: vec![], prefix: vec![],
                    lower: None, upper: None, reverse: false, limit: 1,
                }).map(drop),
                _ => unreachable!(),
            };
            result.map_err(|error| crate::sqlstate(&error).to_owned())
        });
        assert_eq!(reply, Err("57014".into()), "{access}");
        assert_eq!(fields, (None, None, None, None), "{access}");
    }
}

#[test]
fn notice_only_statement_waits_for_reservation() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    run(&mut session, "CREATE TABLE notices(query tsquery)");
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        run(&mut session, "BEGIN");
        // The parser echoes the whitespace-only query in its notice. Its length
        // reveals the low digit of the sequence value, even without RETURNING.
        run(
            &mut session,
            "INSERT INTO notices SELECT to_tsquery('simple', repeat(' ', (nextval('s') % 10)::int))",
        );
        send.send(relational::take_sql_notices()).unwrap();
    });
    let early = before_settlement(&slot, &recv);
    drop(gate);
    client.recv_timeout(WAIT).unwrap();

    assert_eq!(early, None);
    assert_eq!(
        recv.recv_timeout(WAIT).unwrap(),
        [(
            "text-search query doesn't contain lexemes: \" \"".to_owned(),
            None,
            Some("00000")
        )],
    );
}

#[test]
fn draw_dependent_wall_clock_callback_waits_for_reservation() {
    for cancel in [false, true] {
        let f = Fixture::new(Engine::V2);
        let mut session = f.session();
        prepare_sequence(&mut session, "s");
        let db = f.admin.as_ref().unwrap().test_v2();
        db.sequencer.gate_counter_writes(true);
        let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
        let mut session = exhaust_coverage(session);

        let cancellation = crate::Cancellation::default();
        let context = cancellation.clone();
        let (send, recv) = mpsc::channel();
        let slot = sequence_slot(&db, "s");
        let client = spawn(move || {
            let mut clock = clock();
            clock.wall = Some(Arc::new(move || {
                send.send(()).unwrap();
                Ok(100)
            }));
            context.scope(|| session.execute_at(
                &format!("SELECT CASE WHEN nextval('s') = {DRAW} THEN clock_timestamp() END"),
                &[], None, &clock,
            ))
        });
        let early = before_settlement(&slot, &recv);
        if cancel {
            cancellation.cancel();
        } else {
            drop(gate);
            let reply = client.recv_timeout(WAIT).unwrap().unwrap();
            assert!(matches!(reply.rows[0].values[0], Value::TimestampTz(_)));
            assert_eq!(early, None, "wall callback ran before the reservation settled");
            recv.recv_timeout(WAIT).unwrap();
            continue;
        }
        let reply = client.recv_timeout(WAIT);
        drop(gate);
        assert_eq!(reply.unwrap().unwrap_err(), CoreError::Cancelled);
        assert_eq!(early, None, "wall callback ran before settlement failed");
        assert_eq!(recv.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    }
}

#[test]
fn buffered_copy_wall_clock_callback_waits_for_reservation() {
    for cancel in [false, true] {
        let f = Fixture::new(Engine::V2);
        let mut session = f.session();
        prepare_sequence(&mut session, "s");
        run(
            &mut session,
            "CREATE TABLE copied(id bigint DEFAULT nextval('s'), stamp timestamptz DEFAULT clock_timestamp(), payload bigint)",
        );
        let (_, prepared) = session.prepare_copy("COPY copied (payload) FROM STDIN").unwrap();
        let db = f.admin.as_ref().unwrap().test_v2();
        db.sequencer.gate_counter_writes(true);
        let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
        let mut session = exhaust_coverage(session);

        let cancellation = crate::Cancellation::default();
        let context = cancellation.clone();
        let (send, recv) = mpsc::channel();
        let slot = sequence_slot(&db, "s");
        let client = spawn(move || {
            let mut clock = clock();
            clock.wall = Some(Arc::new(move || {
                send.send(()).unwrap();
                Ok(100)
            }));
            let input = relational::CopyInput {
                rows: vec![vec![Some(Value::Int(42))]],
                options: Default::default(),
                first_line: 1,
                skipped: 0,
            };
            context.scope(|| session.normalize_buffered_copy_cells_at(&prepared, input, &clock))
                .map(|(_, _, skipped)| skipped)
        });
        let early = before_settlement(&slot, &recv);
        if cancel {
            cancellation.cancel();
            let reply = client.recv_timeout(WAIT);
            drop(gate);
            assert_eq!(early, None, "COPY called the wall clock before settlement failed");
            assert_eq!(reply.unwrap(), Err(CoreError::Cancelled));
            assert_eq!(recv.try_recv(), Err(mpsc::TryRecvError::Disconnected));
        } else {
            drop(gate);
            assert_eq!(early, None, "COPY called the wall clock before the reservation settled");
            assert_eq!(client.recv_timeout(WAIT).unwrap(), Ok(0));
            recv.recv_timeout(WAIT).unwrap();
        }
    }
}

#[test]
fn wall_clock_callback_cannot_drain_an_inflight_sequence_notice() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    prepare_sequence(&mut session, "s");
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let mut session = exhaust_coverage(session);

    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        let mut clock = clock();
        clock.wall = Some(Arc::new(move || {
            send.send(relational::take_sql_notices()).unwrap();
            Ok(100)
        }));
        let reply = session.execute_at(
            "SELECT to_tsquery('simple', repeat(' ', (nextval('s') % 10)::int)), clock_timestamp()",
            &[],
            None,
            &clock,
        );
        (reply, relational::take_sql_notices())
    });
    let early = before_settlement(&slot, &recv);
    drop(gate);

    let (reply, notices) = client.recv_timeout(WAIT).unwrap();
    assert!(reply.is_ok(), "{reply:?}");
    assert_eq!(early, None);
    assert_eq!(recv.recv_timeout(WAIT).unwrap(), []);
    assert_eq!(
        notices,
        [(
            "text-search query doesn't contain lexemes: \" \"".to_owned(),
            None,
            Some("00000")
        )]
    );
}

#[test]
fn rollback_does_not_wait_for_pending_coverage() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    run(&mut session, CHECKED);
    assert_eq!(scalar(&mut session, "SELECT setval('s', 2)"), 2);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    session.begin_block().unwrap();
    let (_, draw) = session.native_sequence_callback(Some(&db)).unwrap();
    assert_eq!(draw("nextval", "s", None, false, relational::clock::SequenceOrigin::Call).unwrap(), Value::Int(3));
    drop(draw);
    assert!(!session.sequence_unsettled.is_empty());

    let done = spawn(move || {
        let reply = session.execute_at("ROLLBACK", &[], None, &clock());
        (reply.map(drop), session.in_transaction(), diagnostic_fields(), session)
    });
    let rolled_back = done.recv_timeout(Duration::from_secs(2));
    drop(gate);
    if rolled_back.is_err() {
        // Let the worker release its database handles before fixture cleanup.
        drop(done.recv_timeout(WAIT));
    }
    let (reply, in_transaction, fields, mut session) =
        rolled_back.expect("SQL ROLLBACK waited for an acknowledgement with no deadline");
    assert_eq!(reply, Ok(()));
    assert!(!in_transaction);
    assert_eq!(fields, (None, None, None, None));
    assert!(!session.sequence_unsettled.is_empty());
    assert_eq!(scalar(&mut session, "SELECT currval('s')"), 3);
    assert!(session.sequence_unsettled.is_empty());
}

#[test]
fn dropping_a_session_with_a_held_reservation_releases_its_transaction() {
    let mut f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    assert_eq!(scalar(&mut session, "SELECT setval('s', 2)"), 2);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_acks(false));
    let cancellation = crate::Cancellation::default();
    let context = cancellation.clone();
    let (send, received) = mpsc::channel();
    let (dropped, done) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        run(&mut session, "BEGIN");
        let reply = context.scope(|| session.execute_at("SELECT nextval('s')", &[], None, &clock()));
        send.send(reply.map(drop)).unwrap();
        drop(session);
        dropped.send(()).unwrap();
    });
    assert_eq!(before_settlement(&slot, &received), None);
    cancellation.cancel();
    assert_eq!(crate::sqlstate(&received.recv_timeout(WAIT).unwrap().unwrap_err()), "57014");
    let dropped_while_held = done.recv_timeout(Duration::from_secs(2));
    drop(gate);
    client.recv_timeout(WAIT).unwrap();
    assert_eq!(dropped_while_held, Ok(()));
    assert_eq!(f.admin.as_ref().unwrap().inner.sql_transactions.load(Ordering::Acquire), 0);
    let after_release = next(&mut f.session(), "s");
    assert!(after_release > 3);
    drop(db);
    f.reopen();
    assert!(next(&mut f.session(), "s") > after_release);
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
    let client = spawn(move || {
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
    let mut session = client.recv_timeout(WAIT).unwrap();

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
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || send.send(next(&mut caller, "s")).unwrap());
    let early = before_settlement(&slot, &recv);
    let rewrite = alter.execute_at("ALTER SEQUENCE s CACHE 2", &[], None, &clock());
    drop(gate);
    client.recv_timeout(WAIT).unwrap();
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

#[test]
fn explicit_command_count_waits_for_reservation() {
    explicit_count_waits_for_reservation(
        "INSERT INTO dst SELECT g FROM generate_series(1, nextval('s')) AS series(g)",
    );
}

#[test]
fn explicit_create_as_count_waits_for_reservation() {
    explicit_count_waits_for_reservation(
        "CREATE TABLE copied AS SELECT g FROM generate_series(1, nextval('s')) AS series(g)",
    );
}

fn explicit_count_waits_for_reservation(statement: &'static str) {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    run(&mut session, "CREATE TABLE dst(id bigint PRIMARY KEY)");
    assert_eq!(scalar(&mut session, "SELECT setval('s', 2)"), 2);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        run(&mut session, "BEGIN");
        let result = session.execute_at(statement, &[], None, &clock())
            .map(|result| result.affected);
        send.send(result).unwrap();
    });
    let early = before_settlement(&slot, &recv);
    drop(gate);
    client.recv_timeout(WAIT).unwrap();
    assert_eq!(early, None);
    assert_eq!(recv.recv_timeout(WAIT).unwrap(), Ok(3));
}

#[test]
fn w5_explicit_default_insert_waits_for_reservation() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    run(&mut session, "CREATE TABLE checked(id bigint PRIMARY KEY DEFAULT nextval('s') CHECK (id = 3))");
    assert_eq!(scalar(&mut session, "SELECT setval('s', 2)"), 2);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        session.begin_block().unwrap();
        send.send(run(&mut session, "INSERT INTO checked DEFAULT VALUES").affected).unwrap();
        session.commit_block().unwrap();
    });
    let early = before_settlement(&slot, &recv);
    drop(gate);
    client.recv_timeout(WAIT).unwrap();
    assert_eq!(early, None);
    assert_eq!(recv.recv_timeout(WAIT).unwrap(), 1);
}

#[test]
fn w4_conflict_count_waits_for_default_draw() {
    let f = Fixture::new(Engine::V2);
    let mut session = f.session();
    run(&mut session, "CREATE SEQUENCE s");
    run(&mut session, CHECKED);
    run(&mut session, "INSERT INTO checked VALUES (3)");
    assert_eq!(scalar(&mut session, "SELECT setval('s', 2)"), 2);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    let gate = ReleaseGate(|| db.sequencer.gate_counter_writes(false));
    let (send, recv) = mpsc::channel();
    let slot = sequence_slot(&db, "s");
    let client = spawn(move || {
        session.begin_block().unwrap();
        let affected = run(&mut session, "INSERT INTO checked VALUES (DEFAULT) ON CONFLICT DO NOTHING").affected;
        send.send(affected).unwrap();
    });
    let early = before_settlement(&slot, &recv);
    drop(gate);
    client.recv_timeout(WAIT).unwrap();
    assert_eq!(early, None);
    assert_eq!(recv.recv_timeout(WAIT).unwrap(), 0);
}
