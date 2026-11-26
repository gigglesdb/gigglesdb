//! GD-0061 R3-311: the simple-query path, SQL cursors, COPY TO STDOUT and the
//! transaction helper go through the release protocol. No task row reaches
//! the wire unreleased. The non-task exchange pins the exact wire frames.
//!
//! Over the agent listener a task SELECT runs (`Select` is in
//! `task::admit::ENABLED` since R3-315) and is released before it is
//! encoded; DECLARE, FETCH, MOVE and COPY are refused as their class before
//! any table read. These wire tests prove the release and that those
//! refusals are logged once, charge nothing and put no row (and no CopyOut
//! header) on the wire. The per-chunk, per-fetch and before-the-header
//! releases of a task's rows are also proven by the server-lib tests of the
//! same names (`pgwire::task_release_tests`).
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{AgentServer, Msg, Wire, agent_wire, error_of, ordinary_wire};
use giggles_core::{
    roles::TablePrivilege,
    task::{
        StatementClass, TaskError,
        readlog::{ENTRY_PGWIRE_SIMPLE, class_code, outcome_of},
    },
};

/// Five task-visible rows and one other, readable and updatable by the
/// ordinary login; the executor holds SELECT.
fn seed(server: &AgentServer) {
    server
        .admin
        .sql_session()
        .execute(
            "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'failed'), \
             (3, 42, 'queued'), (4, 42, 'failed'), (5, 42, 'delivered'), (6, 7, 'failed')",
            &[],
            None,
        )
        .unwrap();
    server
        .admin
        .roles()
        .grant_table(
            "shipments",
            "oncall_lead",
            &[TablePrivilege::Select, TablePrivilege::Update],
        )
        .unwrap();
    server
        .admin
        .roles()
        .grant_table("shipments", "agent_exec", &[TablePrivilege::Select])
        .unwrap();
}

fn query(sql: &str) -> Vec<u8> {
    let mut out = vec![b'Q'];
    out.extend(((sql.len() + 5) as u32).to_be_bytes());
    out.extend_from_slice(sql.as_bytes());
    out.push(0);
    out
}

/// The tags of the messages in a raw answer.
fn tags(raw: &[u8]) -> Vec<u8> {
    let mut tags = Vec::new();
    let mut at = 0;
    while at < raw.len() {
        tags.push(raw[at]);
        at += 1 + u32::from_be_bytes(raw[at + 1..at + 5].try_into().unwrap()) as usize;
    }
    tags
}

/// `sql` on the agent listener is refused as `class` before any table read:
/// no row and no CopyOut header reaches the wire, nothing is charged, and
/// the refusal is logged once as the simple-query statement it was.
async fn refused_once(server: &AgentServer, wire: &mut Wire, sql: &str, class: StatementClass) {
    let raw = wire.exchange_raw(&query(sql)).await;
    assert_eq!(tags(&raw), [b'E', b'Z'], "{sql}: {raw:?}");
    let message = String::from_utf8_lossy(&raw);
    assert!(message.contains("0A000"), "{sql}: {message}");
    assert!(
        message.contains(TaskError::NotAllowedInTaskSession(class).message()),
        "{sql}: {message}"
    );
    let task = server.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let disclosure = server.admin.task_disclosure(task).unwrap();
    assert_eq!(disclosure.rows_returned, 0, "{sql}");
    assert!(disclosure.debits.is_empty(), "{sql}: {disclosure:?}");
    let records = server.admin.task_read_log(task).unwrap();
    let record = records.last().expect("a read-log record");
    assert_eq!(record.text, sql);
    assert_eq!(record.class, class_code(class), "{sql}");
    assert_eq!(record.entry, ENTRY_PGWIRE_SIMPLE, "{sql}");
    assert_eq!(record.outcome, outcome_of("0A000"), "{sql}");
    assert_eq!(record.released_rows, 0, "{sql}");
}

/// A task SELECT on the simple path: only the task's rows reach the wire,
/// released, charged and logged once as a simple-query statement. A task
/// SELECT naming the hidden `merchant_id` is refused before it runs. (Per-
/// chunk release of a long result is proven by the lib test
/// `pgwire::task_release_tests::simple_select_released_per_chunk`.) The same
/// SELECT from an ordinary session streams all its rows.
/// What a task was charged: the head's counters and the debit records.
fn charge(
    d: &giggles_core::task::release::Disclosure,
) -> (u64, u64, u64, Vec<giggles_core::task::release::DebitView>) {
    (d.rows_read, d.rows_returned, d.bytes_returned, d.debits.clone())
}

#[tokio::test]
async fn simple_select_released_per_chunk() {
    let (server, _agent, ordinary, mut wire) = agent_wire().await;
    seed(&server);
    let task_sql = "SELECT id, status FROM shipments ORDER BY id";
    let messages = wire.simple(task_sql).await;
    assert_eq!(error_of(&messages), None, "{messages:?}");
    let ids: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            Msg::Row(values) => values[0].clone(),
            _ => None,
        })
        .collect();
    assert_eq!(ids, ["1", "2", "3", "4", "5"], "{messages:?}");
    let task = server.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let records = server.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(
        (records[0].text.as_str(), records[0].entry, records[0].class),
        (task_sql, ENTRY_PGWIRE_SIMPLE, class_code(StatementClass::Select))
    );
    assert_eq!((records[0].pages, records[0].released_rows), (1, 5));
    let charged = server.admin.task_disclosure(task).unwrap();
    assert!(charged.rows_returned >= 5, "{charged:?}");
    let sql = "SELECT id, status FROM shipments WHERE merchant_id = 42 ORDER BY id";
    let messages = wire.simple(sql).await;
    assert_eq!(error_of(&messages).map(|e| e.0), Some("42501".to_owned()), "{messages:?}");
    assert!(!messages.iter().any(|m| matches!(m, Msg::Row(_))), "{messages:?}");
    assert_eq!(charge(&server.admin.task_disclosure(task).unwrap()), charge(&charged));

    let (_listener, mut ordinary) = ordinary_wire(&server, &ordinary).await;
    let messages = ordinary.simple(sql).await;
    assert_eq!(error_of(&messages), None, "{messages:?}");
    let rows = messages.iter().filter(|m| matches!(m, Msg::Row(_))).count();
    assert_eq!(rows, 5, "{messages:?}");
    assert!(messages.contains(&Msg::Complete("SELECT 5".into())), "{messages:?}");
}

/// DECLARE, FETCH and MOVE on the agent listener are refused as cursors and
/// logged, each once; nothing is charged. (Release per FETCH, and per MOVE,
/// is proven by the lib test
/// `pgwire::task_release_tests::declare_fetch_released_per_fetch`.) An
/// ordinary session's cursor still fetches and moves.
#[tokio::test]
async fn declare_fetch_released_per_fetch() {
    let (server, _agent, ordinary, mut wire) = agent_wire().await;
    seed(&server);
    for sql in [
        "DECLARE c CURSOR WITH HOLD FOR SELECT id FROM shipments",
        "FETCH 2 c",
        "MOVE 1 c",
    ] {
        refused_once(&server, &mut wire, sql, StatementClass::Cursor).await;
    }
    let task = server.admin.tasks().get("inc-436").unwrap().unwrap().id;
    assert_eq!(server.admin.task_read_log(task).unwrap().len(), 3);

    let (_listener, mut ordinary) = ordinary_wire(&server, &ordinary).await;
    for sql in [
        "BEGIN",
        "DECLARE c CURSOR FOR SELECT id FROM shipments WHERE merchant_id = 42 ORDER BY id",
    ] {
        assert_eq!(error_of(&ordinary.simple(sql).await), None, "{sql}");
    }
    let rows = |messages: &[Msg]| -> Vec<String> {
        messages
            .iter()
            .filter_map(|m| match m {
                Msg::Row(values) => values[0].clone(),
                _ => None,
            })
            .collect()
    };
    let messages = ordinary.simple("FETCH 2 c").await;
    assert_eq!(rows(&messages), ["1", "2"], "{messages:?}");
    let messages = ordinary.simple("MOVE 1 c").await;
    assert!(rows(&messages).is_empty(), "{messages:?}");
    assert!(messages.contains(&Msg::Complete("MOVE 1".into())), "{messages:?}");
    let messages = ordinary.simple("FETCH ALL c").await;
    assert_eq!(rows(&messages), ["4", "5"], "{messages:?}");
    assert_eq!(error_of(&ordinary.simple("COMMIT").await), None);
}

/// COPY TO STDOUT on the agent listener is refused as COPY before the
/// statement runs: the answer is an error, never a CopyOutResponse. (The
/// release of the whole result before the header is proven by the lib test
/// `pgwire::task_release_tests::copy_out_released_before_first_byte`.) An
/// ordinary session's COPY still streams.
#[tokio::test]
async fn copy_out_released_before_first_byte() {
    let (server, _agent, ordinary, mut wire) = agent_wire().await;
    seed(&server);
    let sql = "COPY (SELECT id, status FROM shipments WHERE merchant_id = 42) TO STDOUT";
    refused_once(&server, &mut wire, sql, StatementClass::Copy).await;

    let (_listener, mut ordinary) = ordinary_wire(&server, &ordinary).await;
    let raw = ordinary.exchange_raw(&query(sql)).await;
    let tags = tags(&raw);
    assert_eq!(tags.first(), Some(&b'H'), "{raw:?}");
    assert_eq!(tags.iter().filter(|t| **t == b'd').count(), 5, "{raw:?}");
}

#[tokio::test]
async fn task_set_constraints_requires_sql_authority() {
    let (server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in [
        "SET CONSTRAINTS ALL DEFERRED",
        "SET CONSTRAINTS nosuch IMMEDIATE",
        "SET /* comment */ CONSTRAINTS \"unterminated",
    ] {
        for frames in [
            wire.simple(sql).await,
            wire.parse_describe(sql, &[]).await,
            wire.extended(sql, &[], 0).await,
        ] {
            assert_eq!(error_of(&frames).as_ref().map(|(code, _)| code.as_str()), Some("42501"), "{sql}: {frames:?}");
            assert!(matches!(frames.as_slice(), [Msg::Error { .. }, Msg::Ready(b'I')]), "{sql}: {frames:?}");
        }
    }
    drop(wire);
    server.agent.shutdown().await.unwrap();
}

/// Simple-query, cursor, COPY and transaction-helper exchanges of an ordinary
/// session: a streamed SELECT, an UPDATE ... RETURNING, a cursor fetched and
/// moved inside a transaction with a savepoint released and rolled back to,
/// COPY TO STDOUT in text and CSV, and the aborted-transaction ROLLBACK
/// escape.
fn non_task_exchange() -> Vec<Vec<u8>> {
    [
        "SELECT id, status FROM shipments WHERE merchant_id = 42 ORDER BY id",
        "UPDATE shipments SET status = 'seen' WHERE id < 3 RETURNING id, status",
        "BEGIN",
        "SAVEPOINT a",
        "DECLARE c CURSOR FOR SELECT id, status FROM shipments ORDER BY id",
        "FETCH 2 c",
        "MOVE 2 c",
        "FETCH ALL c",
        "MOVE 0 c",
        "CLOSE c",
        "RELEASE a",
        "SAVEPOINT b",
        "ROLLBACK TO b",
        "ROLLBACK TO nope",
        "ROLLBACK",
        "COPY (SELECT id, status FROM shipments ORDER BY id) TO STDOUT",
        "COPY (SELECT id, status FROM shipments ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER)",
        "BEGIN",
        "SELECT 1/0",
        "SELECT 1",
        "ROLLBACK",
        "SELECT id FROM shipments WHERE id = 6; SELECT 2",
    ]
    .into_iter()
    .map(query)
    .collect()
}

/// The answers to `non_task_exchange`, recorded from the server before
/// R3-311 (BASE 2e4fd10e). Exchanges 19 and 20 (array indices 18 and 19) were re-recorded for
/// PostgreSQL 18.6's division-by-zero and aborted-transaction messages.
const RECORDED: [&str; 22] = [
    "54000000340002696400000000000000000000140008ffffffff00007374617475730000000000000000000019ffffffffffff0000440000001500020000000131000000066661696c6564440000001500020000000132000000066661696c656444000000150002000000013300000006717565756564440000001500020000000134000000066661696c65644400000018000200000001350000000964656c697665726564430000000d53454c4543542035005a0000000549",
    "54000000340002696400000000000000000000140008ffffffff00007374617475730000000000000000000019ffffffffffff0000440000001300020000000131000000047365656e440000001300020000000132000000047365656e430000000d5550444154452032005a0000000549",
    "430000000a424547494e005a0000000554",
    "430000000e53415645504f494e54005a0000000554",
    "43000000134445434c41524520435552534f52005a0000000554",
    "54000000340002696400000000000000000000140008ffffffff00007374617475730000000000000000000019ffffffffffff0000440000001300020000000131000000047365656e440000001300020000000132000000047365656e430000000c46455443482032005a0000000554",
    "430000000b4d4f56452032005a0000000554",
    "54000000340002696400000000000000000000140008ffffffff00007374617475730000000000000000000019ffffffffffff00004400000018000200000001350000000964656c697665726564440000001500020000000136000000066661696c6564430000000c46455443482032005a0000000554",
    "430000000b4d4f56452030005a0000000554",
    "4300000011434c4f534520435552534f52005a0000000554",
    "430000000c52454c45415345005a0000000554",
    "430000000e53415645504f494e54005a0000000554",
    "430000000d524f4c4c4241434b005a0000000554",
    "4500000093534552524f5200433342303031004d73617665706f696e7420226e6f70652220646f6573206e6f74206578697374004470686173653d73696d706c652071756572793b2071756572795f69643d3737333430313932396364356638633964663130653166376463666335346561303735636634306664313735336433393166363936323465326632303034333900005a0000000545",
    "430000000d524f4c4c4241434b005a0000000549",
    "480000000b00000200000000640000000b31097365656e0a640000000b32097365656e0a640000000d33097175657565640a640000000d34096661696c65640a6400000010350964656c6976657265640a640000000d36096661696c65640a6300000004430000000b434f50592036005a0000000549",
    "480000000b00000200000000640000000e69642c7374617475730a640000000b312c7365656e0a640000000b322c7365656e0a640000000d332c7175657565640a640000000d342c6661696c65640a6400000010352c64656c6976657265640a640000000d362c6661696c65640a6300000004430000000b434f50592036005a0000000549",
    "430000000a424547494e005a0000000554",
    "4500000087534552524f5200433232303132004d6469766973696f6e206279207a65726f004470686173653d6170706c69636174696f6e2053514c3b2071756572795f69643d6234353464323739333236353639663839393030323761633261626165373437613862386130366136343037346462656662353939363861666336386462363100005a0000000545",
    "45000000c8534552524f5200433235503032004d63757272656e74207472616e73616374696f6e2069732061626f727465642c20636f6d6d616e64732069676e6f72656420756e74696c20656e64206f66207472616e73616374696f6e20626c6f636b004470686173653d636174616c6f6720657865637574696f6e3b2071756572795f69643d6530303465626435623535333261346238353938346136326638616434386138316161333436306331636130373730316633383631333564373263646563663500005a0000000545",
    "430000000d524f4c4c4241434b005a0000000549",
    "540000001b0001696400000000000000000000140008ffffffff0000440000000b00010000000136430000000d53454c454354203100540000002100013f636f6c756d6e3f00000000000000000000170004ffffffff0000440000000b00010000000132430000000d53454c4543542031005a0000000549",
];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Compare a non-task session's simple-query, cursor, COPY and savepoint
/// replies byte for byte with the recorded exchange.
#[tokio::test]
async fn non_task_simple_and_copy_unchanged() {
    let (server, _agent, ordinary, _wire) = agent_wire().await;
    seed(&server);
    let (_listener, mut wire) = ordinary_wire(&server, &ordinary).await;
    let mut answers = Vec::new();
    for request in non_task_exchange() {
        answers.push(hex(&wire.exchange_raw(&request).await));
    }
    for (i, (answer, recorded)) in answers.iter().zip(RECORDED).enumerate() {
        assert_eq!(answer, recorded, "exchange {i}; all answers: {answers:#?}");
    }
}
