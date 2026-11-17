//! GD-0061 R3-310: the extended-protocol Execute path goes through the release
//! protocol. Every page of rows a task session sends over Execute is released
//! (masked, checked and charged) first, and a non-task session's exchange is
//! unchanged byte for byte.
//!
//! `Select` is not yet in `task::admit::ENABLED`, so a task SELECT cannot run
//! over the agent listener. The task-session paging and revalidation proofs
//! that need it run as server-lib tests with the cfg(test) admission route
//! (`pgwire::task_release_tests`); these wire tests cover what the listener
//! reaches today: the refusal is logged once and charges nothing, and the
//! non-task path is identical to a recorded exchange.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{AgentServer, Msg, Wire, agent_wire, error_of, ordinary_wire};
use giggles_core::{
    roles::TablePrivilege,
    task::{
        StatementClass, TaskError,
        readlog::{ENTRY_PGWIRE_EXTENDED, class_code, outcome_of},
    },
};

fn frame(tag: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend(((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
}

fn cstr(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(text.as_bytes());
    out.push(0);
}

fn parse(statement: &str, sql: &str, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    cstr(statement, &mut body);
    cstr(sql, &mut body);
    body.extend(0u16.to_be_bytes());
    frame(b'P', &body, out);
}

fn bind(portal: &str, statement: &str, params: &[&str], out: &mut Vec<u8>) {
    let mut body = Vec::new();
    cstr(portal, &mut body);
    cstr(statement, &mut body);
    body.extend(0u16.to_be_bytes());
    body.extend((params.len() as u16).to_be_bytes());
    for param in params {
        body.extend((param.len() as i32).to_be_bytes());
        body.extend_from_slice(param.as_bytes());
    }
    body.extend(0u16.to_be_bytes());
    frame(b'B', &body, out);
}

fn describe_portal(portal: &str, out: &mut Vec<u8>) {
    let mut body = vec![b'P'];
    cstr(portal, &mut body);
    frame(b'D', &body, out);
}

fn execute(portal: &str, max_rows: u32, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    cstr(portal, &mut body);
    body.extend(max_rows.to_be_bytes());
    frame(b'E', &body, out);
}

fn sync(out: &mut Vec<u8>) {
    frame(b'S', &[], out);
}

/// Five task-visible rows and one other, readable and updatable by the
/// ordinary login.
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
}

/// A SELECT whose portal is read in three Executes, an UPDATE ... RETURNING
/// whose retained result is served in slices and once more after completion,
/// a SHOW (a server-built result) and an Execute of an unknown portal.
fn non_task_exchange() -> Vec<Vec<u8>> {
    let mut select = Vec::new();
    parse(
        "",
        "SELECT id, status FROM shipments WHERE merchant_id = $1 ORDER BY id",
        &mut select,
    );
    bind("", "", &["42"], &mut select);
    describe_portal("", &mut select);
    execute("", 2, &mut select);
    execute("", 2, &mut select);
    execute("", 0, &mut select);
    sync(&mut select);
    let mut returning = Vec::new();
    parse(
        "s1",
        "UPDATE shipments SET status = 'seen' WHERE merchant_id = $1 RETURNING id, status",
        &mut returning,
    );
    bind("p1", "s1", &["42"], &mut returning);
    describe_portal("p1", &mut returning);
    execute("p1", 2, &mut returning);
    execute("p1", 2, &mut returning);
    execute("p1", 0, &mut returning);
    execute("p1", 0, &mut returning);
    sync(&mut returning);
    let mut show = Vec::new();
    parse("", "SHOW extra_float_digits", &mut show);
    bind("", "", &[], &mut show);
    describe_portal("", &mut show);
    execute("", 0, &mut show);
    sync(&mut show);
    let mut missing = Vec::new();
    execute("nope", 0, &mut missing);
    sync(&mut missing);
    vec![select, returning, show, missing]
}

/// The answers to `non_task_exchange`, recorded from the server before
/// R3-310 (BASE 12ebf438).
const RECORDED: [&str; 4] = [
    "3100000004320000000454000000340002696400000000000000000000140008ffffffff00007374617475730000000000000000000019ffffffffffff0000440000001500020000000131000000066661696c6564440000001500020000000132000000066661696c6564730000000444000000150002000000013300000006717565756564440000001500020000000134000000066661696c656473000000044400000018000200000001350000000964656c697665726564430000000d53454c4543542035005a0000000549",
    "3100000004320000000454000000340002696400000000000000000000140008ffffffff00007374617475730000000000000000000019ffffffffffff0000440000001300020000000131000000047365656e440000001300020000000132000000047365656e7300000004440000001300020000000133000000047365656e440000001300020000000134000000047365656e7300000004440000001300020000000135000000047365656e430000000d555044415445203500430000000d5550444154452035005a0000000549",
    "31000000043200000004540000002b000165787472615f666c6f61745f6469676974730000000000000000000019ffffffffffff0000440000000b00010000000131430000000953484f57005a0000000549",
    "450000005c534552524f5200433334303030004d726571756573742072656a65637465643b20636f6e73756c7420646f63756d656e74656420636f6465004470686173653d657865637574653b2071756572795f69643d6e6f6e6500005a0000000549",
];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A task SELECT on the extended path. `Select` is not enabled, so admission
/// refuses it at Parse before any catalog or table read: no page is produced,
/// nothing is charged, and the refusal is logged once with its text, class and
/// entry point. (The page-per-fetch release is proven by the lib test
/// `pgwire::task_release_tests::portal_paging_releases_each_fetch`.)
#[tokio::test]
async fn portal_paging_releases_each_fetch() {
    let (server, _agent, _ordinary, mut wire) = agent_wire().await;
    seed(&server);
    let sql = "SELECT id, status FROM shipments WHERE merchant_id = 42";
    let messages = wire.extended(sql, &[], 0).await;
    assert_eq!(
        error_of(&messages),
        Some((
            "0A000".to_owned(),
            TaskError::NotAllowedInTaskSession(StatementClass::Select)
                .message()
                .to_owned()
        )),
        "{messages:?}"
    );
    assert!(!messages.iter().any(|m| matches!(m, Msg::Row(_))), "{messages:?}");
    let task = server.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let disclosure = server.admin.task_disclosure(task).unwrap();
    assert_eq!(disclosure.rows_returned, 0);
    assert!(disclosure.debits.is_empty(), "{disclosure:?}");
    let records = server.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.text, sql);
    assert_eq!(record.class, class_code(StatementClass::Select));
    assert_eq!(record.entry, ENTRY_PGWIRE_EXTENDED);
    assert_eq!(record.outcome, outcome_of("0A000"));
    assert_eq!(record.released_rows, 0);
}

/// A task portal never retains a result to serve in slices: Execute with a
/// row limit is refused as a cursor before the statement runs, and logged.
/// (Revalidation of a retained released result before each later slice is
/// proven by the lib test
/// `pgwire::task_release_tests::cached_returning_result_revalidated_per_slice`.)
/// The non-task RETURNING slices are unchanged.
#[tokio::test]
async fn cached_returning_result_revalidated_per_slice() {
    let (server, _agent, ordinary, mut wire) = agent_wire().await;
    seed(&server);
    let sql = "SHOW extra_float_digits";
    let messages = wire.extended(sql, &[], 1).await;
    assert_eq!(
        error_of(&messages),
        Some((
            "0A000".to_owned(),
            TaskError::NotAllowedInTaskSession(StatementClass::Cursor)
                .message()
                .to_owned()
        )),
        "{messages:?}"
    );
    let records = server.admin.task_read_log(server.admin.tasks().get("inc-436").unwrap().unwrap().id).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].text, sql);
    assert_eq!(records[0].class, class_code(StatementClass::Cursor));
    // The same statement without a row limit runs (Show is admitted).
    let messages = wire.extended(sql, &[], 0).await;
    assert_eq!(error_of(&messages), None, "{messages:?}");

    let (_listener, mut ordinary) = ordinary_wire(&server, &ordinary).await;
    let mut out = Vec::new();
    parse(
        "",
        "UPDATE shipments SET status = 'seen' WHERE merchant_id = 42 RETURNING id",
        &mut out,
    );
    bind("", "", &[], &mut out);
    for _ in 0..3 {
        execute("", 2, &mut out);
    }
    sync(&mut out);
    let messages = parsed(&mut ordinary, &out).await;
    let rows: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            Msg::Row(values) => values[0].clone(),
            _ => None,
        })
        .collect();
    assert_eq!(rows, ["1", "2", "3", "4", "5"], "{messages:?}");
    assert_eq!(
        messages
            .iter()
            .filter(|m| matches!(m, Msg::Suspended))
            .count(),
        2,
        "{messages:?}"
    );
    assert!(messages.contains(&Msg::Complete("UPDATE 5".into())), "{messages:?}");
}

async fn parsed(wire: &mut Wire, bytes: &[u8]) -> Vec<Msg> {
    let raw = wire.exchange_raw(bytes).await;
    let mut messages = Vec::new();
    let mut at = 0;
    while at < raw.len() {
        let tag = raw[at];
        let length = u32::from_be_bytes(raw[at + 1..at + 5].try_into().unwrap()) as usize;
        let body = &raw[at + 5..at + 1 + length];
        at += 1 + length;
        messages.push(match tag {
            b'D' => {
                let count = u16::from_be_bytes([body[0], body[1]]) as usize;
                let mut i = 2;
                let mut values = Vec::new();
                for _ in 0..count {
                    let n = i32::from_be_bytes(body[i..i + 4].try_into().unwrap());
                    i += 4;
                    if n < 0 {
                        values.push(None);
                    } else {
                        values.push(Some(
                            String::from_utf8_lossy(&body[i..i + n as usize]).into_owned(),
                        ));
                        i += n as usize;
                    }
                }
                Msg::Row(values)
            }
            b's' => Msg::Suspended,
            b'C' => Msg::Complete(String::from_utf8_lossy(&body[..body.len() - 1]).into_owned()),
            b'Z' => Msg::Ready(body[0]),
            b'E' => Msg::Error {
                code: String::new(),
                message: String::from_utf8_lossy(body).into_owned(),
            },
            other => Msg::Other(other),
        });
    }
    messages
}

/// A non-task session's extended exchange is byte-for-byte what the server
/// answered before R3-310: paging, a retained RETURNING result in slices,
/// a server-built SHOW and an unknown portal.
#[tokio::test]
async fn non_task_extended_query_unchanged() {
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
