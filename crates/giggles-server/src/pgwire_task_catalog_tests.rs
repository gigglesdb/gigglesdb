//! GD-0061 R3-312: catalog queries in a task session, driven through
//! `State::handle` with real Query, Parse, Bind and Execute bodies. `Catalog`
//! and `Select` are in `task::admit::ENABLED` since R3-315.
use super::*;
use crate::{AgentBinding, Binding, Role, Service};
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, TablePrivilege},
    task::definition::{DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText},
};
use std::sync::Arc;

static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
const AGENT: &str = "ab";
const ORDINARY: &str = "cd";

/// Text a task session must never receive: its hidden columns, another table
/// and its column, and the index, comments and default that name them.
const HIDDEN: &[&str] = &[
    "secret_note",
    "customer_id",
    "customers",
    "ssn",
    "shipments_secret_idx",
    "zebracomment",
    "quokka",
    "wombat",
];

/// `shipments` (task `inc-436`: merchant 42; `id`, `merchant_id` and `label`
/// readable, `status` masked as `***`, `secret_note` and `customer_id`
/// hidden) next to `customers`, with an index, comments, a default and a
/// foreign key that name hidden things. Returns the service, the agent
/// session, and an ordinary session of the issuer.
fn fixture() -> (Arc<Service>, Session, Session) {
    let path = crate::test_support::fixture_root().join(format!(
        "task-catalog-{}-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    for statement in [
        "CREATE TABLE customers (id bigint primary key, ssn text)",
        "INSERT INTO customers VALUES (1, '123-45-6789'), (2, '987-65-4321')",
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         label text DEFAULT 'wombat secret_note', secret_note text, \
         customer_id bigint REFERENCES customers (id))",
        "INSERT INTO shipments VALUES (1, 42, 'failed', 'a', 'n1', 1), (2, 42, 'queued', 'b', 'n2', 2), \
         (3, 42, 'failed', 'c', 'n3', 1), (4, 7, 'failed', 'd', 'n4', 2)",
        "CREATE INDEX shipments_secret_idx ON shipments (secret_note)",
        "CREATE INDEX shipments_status_idx ON shipments (status)",
        "COMMENT ON TABLE shipments IS 'quokka customers'",
        "COMMENT ON COLUMN shipments.status IS 'current status'",
        "COMMENT ON COLUMN shipments.secret_note IS 'zebracomment'",
    ] {
        admin.sql_session().execute(statement, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("agent_exec", true)] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
        for table in ["shipments", "customers"] {
            roles.grant_table(table, name, &[TablePrivilege::Select]).unwrap();
        }
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let clock = crate::clock::Clock::new(0, 3600).unwrap();
    let now = crate::agent::wall_now(&clock).unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into(), "merchant_id".into(), "label".into()],
                masked: vec![("status".into(), Some(Value::Text("***".into())))],
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 10_000,
                    rows_returned: 10_000,
                    bytes_returned: 10_000_000,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: "2999-01-01 00:00:00+00".into(),
            },
            now,
        )
        .unwrap();
    let service = Service::with_agent(
        admin,
        vec![Binding {
            fingerprint: ORDINARY.repeat(32),
            role: Role::Database {
                name: "incident_controller".into(),
            },
        }],
        vec![AgentBinding {
            fingerprint: AGENT.repeat(32),
            executor: "agent_exec".into(),
            tasks: vec!["inc-436".into()],
        }],
        clock,
        crate::admission::ServiceLimits::default(),
    )
    .unwrap();
    let agent = service.agent_session(&AGENT.repeat(32), None).unwrap();
    agent.sql.set_disclosure_chunk(0, 0);
    let ordinary = service.session(&ORDINARY.repeat(32)).unwrap();
    (service, agent, ordinary)
}

fn state() -> State {
    State {
        output: None,
        diagnostic: Diagnostic::default(),
        admin_completed: false,
        implicit: false,
        retain_durability_permits: false,
        settings: crate::pg_settings::SessionSettings::default(),
        statements: BTreeMap::new(),
        unnamed_cache: Vec::new(),
        named_statement_bytes: 0,
        portals: BTreeMap::new(),
        cursors: cursor::Cursors::default(),
        drain: false,
        copy: None,
        copy_completion: vec![],
        copy_active: None,
        copy_permit: None,
        execution_admission: None,
    }
}

fn cstr(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(text.as_bytes());
    out.push(0);
}

/// A simple Query; the bytes answered, or the error.
fn simple(state: &mut State, session: &mut Session, sql: &str) -> R<Vec<u8>> {
    let mut out = Vec::new();
    let mut body = Vec::new();
    cstr(sql, &mut body);
    state.handle(b'Q', &body, session, &mut out, std::time::Instant::now())?;
    Ok(out)
}

/// Parse, Bind, Describe portal, Execute (no row limit) and Sync on the
/// unnamed statement and portal; the bytes answered, or the first error.
fn extended(state: &mut State, session: &mut Session, sql: &str) -> R<Vec<u8>> {
    let mut out = Vec::new();
    let now = std::time::Instant::now();
    let mut parse = Vec::new();
    cstr("", &mut parse);
    cstr(sql, &mut parse);
    parse.extend(0u16.to_be_bytes());
    state.handle(b'P', &parse, session, &mut out, now)?;
    let mut bind = Vec::new();
    cstr("", &mut bind);
    cstr("", &mut bind);
    bind.extend([0, 0, 0, 0, 0, 0]);
    state.handle(b'B', &bind, session, &mut out, now)?;
    let mut describe = vec![b'P'];
    cstr("", &mut describe);
    state.handle(b'D', &describe, session, &mut out, now)?;
    let mut execute = Vec::new();
    cstr("", &mut execute);
    execute.extend(0u32.to_be_bytes());
    state.handle(b'E', &execute, session, &mut out, now)?;
    state.handle(b'S', &[], session, &mut out, now)?;
    Ok(out)
}

/// The DataRow messages in `out`, as their text values.
fn data_rows(out: &[u8]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    let mut at = 0;
    while at < out.len() {
        let tag = out[at];
        let length = u32::from_be_bytes(out[at + 1..at + 5].try_into().unwrap()) as usize;
        let body = &out[at + 5..at + 1 + length];
        at += 1 + length;
        if tag != b'D' {
            continue;
        }
        let count = u16::from_be_bytes([body[0], body[1]]) as usize;
        let mut i = 2;
        let mut values = Vec::new();
        for _ in 0..count {
            let n = i32::from_be_bytes(body[i..i + 4].try_into().unwrap());
            i += 4;
            if n < 0 {
                values.push(None);
            } else {
                values.push(Some(String::from_utf8_lossy(&body[i..i + n as usize]).into_owned()));
                i += n as usize;
            }
        }
        rows.push(values);
    }
    rows
}

fn text_rows(out: &[u8]) -> Vec<Vec<String>> {
    data_rows(out)
        .into_iter()
        .map(|row| row.into_iter().map(Option::unwrap_or_default).collect())
        .collect()
}

/// No hidden name anywhere in the answer (every message, every byte).
fn assert_no_hidden(sql: &str, out: &[u8]) {
    let text = String::from_utf8_lossy(out).to_lowercase();
    for name in HIDDEN {
        assert!(!text.contains(name), "{name} in the answer to {sql}: {text:?}");
    }
}

/// `sql` on both entry points of a task session; both answers, after checking
/// they carry the same rows and no hidden name.
fn task_query(state: &mut State, session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let simple_out = simple(state, session, sql).unwrap_or_else(|code| panic!("{sql}: {code}"));
    let extended_out =
        extended(state, session, sql).unwrap_or_else(|code| panic!("{sql}: {code}"));
    assert_no_hidden(sql, &simple_out);
    assert_no_hidden(sql, &extended_out);
    let rows = text_rows(&simple_out);
    assert_eq!(rows, text_rows(&extended_out), "{sql}");
    rows
}

const PG_ATTRIBUTE: &str = "SELECT a.attname FROM pg_catalog.pg_attribute a \
     JOIN pg_catalog.pg_class c ON a.attrelid = c.oid \
     WHERE c.relname = 'shipments' AND a.attnum > 0 ORDER BY a.attnum";
const PG_CLASS: &str =
    "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relnamespace = 2200 ORDER BY relname";
const COLUMNS: &str = "SELECT table_name, column_name, column_default FROM information_schema.columns \
     WHERE table_schema = 'public' ORDER BY table_name, ordinal_position";
const MIXED: &str = "SELECT c.relname, count(t.*) FROM pg_catalog.pg_class c LEFT JOIN shipments t \
     ON true WHERE c.relname = 'shipments' GROUP BY c.relname ORDER BY c.relname";
const MIXED_ROWS: &str = "SELECT t.id, t.status FROM shipments t \
     JOIN pg_catalog.pg_class c ON c.relname = 'shipments' ORDER BY t.id";
/// Catalog relations that could name a hidden thing.
const SWEEP: &[&str] = &[
    "SELECT * FROM pg_catalog.pg_class",
    "SELECT * FROM pg_catalog.pg_attribute",
    "SELECT * FROM pg_catalog.pg_index",
    "SELECT * FROM pg_catalog.pg_indexes",
    "SELECT * FROM pg_catalog.pg_constraint",
    "SELECT * FROM pg_catalog.pg_description",
    "SELECT * FROM pg_catalog.pg_attrdef",
    "SELECT * FROM pg_catalog.pg_namespace",
    "SELECT * FROM pg_catalog.pg_tables",
    "SELECT * FROM pg_catalog.pg_type",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM information_schema.columns",
    "SELECT * FROM information_schema.table_constraints",
    "SELECT * FROM information_schema.key_column_usage",
];

/// `pg_attribute` lists the task table's readable and masked columns, in
/// table order, and nothing hidden.
#[test]
fn pg_attribute_lists_only_readable_and_masked() {
    let (_service, mut session, _) = fixture();
    let mut state = state();
    let rows = task_query(&mut state, &mut session, PG_ATTRIBUTE);
    assert_eq!(
        rows,
        [["id"], ["merchant_id"], ["status"], ["label"]].map(|r| r.map(String::from).to_vec())
    );
    // Every catalog relation that could name a column, scanned whole on
    // both entry points: nothing hidden in any of them.
    for sql in SWEEP {
        task_query(&mut state, &mut session, sql);
    }
}

/// `pg_class` lists the task table and only the indexes on shown columns.
#[test]
fn pg_class_lists_only_task_table() {
    let (_service, mut session, _) = fixture();
    let mut state = state();
    let rows = task_query(&mut state, &mut session, PG_CLASS);
    assert_eq!(
        rows,
        [
            ["shipments", "r"],
            ["shipments_pkey", "i"],
            ["shipments_status_idx", "i"],
        ]
        .map(|r| r.map(String::from).to_vec())
    );
    // The comment on a shown column survives; the table comment that names
    // another table and the hidden column's comment are dropped.
    let rows = task_query(
        &mut state,
        &mut session,
        "SELECT description FROM pg_catalog.pg_description WHERE objoid <> 2200 ORDER BY objsubid",
    );
    assert_eq!(rows, [vec!["current status".to_owned()]]);
}

/// `information_schema.columns` lists the shown columns of the task table;
/// a default that names a hidden column is dropped.
#[test]
fn information_schema_columns_filtered() {
    let (_service, mut session, _) = fixture();
    let mut state = state();
    let rows = task_query(&mut state, &mut session, COLUMNS);
    assert_eq!(
        rows,
        [
            ["shipments", "id", ""],
            ["shipments", "merchant_id", ""],
            ["shipments", "status", ""],
            ["shipments", "label", ""],
        ]
        .map(|r| r.map(String::from).to_vec())
    );
}

/// Application rows a mixed catalog query reads go through the release
/// protocol: only the task's rows, masked, charged and logged with the
/// catalog query's text. A hidden column or another table is not there to
/// name.
#[test]
fn user_relation_rows_in_catalog_query_are_charged() {
    let (service, mut session, _) = fixture();
    let mut state = state();
    let task = service.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let out = simple(&mut state, &mut session, MIXED).unwrap();
    assert_no_hidden(MIXED, &out);
    assert_eq!(text_rows(&out), [["shipments".to_owned(), "3".to_owned()]]);
    let disclosure = service.admin.task_disclosure(task).unwrap();
    assert_eq!(disclosure.rows_returned, 3, "{disclosure:?}");
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].text, MIXED);
    assert_eq!(records[0].released_rows, 3);
    assert_eq!(records[0].masked_values, 3);

    let out = extended(&mut state, &mut session, MIXED_ROWS).unwrap();
    assert_no_hidden(MIXED_ROWS, &out);
    assert_eq!(
        text_rows(&out),
        (1..=3)
            .map(|id| vec![id.to_string(), "***".to_owned()])
            .collect::<Vec<_>>()
    );
    assert_eq!(service.admin.task_disclosure(task).unwrap().rows_returned, 6);
    let records = service.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[1].text, MIXED_ROWS);

    // A hidden column answers as a nonexistent one, and another table as a
    // nonexistent table: nothing tells them apart.
    for (sql, absent) in [
        (
            "SELECT t.secret_note FROM shipments t JOIN pg_catalog.pg_class c ON c.relname = 'shipments'",
            "SELECT t.no_such FROM shipments t JOIN pg_catalog.pg_class c ON c.relname = 'shipments'",
        ),
        (
            "SELECT t.ssn FROM customers t JOIN pg_catalog.pg_class c ON c.relname = 'customers'",
            "SELECT t.ssn FROM no_such t JOIN pg_catalog.pg_class c ON c.relname = 'no_such'",
        ),
    ] {
        let simple_error = simple(&mut state, &mut session, sql).unwrap_err();
        let extended_error = extended(&mut state, &mut session, sql).unwrap_err();
        assert_eq!(simple_error, extended_error, "{sql}");
        assert_eq!(
            simple_error,
            simple(&mut state, &mut session, absent).unwrap_err(),
            "{sql}"
        );
    }
    assert_eq!(service.admin.task_disclosure(task).unwrap().rows_returned, 6);
}

/// A non-task session's catalog is not filtered: the same queries show the
/// other table, the hidden columns and their comments, index and default.
#[test]
fn non_task_catalog_unchanged() {
    let (_service, _agent, mut session) = fixture();
    let mut state = state();
    let out = simple(&mut state, &mut session, PG_CLASS).unwrap();
    assert_eq!(
        text_rows(&out),
        [
            ["customers", "r"],
            ["customers_pkey", "i"],
            ["shipments", "r"],
            ["shipments_pkey", "i"],
            ["shipments_secret_idx", "i"],
            ["shipments_status_idx", "i"],
        ]
        .map(|r| r.map(String::from).to_vec())
    );
    let out = extended(&mut state, &mut session, PG_ATTRIBUTE).unwrap();
    assert_eq!(
        text_rows(&out),
        [
            ["id"],
            ["merchant_id"],
            ["status"],
            ["label"],
            ["secret_note"],
            ["customer_id"],
        ]
        .map(|r| r.map(String::from).to_vec())
    );
    let out = simple(&mut state, &mut session, MIXED).unwrap();
    assert_eq!(text_rows(&out), [["shipments".to_owned(), "4".to_owned()]]);
    let out = simple(
        &mut state,
        &mut session,
        "SELECT description FROM pg_catalog.pg_description WHERE objoid <> 2200 ORDER BY objsubid",
    )
    .unwrap();
    assert_eq!(
        text_rows(&out),
        [["quokka customers"], ["current status"], ["zebracomment"]]
            .map(|r| r.map(String::from).to_vec())
    );
}

#[test]
fn task_sessions_exclude_unnamed_plan_cache() {
    let (_, agent, _) = fixture();
    assert!(agent.task().is_some());
    assert_eq!(agent.sql.committed_catalog_version(), None);
}
