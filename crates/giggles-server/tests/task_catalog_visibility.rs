//! GD-0061 R3-312: catalog queries in a task session. A task session's catalog
//! shows only the task's table with its readable and masked columns; indexes,
//! constraints, comments and defaults that mention another column or table are
//! dropped, and the application rows a mixed catalog query reads go through
//! the release protocol. A non-task session's catalog answers are unchanged
//! byte for byte.
//!
//! `Catalog` and `Select` are in `task::admit::ENABLED` since R3-315 (the
//! Phase 3 close), so these wire tests run catalog queries over the agent
//! listener, in the simple and the extended protocol, and check the filtered
//! answers: the task (readable `id` and `status`, hidden `merchant_id`) sees
//! only its table and shown columns, and no hidden name reaches the wire.
//! The server-lib tests of the same names (`pgwire::task_catalog_tests`)
//! cover a richer fixture (masked and labelled columns, comments, defaults).
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{AgentServer, Msg, agent_wire, error_of, ordinary_wire};
use giggles_core::{
    roles::TablePrivilege,
    task::readlog::{ENTRY_PGWIRE_EXTENDED, ENTRY_PGWIRE_SIMPLE},
};

/// Names a task session must never see: the hidden column of `shipments`,
/// another table and its column, and objects named after them.
const HIDDEN: &[&str] = &[
    "merchant_id",
    "customers",
    "ssn",
    "shipments_merchant_idx",
];

const PG_ATTRIBUTE: &str = "SELECT a.attname, a.attnum, a.atttypid FROM pg_catalog.pg_attribute a \
     JOIN pg_catalog.pg_class c ON a.attrelid = c.oid WHERE c.relnamespace = 2200 AND a.attnum > 0 \
     ORDER BY c.relname, a.attnum";
const PG_CLASS: &str =
    "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relnamespace = 2200 ORDER BY relname";
const COLUMNS: &str = "SELECT table_name, column_name, data_type FROM information_schema.columns \
     WHERE table_schema = 'public' ORDER BY table_name, ordinal_position";
const MIXED: &str = "SELECT c.relname, count(t.*) FROM pg_catalog.pg_class c LEFT JOIN shipments t \
     ON true WHERE c.relname = 'shipments' GROUP BY c.relname ORDER BY c.relname";
const DESCRIPTIONS: &str = "SELECT objoid, objsubid, description FROM pg_catalog.pg_description \
     ORDER BY objoid, objsubid";
const INDEXES: &str =
    "SELECT indexname, indexdef FROM pg_catalog.pg_indexes WHERE schemaname = 'public' ORDER BY indexname";

/// Task rows, another table, an index and comments that name hidden things,
/// SELECT on both tables for the ordinary login and on `shipments` for the
/// executor.
fn seed(server: &AgentServer) {
    let mut sql = server.admin.sql_session();
    for statement in [
        "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'queued'), (3, 7, 'failed')",
        "CREATE TABLE customers (id bigint primary key, ssn text)",
        "INSERT INTO customers VALUES (1, '123-45-6789')",
        "CREATE INDEX shipments_merchant_idx ON shipments (merchant_id)",
        "COMMENT ON TABLE shipments IS 'joined to customers'",
        "COMMENT ON COLUMN shipments.status IS 'current status'",
        "COMMENT ON COLUMN shipments.merchant_id IS 'owner'",
    ] {
        sql.execute(statement, &[], None).unwrap();
    }
    for table in ["shipments", "customers"] {
        server
            .admin
            .roles()
            .grant_table(table, "oncall_lead", &[TablePrivilege::Select])
            .unwrap();
    }
    server
        .admin
        .roles()
        .grant_table("shipments", "agent_exec", &[TablePrivilege::Select])
        .unwrap();
}

/// Every text the answer carries, lower-cased: no hidden name may occur.
fn assert_no_hidden(messages: &[Msg]) {
    let text = format!("{messages:?}").to_lowercase();
    for name in HIDDEN {
        assert!(!text.contains(name), "{name} in {messages:?}");
    }
}

fn rows(messages: &[Msg]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|m| match m {
            Msg::Row(values) => Some(values.iter().map(|v| v.clone().unwrap_or_default()).collect()),
            _ => None,
        })
        .collect()
}

/// A catalog query on both entry points answers `expected` from the task's
/// filtered catalog, with no hidden name in the answer. Returns the server,
/// for checks of what was charged and logged.
async fn filtered_on_both_paths(sql: &str, expected: &[&[&str]]) -> AgentServer {
    let (server, _agent, _ordinary, mut wire) = agent_wire().await;
    seed(&server);
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|v| v.to_string()).collect())
        .collect();
    let simple = wire.simple(sql).await;
    let extended = wire.extended(sql, &[], 0).await;
    for messages in [&simple, &extended] {
        assert_eq!(error_of(messages), None, "{messages:?}");
        assert_no_hidden(messages);
        assert_eq!(rows(messages), expected, "{sql}: {messages:?}");
    }
    server
}

/// A catalog query that reads no application row charges nothing.
async fn filtered_uncharged(sql: &str, expected: &[&[&str]]) {
    let server = filtered_on_both_paths(sql, expected).await;
    let task = server.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let disclosure = server.admin.task_disclosure(task).unwrap();
    assert_eq!((disclosure.rows_returned, disclosure.debits.len()), (0, 0), "{disclosure:?}");
}

#[tokio::test]
async fn pg_attribute_lists_only_readable_and_masked() {
    filtered_uncharged(
        PG_ATTRIBUTE,
        &[&["id", "1", "20"], &["status", "2", "25"], &["id", "1", "20"]],
    )
    .await;
}

#[tokio::test]
async fn pg_class_lists_only_task_table() {
    filtered_uncharged(PG_CLASS, &[&["shipments", "r"], &["shipments_pkey", "i"]]).await;
}

#[tokio::test]
async fn information_schema_columns_filtered() {
    filtered_uncharged(
        COLUMNS,
        &[&["shipments", "id", "bigint"], &["shipments", "status", "text"]],
    )
    .await;
}

/// The application rows of a mixed catalog query are only the task's rows
/// (two of merchant 42), released, charged and logged with the catalog
/// query's text on each entry point.
#[tokio::test]
async fn user_relation_rows_in_catalog_query_are_charged() {
    let server = filtered_on_both_paths(MIXED, &[&["shipments", "2"]]).await;
    let task = server.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let records = server.admin.task_read_log(task).unwrap();
    assert_eq!(records.len(), 2, "{records:?}");
    for (record, entry) in records.iter().zip([ENTRY_PGWIRE_SIMPLE, ENTRY_PGWIRE_EXTENDED]) {
        assert_eq!((record.text.as_str(), record.entry), (MIXED, entry), "{record:?}");
        assert_eq!((record.pages, record.released_rows), (1, 2), "{record:?}");
        assert_eq!(record.sqlstate_text(), "00000", "{record:?}");
    }
    let disclosure = server.admin.task_disclosure(task).unwrap();
    assert!(disclosure.rows_returned >= 4, "{disclosure:?}");
    assert_eq!(disclosure.debits.len(), 2, "{disclosure:?}");
}

/// The catalog exchanges a non-task session sends, each in the simple and the
/// extended protocol.
fn non_task_exchange() -> Vec<Vec<u8>> {
    fn frame(tag: u8, body: &[u8], out: &mut Vec<u8>) {
        out.push(tag);
        out.extend(((body.len() + 4) as u32).to_be_bytes());
        out.extend_from_slice(body);
    }
    fn cstr(text: &str, out: &mut Vec<u8>) {
        out.extend_from_slice(text.as_bytes());
        out.push(0);
    }
    let mut exchanges = Vec::new();
    for sql in [PG_ATTRIBUTE, PG_CLASS, COLUMNS, MIXED, DESCRIPTIONS, INDEXES] {
        let mut simple = Vec::new();
        let mut body = Vec::new();
        cstr(sql, &mut body);
        frame(b'Q', &body, &mut simple);
        exchanges.push(simple);
        let mut extended = Vec::new();
        let mut body = Vec::new();
        cstr("", &mut body);
        cstr(sql, &mut body);
        body.extend(0u16.to_be_bytes());
        frame(b'P', &body, &mut extended);
        let mut body = Vec::new();
        cstr("", &mut body);
        cstr("", &mut body);
        body.extend([0, 0, 0, 0, 0, 0]);
        frame(b'B', &body, &mut extended);
        let mut body = vec![b'P'];
        cstr("", &mut body);
        frame(b'D', &body, &mut extended);
        let mut body = Vec::new();
        cstr("", &mut body);
        body.extend(2u32.to_be_bytes());
        frame(b'E', &body, &mut extended);
        let mut body = Vec::new();
        cstr("", &mut body);
        body.extend(0u32.to_be_bytes());
        frame(b'E', &body, &mut extended);
        frame(b'S', &[], &mut extended);
        exchanges.push(extended);
    }
    exchanges
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The answers to `non_task_exchange`, recorded from the server before
/// R3-312 (BASE 50067384).
const RECORDED: &[&str] = &[
    "540000005400036174746e616d6500000000000000000000130040ffffffff00006174746e756d00000000000000000000150002ffffffff00006174747479706964000000000000000000001a0004ffffffff0000440000001700030000000269640000000131000000023230440000001800030000000373736e0000000132000000023235440000001700030000000269640000000131000000023230440000001700030000000269640000000131000000023230440000002000030000000b6d65726368616e745f69640000000132000000023230440000001b0003000000067374617475730000000133000000023235440000002000030000000b6d65726368616e745f69640000000131000000023230440000001700030000000269640000000131000000023230430000000d53454c4543542038005a0000000549",
    "31000000043200000004540000005400036174746e616d6500000000000000000000130040ffffffff00006174746e756d00000000000000000000150002ffffffff00006174747479706964000000000000000000001a0004ffffffff0000440000001700030000000269640000000131000000023230440000001800030000000373736e00000001320000000232357300000004440000001700030000000269640000000131000000023230440000001700030000000269640000000131000000023230440000002000030000000b6d65726368616e745f69640000000132000000023230440000001b0003000000067374617475730000000133000000023235440000002000030000000b6d65726368616e745f69640000000131000000023230440000001700030000000269640000000131000000023230430000000d53454c4543542038005a0000000549",
    "540000003a000272656c6e616d6500000000000000000000130040ffffffff000072656c6b696e6400000000000000000000120001ffffffff00004400000018000200000009637573746f6d6572730000000172440000001d00020000000e637573746f6d6572735f706b657900000001694400000018000200000009736869706d656e747300000001724400000025000200000016736869706d656e74735f6d65726368616e745f6964780000000169440000001d00020000000e736869706d656e74735f706b65790000000169430000000d53454c4543542035005a0000000549",
    "31000000043200000004540000003a000272656c6e616d6500000000000000000000130040ffffffff000072656c6b696e6400000000000000000000120001ffffffff00004400000018000200000009637573746f6d6572730000000172440000001d00020000000e637573746f6d6572735f706b6579000000016973000000044400000018000200000009736869706d656e747300000001724400000025000200000016736869706d656e74735f6d65726368616e745f6964780000000169440000001d00020000000e736869706d656e74735f706b65790000000169430000000d53454c4543542035005a0000000549",
    "540000005d00037461626c655f6e616d6500000000000000000000130040ffffffff0000636f6c756d6e5f6e616d6500000000000000000000130040ffffffff0000646174615f747970650000000000000000000413ffffffffffff00004400000023000300000009637573746f6d65727300000002696400000006626967696e744400000022000300000009637573746f6d6572730000000373736e00000004746578744400000023000300000009736869706d656e747300000002696400000006626967696e74440000002c000300000009736869706d656e74730000000b6d65726368616e745f696400000006626967696e744400000025000300000009736869706d656e7473000000067374617475730000000474657874430000000d53454c4543542035005a0000000549",
    "31000000043200000004540000005d00037461626c655f6e616d6500000000000000000000130040ffffffff0000636f6c756d6e5f6e616d6500000000000000000000130040ffffffff0000646174615f747970650000000000000000000413ffffffffffff00004400000023000300000009637573746f6d65727300000002696400000006626967696e744400000022000300000009637573746f6d6572730000000373736e000000047465787473000000044400000023000300000009736869706d656e747300000002696400000006626967696e74440000002c000300000009736869706d656e74730000000b6d65726368616e745f696400000006626967696e744400000025000300000009736869706d656e7473000000067374617475730000000474657874430000000d53454c4543542035005a0000000549",
    "5400000038000272656c6e616d6500000000000000000000130040ffffffff0000636f756e7400000000000000000000140008ffffffff00004400000018000200000009736869706d656e74730000000133430000000d53454c4543542031005a0000000549",
    "310000000432000000045400000038000272656c6e616d6500000000000000000000130040ffffffff0000636f756e7400000000000000000000140008ffffffff00004400000018000200000009736869706d656e74730000000133430000000d53454c454354203100430000000d53454c4543542031005a0000000549",
    "540000005800036f626a6f6964000000000000000000001a0004ffffffff00006f626a737562696400000000000000000000170004ffffffff00006465736372697074696f6e0000000000000000000019ffffffffffff0000440000002d000300000004323230300000000130000000167374616e64617264207075626c696320736368656d61440000002b00030000000536353534300000000130000000136a6f696e656420746f20637573746f6d657273440000001d00030000000536353534300000000132000000056f776e65724400000026000300000005363535343000000001330000000e63757272656e7420737461747573430000000d53454c4543542034005a0000000549",
    "31000000043200000004540000005800036f626a6f6964000000000000000000001a0004ffffffff00006f626a737562696400000000000000000000170004ffffffff00006465736372697074696f6e0000000000000000000019ffffffffffff0000440000002d000300000004323230300000000130000000167374616e64617264207075626c696320736368656d61440000002b00030000000536353534300000000130000000136a6f696e656420746f20637573746f6d6572737300000004440000001d00030000000536353534300000000132000000056f776e65724400000026000300000005363535343000000001330000000e63757272656e7420737461747573430000000d53454c4543542034005a0000000549",
    "540000003d0002696e6465786e616d6500000000000000000000130040ffffffff0000696e6465786465660000000000000000000019ffffffffffff0000440000006300020000000e637573746f6d6572735f706b65790000004743524541544520554e4951554520494e44455820637573746f6d6572735f706b6579204f4e207075626c69632e637573746f6d657273205553494e4720627472656520286964294400000075000200000016736869706d656e74735f6d65726368616e745f6964780000005143524541544520494e44455820736869706d656e74735f6d65726368616e745f696478204f4e207075626c69632e736869706d656e7473205553494e4720627472656520286d65726368616e745f696429440000006300020000000e736869706d656e74735f706b65790000004743524541544520554e4951554520494e44455820736869706d656e74735f706b6579204f4e207075626c69632e736869706d656e7473205553494e472062747265652028696429430000000d53454c4543542033005a0000000549",
    "31000000043200000004540000003d0002696e6465786e616d6500000000000000000000130040ffffffff0000696e6465786465660000000000000000000019ffffffffffff0000440000006300020000000e637573746f6d6572735f706b65790000004743524541544520554e4951554520494e44455820637573746f6d6572735f706b6579204f4e207075626c69632e637573746f6d657273205553494e4720627472656520286964294400000075000200000016736869706d656e74735f6d65726368616e745f6964780000005143524541544520494e44455820736869706d656e74735f6d65726368616e745f696478204f4e207075626c69632e736869706d656e7473205553494e4720627472656520286d65726368616e745f6964297300000004440000006300020000000e736869706d656e74735f706b65790000004743524541544520554e4951554520494e44455820736869706d656e74735f706b6579204f4e207075626c69632e736869706d656e7473205553494e472062747265652028696429430000000d53454c4543542033005a0000000549",
];

/// A non-task session's catalog answers (including a mixed catalog and
/// application query, comments and indexes that a task would not see) are
/// byte-for-byte what the server answered before R3-312.
#[tokio::test]
async fn non_task_catalog_unchanged() {
    let (server, _agent, ordinary, _wire) = agent_wire().await;
    seed(&server);
    let (_listener, mut wire) = ordinary_wire(&server, &ordinary).await;
    let mut answers = Vec::new();
    for request in non_task_exchange() {
        answers.push(hex(&wire.exchange_raw(&request).await));
    }
    assert_eq!(answers.len(), RECORDED.len());
    for (i, (answer, recorded)) in answers.iter().zip(RECORDED).enumerate() {
        assert_eq!(answer, recorded, "exchange {i}; all answers: {answers:#?}");
    }
}
