//! R3-309: read-log internals (retention interplay, the text fingerprint,
//! the SHOW TASK READS parser).
use super::*;
use crate::storage::engine::commit::ManualClock;
use crate::task::context::open_context;
use crate::task::definition::{
    LoadLimits, MutationCaps, NewTask, PredicateText, TaskName, WallClockMs,
};
use crate::task::retention::{sweep, RetentionConfig};
use crate::task::statements::{parse, TaskStatement};

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
// 2026-11-05 17:00:00 UTC.
const NOW: u64 = 1_793_898_000_000;

struct Fixture {
    admin: crate::storage::Admin,
    clock: Arc<ManualClock>,
    task: TaskObjectId,
}

fn fixture() -> Fixture {
    use crate::storage::roles::{NewRole, TablePrivilege};
    let root = crate::test_artifact_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = root.join(format!("task-readlog-{:032x}", u128::from_be_bytes(token)));
    let admin = crate::storage::Admin::create_engine(
        &path,
        crate::storage::Limits::default(),
        crate::storage::Engine::V2,
    )
    .unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
        &[],
        None,
    )
    .unwrap();
    sql.execute(
        "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'delivered'), (3, 7, 'failed')",
        &[],
        None,
    )
    .unwrap();
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
    }
    roles
        .grant_table(
            "shipments",
            "incident_controller",
            &[TablePrivilege::Select],
        )
        .unwrap();
    let issuer = roles.get("incident_controller").unwrap().unwrap();
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
                readable: vec!["id".into(), "status".into()],
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 1 << 20,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: "2026-12-31 00:00:00+00".into(),
            },
            WallClockMs(NOW as i64),
        )
        .unwrap();
    let clock = Arc::new(ManualClock::new(NOW));
    sql.v2_db().unwrap().set_wall_clock(clock.clone());
    let task = admin.tasks().get("inc-436").unwrap().unwrap().id;
    Fixture { admin, clock, task }
}

impl Fixture {
    fn agent(&self) -> SqlSession {
        let mut sql = self.admin.sql_session();
        let ctx = open_context(
            &sql,
            &TaskName::new("inc-436").unwrap(),
            &"agent_exec".to_string(),
            [7; 32],
            WallClockMs(self.clock_ms() as i64),
            0,
        )
        .unwrap();
        sql.bind_task(ctx).unwrap();
        sql.set_disclosure_chunk(0, 0);
        sql
    }

    fn clock_ms(&self) -> u64 {
        self.admin.sql_session().v2_db().unwrap().wall_now_ms()
    }

    fn records(&self) -> Vec<ReadRecord> {
        self.admin.task_read_log(self.task).unwrap()
    }
}

#[test]
fn finalize_does_not_resurrect_swept_entry() {
    let f = fixture();
    let mut s = f.agent();
    let p = s
        .prepare("SELECT id, status FROM shipments ORDER BY id")
        .unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let page = cursor.fetch_unreleased(1, 1 << 20).unwrap();
    s.release_page(page).unwrap();
    // The first debit created the record, open.
    let open = f.records();
    assert_eq!(open.len(), 1);
    assert_eq!((open[0].read_seq, open[0].outcome), (1, OUTCOME_OPEN));
    // The statement runs past the read-log retention; the sweep deletes it.
    f.clock.set(NOW + 2 * DAY_MS);
    let db = s.v2_db().unwrap();
    let config = RetentionConfig {
        read_log_ms: DAY_MS,
        ..RetentionConfig::default()
    };
    let stats = sweep(&db, &config).unwrap();
    assert_eq!((stats.read_logs_deleted, stats.debits_deleted), (1, 1));
    assert!(f.records().is_empty());
    // The statement's end does not recreate the swept record.
    drop(cursor);
    s.finish_task_statement(Ok(()));
    assert!(f.records().is_empty());
    // The next statement takes the next read_seq; nothing reused it.
    s.set_task_statement_text("SELECT 1");
    let page = s.execute_unreleased("SELECT 1", &[], None).unwrap();
    s.release_page(page).unwrap();
    let after = f.records();
    assert_eq!(after.len(), 1);
    assert_eq!((after[0].read_seq, after[0].outcome), (2, OUTCOME_COMPLETE));
}

#[test]
fn finalize_charges_pending_rows_read_once() {
    let f = fixture();
    let mut s = f.agent();
    let p = s
        .prepare("SELECT id, status FROM shipments ORDER BY id")
        .unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let page = cursor.fetch_unreleased(1, 1 << 20).unwrap();
    s.release_page(page).unwrap();
    // A fetched page never released: its rows were read, never returned.
    let _unreleased = cursor.fetch_unreleased(5, 1 << 20).unwrap();
    s.finish_task_statement(Ok(()));
    drop(cursor);
    s.finish_task_statement(Ok(()));
    let records = f.records();
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r.outcome, OUTCOME_COMPLETE);
    assert_eq!((r.charged_rows_read, r.released_rows, r.pages), (2, 1, 1));
    assert!(r.rows_scanned >= 2, "{r:?}");
    let check = f.admin.task_read_verify(f.task).unwrap();
    assert!(check.ok(), "{check:?}");
    assert_eq!(check.head_rows_read, 2);
}

#[test]
fn fingerprint_caps_at_a_character_boundary() {
    let ascii = "a".repeat(1500);
    let print = fingerprint_and_cap(&ascii);
    assert_eq!((print.len, print.text.len()), (1500, 1024));
    let expected: [u8; 32] = Sha256::digest(ascii.as_bytes()).into();
    assert_eq!(print.sha256, expected);
    // 1,023 ASCII bytes then a 3-byte character: the cut falls before it.
    let text = format!("{}{}", "a".repeat(1023), "€".repeat(4));
    let print = fingerprint_and_cap(&text);
    assert_eq!(print.text.len(), 1023);
    assert_eq!(print.len as usize, text.len());
    let short = fingerprint_and_cap("SELECT 1");
    assert_eq!((short.len, short.text.as_str()), (8, "SELECT 1"));
}

#[test]
fn class_codes_round_trip() {
    for class in StatementClass::ALL {
        assert_eq!(class_of_code(class_code(class)), Some(class));
    }
    assert_eq!(outcome_of("42501"), OUTCOME_DENIED);
    assert_eq!(outcome_of("0A000"), OUTCOME_DENIED);
    assert_eq!(outcome_of("54T01"), OUTCOME_BUDGET);
    assert_eq!(outcome_of("57014"), OUTCOME_CANCELLED);
    assert_eq!(outcome_of("22012"), OUTCOME_ERROR);
}

#[test]
fn show_task_reads_parses() {
    assert_eq!(
        parse("SHOW TASK READS \"inc-436\" AFTER 5 LIMIT 10"),
        Some(TaskStatement::ShowReads {
            task: "inc-436".into(),
            after: Some(5),
            limit: Some(10),
        })
    );
    assert_eq!(
        parse("show task reads t;"),
        Some(TaskStatement::ShowReads {
            task: "t".into(),
            after: None,
            limit: None
        })
    );
    // A task named `reads` is still SHOW TASK.
    assert_eq!(
        parse("SHOW TASK reads"),
        Some(TaskStatement::Show(Some("reads".into())))
    );
    assert_eq!(
        parse("SHOW TASK READS t AFTER x"),
        Some(TaskStatement::Error("42601"))
    );
    assert_eq!(
        parse("SHOW TASK READS t LIMIT 1 AFTER 2"),
        Some(TaskStatement::Error("42601"))
    );
}
