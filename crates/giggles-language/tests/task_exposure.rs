//! GD-0061 R3-502: the native `show task exposure` (for an existing task and
//! for a `create task` draft) and `show task reads` lower to the SQL
//! statements and give the same rows through the same core functions.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::SqlResult;
use giggles_core::roles::{CatalogRole, NewRole, TablePrivilege};
use giggles_core::task::catalog::TaskDdlError;
use giggles_core::task::context::open_context;
use giggles_core::task::definition::{TaskName, WallClockMs};
use giggles_core::task::statements::{TaskStatement, parse as parse_sql};
use giggles_core::{Admin, Engine, Limits, SqlSession};
use giggles_language::{LanguageVersion, parse_task_with_version, show_task};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const V: LanguageVersion = LanguageVersion::Embedded022;
const TASK: &str = "inc-436";

const SQL_CREATE: &str = r#"CREATE TASK "inc-436"
  EXECUTOR repair_agent APPROVER oncall_lead
  ON shipments
  VISIBLE ROWS WHERE (merchant_id = 42)
  READABLE (id, status, failed_at, attempt_count)
  MASKED (customer_email)
  ALLOW UPDATE (status)
    OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' AND failed_at < '2026-11-05 17:07:00+00')
    NEW ROW WHERE (status = 'queued')
    AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS
    APPROVE ABOVE 5
  DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED
  WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2)
  EXPIRES '2099-12-31 00:00:00+00'"#;

const NATIVE_CREATE: &str = r#"create task "inc-436" for role repair_agent approver oncall_lead on shipments
  visible when merchant_id = 42
  readable (id, status, failed_at, attempt_count) masked (customer_email)
  allow update (status)
    from old.status = "failed" and old.failed_at >= timestamp("2026-11-05 17:00:00+00")
      and old.failed_at < timestamp("2026-11-05 17:07:00+00")
    to new.status = "queued"
    at most 25 distinct rows and 25 transitions approve above 5
  disclose at most 1000 rows read and 2097152 bytes
  limits (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2)
  expires timestamp("2099-12-31 00:00:00+00")"#;

fn now() -> WallClockMs {
    WallClockMs(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64)
}

struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
}

impl Fixture {
    /// The table (31 rows of merchant 42, 2 of merchant 43), the roles and
    /// their grants; no task.
    fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "lang-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let f = Self { path, admin: Some(admin) };
        f.sql(
            "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
             failed_at timestamptz, attempt_count int, customer_email text)",
        );
        let rows: Vec<String> = (1..=33)
            .map(|id| {
                let merchant = if id > 31 { 43 } else { 42 };
                format!("({id}, {merchant}, 'failed', '2026-11-05 17:03:00+00', 1, 'c{id}@shop.example')")
            })
            .collect();
        f.sql(&format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
        let roles = f.admin().roles();
        for (name, task_only) in [
            ("incident_controller", false),
            ("oncall_lead", false),
            ("repair_agent", true),
            ("stranger", false),
        ] {
            roles
                .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
                .unwrap();
            if task_only {
                roles.set_task_only(name, true).unwrap();
            }
        }
        for name in ["incident_controller", "repair_agent"] {
            roles
                .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
                .unwrap();
        }
        f
    }

    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }

    fn sql(&self, statement: &str) {
        self.admin()
            .sql_session()
            .execute(statement, &[], None)
            .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }

    fn role(&self, name: &str) -> CatalogRole {
        self.admin().roles().get(name).unwrap().unwrap()
    }

    fn create(&self) {
        let Some(TaskStatement::Create(new)) = parse_sql(SQL_CREATE) else {
            panic!("CREATE TASK did not parse");
        };
        self.admin()
            .tasks()
            .create(&self.role("incident_controller"), *new, now())
            .unwrap();
    }

    /// `statement` through the shared runner, as `role` on an ordinary session.
    fn show(&self, role: &str, statement: &TaskStatement) -> Result<SqlResult, TaskDdlError> {
        let extra = [("credential".to_string(), "aaaaaaaa as repair_agent".to_string())];
        show_task(&self.admin().sql_session(), &self.role(role), statement, now(), &extra)
            .expect("a task report statement")
    }

    /// A session bound to the task, with exact debits.
    fn agent(&self) -> SqlSession {
        let mut s = self.admin().sql_session();
        let ctx = open_context(
            &s,
            &TaskName::new(TASK).unwrap(),
            &"repair_agent".to_string(),
            [7; 32],
            now(),
            0,
        )
        .unwrap();
        s.bind_task(ctx).unwrap();
        s.set_disclosure_chunk(0, 0);
        s
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin = None;
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// The SQL and native texts parse to the same statement.
fn both(sql: &str, native: &str) -> TaskStatement {
    let from_sql = parse_sql(sql).unwrap_or_else(|| panic!("not a task statement: {sql}"));
    let from_native = parse_task_with_version(native, V).unwrap_or_else(|e| panic!("{native}: {e:?}"));
    assert_eq!(from_native, from_sql, "{native}");
    assert!(!matches!(from_sql, TaskStatement::Error(_)), "{sql}");
    from_sql
}

#[test]
fn native_and_sql_reports_equal() {
    let f = Fixture::new("task-exposure-native");
    // The draft, before the task exists.
    let sql_draft = format!("SHOW TASK EXPOSURE FOR {SQL_CREATE}");
    let native_draft = format!("show task exposure for {NATIVE_CREATE}");
    let draft = both(&sql_draft, &native_draft);
    assert!(matches!(draft, TaskStatement::ShowExposureDraft(_)));
    let native = parse_task_with_version(&native_draft, V).unwrap();
    let from_sql = f.show("incident_controller", &parse_sql(&sql_draft).unwrap()).unwrap();
    let from_native = f.show("incident_controller", &native).unwrap();
    assert_eq!(from_native, from_sql);
    assert!(!from_sql.rows.is_empty());
    // A draft that fails validation fails the same way in both.
    for role in ["stranger", "repair_agent"] {
        let a = f.show(role, &parse_sql(&sql_draft).unwrap()).unwrap_err();
        let b = f.show(role, &native).unwrap_err();
        assert_eq!(a, b, "{role}");
    }
    assert!(f.admin().tasks().list().unwrap().is_empty());
    // The created task.
    f.create();
    let shown = both(r#"SHOW TASK EXPOSURE "inc-436""#, r#"show task exposure "inc-436";"#);
    assert_eq!(shown, TaskStatement::ShowExposure(TASK.into()));
    let report = f.show("incident_controller", &shown).unwrap();
    let again = f
        .show("incident_controller", &parse_task_with_version(r#"show task exposure "inc-436""#, V).unwrap())
        .unwrap();
    assert_eq!(again, report);
    // The created report adds the charged and changed sections.
    assert_eq!(report.rows.len(), from_sql.rows.len() + 7 + 3);
    // Refusals agree too.
    assert_eq!(f.show("stranger", &shown).unwrap_err().sqlstate, "42501");
    // A task named exposure, and one named for.
    assert_eq!(
        parse_task_with_version("show task exposure", V).unwrap(),
        TaskStatement::Show(Some("exposure".into()))
    );
    assert_eq!(
        parse_task_with_version("show task exposure for", V).unwrap(),
        TaskStatement::ShowExposure("for".into())
    );
    // Other statements are not reports.
    assert!(
        show_task(&f.admin().sql_session(), &f.role("incident_controller"), &TaskStatement::Show(None), now(), &[])
            .is_none()
    );
}

#[test]
fn native_reads_equal_sql_reads() {
    let f = Fixture::new("task-reads-native");
    f.create();
    let mut s = f.agent();
    for sql in [
        "SELECT id, status FROM shipments WHERE id <= 5 ORDER BY id",
        "SELECT id FROM shipments ORDER BY id",
        "SELECT 1",
    ] {
        let page = s.execute_unreleased(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        s.release_page(page).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    drop(s);
    for (sql, native, rows) in [
        (r#"SHOW TASK READS "inc-436""#, r#"show task reads "inc-436""#, 3),
        (r#"SHOW TASK READS "inc-436" AFTER 1"#, r#"show task reads "inc-436" after 1"#, 2),
        (r#"SHOW TASK READS "inc-436" LIMIT 2"#, r#"show task reads "inc-436" limit 2;"#, 2),
        (
            r#"SHOW TASK READS "inc-436" AFTER 1 LIMIT 1"#,
            r#"show task reads "inc-436" after 1 limit 1"#,
            1,
        ),
    ] {
        let statement = both(sql, native);
        let from_sql = f.show("incident_controller", &statement).unwrap();
        let from_native = f
            .show("incident_controller", &parse_task_with_version(native, V).unwrap())
            .unwrap();
        assert_eq!(from_native, from_sql, "{native}");
        assert_eq!(from_sql.rows.len(), rows, "{sql}");
    }
    // The same refusal, and a task named reads.
    let statement = both(r#"SHOW TASK READS "inc-436""#, r#"show task reads "inc-436""#);
    assert_eq!(f.show("stranger", &statement).unwrap_err().sqlstate, "42501");
    assert_eq!(
        parse_task_with_version("show task reads", V).unwrap(),
        TaskStatement::Show(Some("reads".into()))
    );
    assert!(parse_task_with_version(r#"show task reads "inc-436" after"#, V).is_err());
}
