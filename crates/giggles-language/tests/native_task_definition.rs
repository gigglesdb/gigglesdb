//! Profile 0.22 native task statements lower to the SQL parser's TaskStatement (R3-112).
//! 0.21 is R2's last profile, so task syntax takes 0.22 (design-0 D14).
use giggles_core::task::definition::{LoadLimits, PredicateText, TaskPrivileges};
use giggles_core::task::statements::{TaskStatement, parse as parse_sql};
use giggles_language::{
    CapabilityInterface, CapabilityState, Error, LanguageVersion, Value, capability_manifest_for,
    parse_input_with_version, parse_task_with_version, profile_for,
};
use std::path::Path;

const V: LanguageVersion = LanguageVersion::Embedded022;
const PREVIOUS: LanguageVersion = LanguageVersion::Embedded021;

const SQL_INC_436: &str = r#"CREATE TASK "inc-436"
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
  EXPIRES '2026-11-05 17:30:00+00';"#;

const NATIVE_INC_436: &str = r#"create task "inc-436" for role repair_agent approver oncall_lead on shipments
  visible when merchant_id = 42
  readable (id, status, failed_at, attempt_count) masked (customer_email)
  allow update (status)
    from old.status = "failed" and old.failed_at >= timestamp("2026-11-05 17:00:00+00")
      and old.failed_at < timestamp("2026-11-05 17:07:00+00")
    to new.status = "queued"
    at most 25 distinct rows and 25 transitions approve above 5
  disclose at most 1000 rows read and 2097152 bytes
  limits (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2)
  expires timestamp("2026-11-05 17:30:00+00");"#;

fn create(statement: TaskStatement) -> giggles_core::task::definition::NewTask {
    match statement {
        TaskStatement::Create(task) => *task,
        other => panic!("not a create: {other:?}"),
    }
}

#[test]
fn native_inc_436_lowers_to_same_statement_as_sql() {
    let sql = create(parse_sql(SQL_INC_436).expect("sql task statement"));
    let native = create(parse_task_with_version(NATIVE_INC_436, V).unwrap());
    assert_eq!(native.name, sql.name);
    assert_eq!(native.name, "inc-436");
    assert_eq!(native.executor, sql.executor);
    assert_eq!(native.approver, sql.approver);
    assert_eq!(native.schema, sql.schema);
    assert_eq!(native.table, sql.table);
    assert_eq!(native.table, "shipments");
    assert_eq!(native.visible_rows, sql.visible_rows);
    assert_eq!(native.readable, sql.readable);
    assert_eq!(native.masked, sql.masked);
    assert_eq!(native.untrusted_text, sql.untrusted_text);
    let (nw, sw) = (native.allowed_write.as_ref().unwrap(), sql.allowed_write.as_ref().unwrap());
    assert_eq!(nw.columns, sw.columns);
    assert_eq!(nw.old_row, sw.old_row);
    assert_eq!(nw.new_row, sw.new_row);
    assert_eq!(native.mutation_caps, sql.mutation_caps);
    assert_eq!(native.disclosure_caps, sql.disclosure_caps);
    assert_eq!(native.limits, sql.limits);
    assert_eq!(native.approval_threshold, sql.approval_threshold);
    assert_eq!(native.expires, sql.expires);
    assert_eq!(native, sql);

    // The remaining optional forms, against their SQL spellings.
    let native = parse_task_with_version(
        "create task nightly for role bot on app.shipments visible when region = \"eu\" or region is null \
         untrusted text (note) readable (id, note, region) masked (email = \"x\", score = -1, vip = true) \
         disclose at most 50 rows read and 10 rows returned and 4096 bytes \
         limits (lock_wait_ms = 7, pending_write_bytes = 8, stored_previews = 9, preview_ttl_s = 10) \
         expires \"2026-12-01 00:00:00+00\"",
        V,
    )
    .unwrap();
    let sql = parse_sql(
        "CREATE TASK nightly EXECUTOR bot ON app.shipments VISIBLE ROWS WHERE (region = 'eu' OR region IS NULL) \
         UNTRUSTED TEXT (note) READABLE (id, note, region) MASKED (email = 'x', score = -1, vip = true) \
         DISCLOSE AT MOST 50 ROWS READ, 10 ROWS RETURNED, 4096 BYTES RETURNED \
         WITH (lock_wait_ms = 7, pending_write_bytes = 8, stored_previews = 9, preview_ttl_s = 10) \
         EXPIRES '2026-12-01 00:00:00+00'",
    )
    .unwrap();
    assert_eq!(native, sql);
    let task = create(native);
    assert_eq!(task.schema.as_deref(), Some("app"));
    assert_eq!(task.masked[1], ("score".into(), Some(Value::Int(-1))));
    assert_eq!(task.limits.statement_ms, LoadLimits::default().statement_ms);
    assert_eq!((task.allowed_write, task.approval_threshold), (None, 0));
}

fn visible(expression: &str) -> Result<String, Error> {
    let text = format!(
        "create task t for role r on shipments visible when {expression} \
         disclose at most 1 rows read and 1 bytes expires \"2026-12-01 00:00:00+00\""
    );
    parse_task_with_version(&text, V).map(|s| create(s).visible_rows.0)
}

#[test]
fn native_expressions_render_to_sql_predicate_text() {
    for (native, sql) in [
        ("a != \"it's\"", "a <> 'it''s'"),
        ("not a = 1 and (b = 2 or c = 3)", "NOT a = 1 AND (b = 2 OR c = 3)"),
        ("a - -1 > (b + 2) * 3", "a - -1 > (b + 2) * 3"),
        ("a - (b - c) = -a", "a - (b - c) = -a"),
        ("coalesce(a, 0) >= abs(b) and length(c) < 5", "coalesce(a, 0) >= abs(b) AND length(c) < 5"),
        ("case when a = 1 then true else false end", "CASE WHEN a = 1 THEN TRUE ELSE FALSE END"),
        ("`order` = 1 and failed_at >= timestamp(\"2026-11-05T17:00:00Z\")",
            "\"order\" = 1 AND failed_at >= '2026-11-05T17:00:00Z'"),
    ] {
        assert_eq!(visible(native).as_deref(), Ok(sql), "{native}");
    }
    // Bindings and block locals have no meaning in a stored task definition.
    assert_eq!(visible("a = $x"), Err(Error::Unsupported));
    assert_eq!(visible("a = @l.count"), Err(Error::Unsupported));
}

#[test]
fn revoke_show_grant_forms() {
    for (native, sql) in [
        ("revoke task \"inc-436\";", "REVOKE TASK \"inc-436\";"),
        ("show task \"inc-436\"", "SHOW TASK \"inc-436\""),
        ("show tasks;", "SHOW TASKS;"),
        ("grant undo, inspect on task \"inc-436\" to oncall_lead", "GRANT UNDO, INSPECT ON TASK \"inc-436\" TO oncall_lead"),
        ("grant inspect on task nightly to a, b", "GRANT INSPECT ON TASK nightly TO a, b"),
        ("revoke undo on task \"inc-436\" from oncall_lead, auditor", "REVOKE UNDO ON TASK \"inc-436\" FROM oncall_lead, auditor"),
    ] {
        assert_eq!(parse_task_with_version(native, V), Ok(parse_sql(sql).unwrap()), "{native}");
    }
    assert_eq!(
        parse_task_with_version("grant undo, inspect on task \"inc-436\" to oncall_lead", V),
        Ok(TaskStatement::Grant {
            grant: true,
            bits: TaskPrivileges::UNDO | TaskPrivileges::INSPECT,
            task: "inc-436".into(),
            roles: vec!["oncall_lead".into()],
        })
    );
    // Approval is the task's approver, not a grant: SQL gives 0A000.
    assert_eq!(parse_sql("GRANT APPROVE ON TASK t TO r"), Some(TaskStatement::Error("0A000")));
    assert_eq!(parse_task_with_version("grant approve on task t to r", V), Err(Error::Unsupported));
    for text in [
        "show task",
        "revoke task",
        "grant undo on task t",
        "revoke undo on task t to r",
        "show tasks extra",
        "work where id = 1",
        "create table t (id int64, primary key (id))",
    ] {
        assert!(matches!(parse_task_with_version(text, V), Err(Error::Syntax { .. })), "{text}");
    }
}

#[test]
fn old_prefix_in_to_clause_rejected() {
    let write = |from: &str, to: &str| {
        let text = format!(
            "create task t for role r on shipments visible when merchant_id = 42 \
             allow update (status) from {from} to {to} at most 2 distinct rows and 2 transitions \
             disclose at most 1 rows read and 1 bytes expires \"2026-12-01 00:00:00+00\""
        );
        parse_task_with_version(&text, V).map(|s| {
            let w = create(s).allowed_write.unwrap();
            (w.old_row.0, w.new_row.0)
        })
    };
    assert_eq!(write("old.status = \"failed\"", "old.status = \"queued\""), Err(Error::Unsupported));
    assert_eq!(write("new.status = \"failed\"", "new.status = \"queued\""), Err(Error::Unsupported));
    assert_eq!(visible("old.merchant_id = 42"), Err(Error::Unsupported));
    assert_eq!(visible("new.merchant_id = 42"), Err(Error::Unsupported));
    assert_eq!(
        write("old.status = \"failed\" and attempt_count < 3", "new.status = \"queued\" and status != \"x\""),
        Ok(("status = 'failed' AND attempt_count < 3".into(), "status = 'queued' AND status <> 'x'".into()))
    );
    // `on old.t` is a schema-qualified table, not a row prefix.
    let task = create(
        parse_task_with_version(
            "create task t for role r on old.shipments visible when a = 1 \
             disclose at most 1 rows read and 1 bytes expires \"2026-12-01 00:00:00+00\"",
            V,
        )
        .unwrap(),
    );
    assert_eq!((task.schema.as_deref(), task.table.as_str()), (Some("old"), "shipments"));
    assert_eq!(task.visible_rows, PredicateText("a = 1".into()));
}

fn all_versions() -> Vec<LanguageVersion> {
    (1..=22)
        .map(|minor| serde_json::from_str(&format!("\"0.{minor}\"")).unwrap())
        .collect()
}

#[test]
fn profile_0_21_rejects_create_task() {
    let versions = all_versions();
    assert_eq!(versions.last(), Some(&V));
    for version in &versions[..versions.len() - 1] {
        assert!(!version.supports_tasks());
        assert_eq!(parse_task_with_version(NATIVE_INC_436, *version), Err(Error::Unsupported), "{version:?}");
        assert_eq!(parse_task_with_version("show tasks", *version), Err(Error::Unsupported), "{version:?}");
    }
    assert!(V.supports_tasks());
    // The general parser is unchanged: task text and ordinary text parse the
    // same way in 0.21 and 0.22.
    for text in [
        NATIVE_INC_436,
        "show tasks",
        "revoke task t",
        "grant undo on task t to r",
        "work where id = $id order by id limit 10 select id",
        "show capabilities",
        "create table t (id int64, primary key (id))",
    ] {
        assert_eq!(
            format!("{:?}", parse_input_with_version(text, V)),
            format!("{:?}", parse_input_with_version(text, PREVIOUS)),
            "{text}"
        );
    }
    assert!(parse_input_with_version(NATIVE_INC_436, V).is_err());
}

fn key(entry: &giggles_language::Capability) -> (CapabilityInterface, &'static str, CapabilityState, &'static str) {
    (entry.interface, entry.feature, entry.state, entry.detail)
}

#[test]
fn capabilities_0_22_lists_task_definition() {
    let current = capability_manifest_for(V);
    let previous = capability_manifest_for(PREVIOUS);
    assert_eq!(current.language_version, "0.22");
    assert_eq!(current.profile, "embedded-trusted-native-0.22");
    let (task, rest): (Vec<_>, Vec<_>) =
        current.capabilities.iter().partition(|entry| entry.feature == "task_definition");
    assert_eq!(task.len(), 1);
    assert_eq!(task[0].interface, CapabilityInterface::RustText);
    assert_eq!(task[0].state, CapabilityState::Implemented);
    assert!(previous.capabilities.iter().all(|entry| entry.feature != "task_definition"));
    // 0.22 advertises what 0.21 does, plus task_definition.
    assert_eq!(
        rest.iter().map(|entry| key(entry)).collect::<Vec<_>>(),
        previous.capabilities.iter().map(key).collect::<Vec<_>>()
    );
    assert_eq!(
        serde_json::to_value(&current.limits).unwrap(),
        serde_json::to_value(&previous.limits).unwrap()
    );
    assert_eq!(profile_for(V).unsupported, profile_for(PREVIOUS).unsupported);
    assert_eq!(profile_for(V).scalar_types, profile_for(PREVIOUS).scalar_types);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for evidence in task[0].evidence.iter().chain(
        current.capabilities.iter().filter(|e| e.state == CapabilityState::Rejected).flat_map(|e| &e.evidence),
    ) {
        let source = std::fs::read_to_string(root.join(evidence.path)).unwrap();
        let offset = source
            .find(&format!("fn {}(", evidence.test))
            .unwrap_or_else(|| panic!("missing {} in {}", evidence.test, evidence.path));
        assert!(source[..offset].trim_end().ends_with("#[test]"), "{}", evidence.test);
    }
}

/// FNV-1a 64 of the compact manifest JSON.
fn fingerprint(version: LanguageVersion) -> (usize, u64) {
    let json = serde_json::to_string(&serde_json::to_value(capability_manifest_for(version)).unwrap()).unwrap();
    let hash = json.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (json.len(), hash)
}

#[test]
fn profile_0_21_capabilities_unchanged() {
    // Frozen from the 0.20 and 0.21 manifests without profile 0.22: first at
    // 85e0ef27, refrozen at main 3f9a2b1d, whose GD-0001 R2-NOCAP stack rewrote
    // shared manifest text (+930 bytes in both profiles), and refrozen at the
    // GD-0082 bench stack on main 66ef93ce, whose v2 transaction cap added
    // engine_rows.*.concurrent_explicit_sessions (+68 bytes in both profiles).
    // Refrozen at GD-0092 AM-3, whose removal of the per-database table cap
    // made limits.catalog_tables null instead of 128 (+1 byte in both profiles).
    // Refrozen for GD-0092's table width increase from 64 to 1600 columns
    // (+2 bytes in limits.columns_per_table in both profiles).
    // The manifest text differs slightly without the v2 engine.
    if cfg!(feature = "engine-rocksdb") {
        assert_eq!(fingerprint(LanguageVersion::Embedded020), (193_968, 0xe4ac_f177_3279_5193));
        assert_eq!(fingerprint(PREVIOUS), (201_610, 0xbdc1_f673_0fb5_8fe9));
    } else {
        assert_eq!(fingerprint(LanguageVersion::Embedded020), (193_965, 0x9e55_0e7b_f0fa_5406));
        assert_eq!(fingerprint(PREVIOUS), (201_607, 0xa4d5_a467_d47a_7fa6));
    }
}
