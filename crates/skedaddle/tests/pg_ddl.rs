#[path = "support/root.rs"]
mod test_root;

use serde_json::json;
use skedaddle::pg::ddl::*;
use skedaddle::pg::pgoutput::Message;
use std::cell::RefCell;
use std::io::Write;
use std::path::Path;

fn attributes(kind: &str) -> serde_json::Value {
    let (oid, collation, storage) = if kind == "integer" {
        (23, 0, "p")
    } else {
        (25, 100, "x")
    };
    json!({"type_oid":oid,"typmod":-1,"collation":collation,"acl":null,"storage":storage,
        "compression":"","options":null,"fdwoptions":null,"local":true,"inhcount":0})
}

fn column(name: &str, attnum: i32, kind: &str) -> Column {
    Column {
        attnum,
        name: name.into(),
        type_name: kind.into(),
        notnull: false,
        dropped: false,
        identity: String::new(),
        generated: String::new(),
        missing: false,
        missing_value: None,
        default: None,
        attributes: attributes(kind),
    }
}

fn baseline() -> TableDescriptor {
    TableDescriptor {
        oid: 42,
        schema: "miniflux".into(),
        columns: vec![column("id", 1, "integer")],
        relation: json!({"kind":"r","partition":false,"filenode":42,"name":"feeds","namespace":2200,
            "owner":10,"acl":null,"options":null,"replica_identity":"d","tablespace":0,
            "persistence":"p","access_method":2,"row_security":false,"force_row_security":false,
            "partition_key":null,"partition_bound":null,"constraints":[],"indexes":[],"triggers":[],"policies":[],"parents":[]}),
    }
}

fn record(sql: &str, before: &TableDescriptor, version: u64) -> DdlRecord {
    let mut after = before.clone();
    if let Ok(additions) = parse_additions(sql) {
        for addition in additions.columns {
            let mut col = column(
                &addition.name,
                after.columns.len() as i32 + 1,
                &addition.type_name,
            );
            match addition.default {
                Some(LiteralDefault::Integer(value)) => {
                    col.default = Some(value.to_string());
                    col.missing = true;
                    col.missing_value = Some(json!([value]));
                }
                Some(LiteralDefault::Text(value)) => {
                    col.default = Some(format!("'{}'::text", value.replace('\'', "''")));
                    col.missing = true;
                    col.missing_value = Some(json!([value]));
                }
                None => {}
            }
            after.columns.push(col);
        }
    }
    DdlRecord {
        origin: None,
        format: WIRE_FORMAT,
        installation: "test".into(),
        server_major: 16,
        version,
        tag: if sql
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("alter table")
        {
            "ALTER TABLE"
        } else {
            "CREATE INDEX"
        }
        .into(),
        xid: "123".into(),
        query: sql.into(),
        commands: vec![DdlCommand {
            classid: Some(1259),
            objid: Some(after.oid),
            objsubid: Some(0),
            command_tag: "ALTER TABLE".into(),
            object_type: "table".into(),
            schema_name: Some(after.schema),
            object_identity: Some("miniflux.feeds".into()),
            in_extension: false,
            columns: Some(after.columns),
            relation: Some(after.relation),
        }],
    }
}

fn decoded(
    bytes: &[u8],
    prefix: &str,
    transactional: bool,
    installation: &str,
) -> Result<DdlRecord, DdlError> {
    decode(
        &Message::LogicalMessage {
            transactional,
            lsn: 99,
            prefix,
            content: bytes,
        },
        installation,
    )
}

#[test]
fn codec_round_trip_and_rejects_foreign_or_malformed_messages() {
    let original = record(
        "ALTER TABLE feeds ADD COLUMN title text DEFAULT 'it''s fine'",
        &baseline(),
        1,
    );
    let bytes = encode(&original).unwrap();
    assert_eq!(
        decoded(&bytes, MESSAGE_PREFIX, true, "test").unwrap(),
        original
    );
    assert!(decoded(&bytes, "other", true, "test").is_err());
    assert!(decoded(&bytes, MESSAGE_PREFIX, false, "test").is_err());
    assert!(decoded(&bytes, MESSAGE_PREFIX, true, "another").is_err());
    assert!(decoded(b"{}", MESSAGE_PREFIX, true, "test").is_err());
    assert!(decoded(b"not json", MESSAGE_PREFIX, true, "test").is_err());
    assert!(decoded(&[0xff], MESSAGE_PREFIX, true, "test").is_err());
    let mut bad = serde_json::to_value(&original).unwrap();
    for (field, value) in [
        ("format", json!(2)),
        ("server_major", json!(19)),
        ("version", json!(0)),
        ("xid", json!("bad")),
        ("extra", json!(true)),
    ] {
        bad[field] = value;
        assert!(
            decoded(
                &serde_json::to_vec(&bad).unwrap(),
                MESSAGE_PREFIX,
                true,
                "test"
            )
            .is_err()
        );
        bad = serde_json::to_value(&original).unwrap();
    }
    bad["commands"][0]["columns"][0]["attnum"] = json!(0);
    assert!(
        decoded(
            &serde_json::to_vec(&bad).unwrap(),
            MESSAGE_PREFIX,
            true,
            "test"
        )
        .is_err()
    );
    assert!(
        decode(
            &Message::Truncate {
                cascade: false,
                restart_identity: false,
                relation_ids: vec![]
            },
            "test"
        )
        .is_err()
    );
    assert!(
        decoded(
            &vec![b' '; MAX_MESSAGE_BYTES + 1],
            MESSAGE_PREFIX,
            true,
            "test"
        )
        .is_err()
    );
}

#[test]
fn oid_numbers_are_strict_and_cover_unsigned_range() {
    let original = record("ALTER TABLE feeds ADD COLUMN x integer", &baseline(), 1);
    for oid in [2_147_483_648u32, u32::MAX] {
        let mut value = serde_json::to_value(&original).unwrap();
        value["commands"][0]["objid"] = json!(oid);
        let result = decoded(
            &serde_json::to_vec(&value).unwrap(),
            MESSAGE_PREFIX,
            true,
            "test",
        )
        .unwrap();
        assert_eq!(result.commands[0].objid, Some(oid));
    }
    for field in ["classid", "objid"] {
        for bad in [
            json!(-1),
            json!(4_294_967_296u64),
            json!(1.5),
            json!("42"),
            json!(true),
            json!(0),
            json!(null),
        ] {
            let mut value = serde_json::to_value(&original).unwrap();
            value["commands"][0][field] = bad;
            assert!(
                decoded(
                    &serde_json::to_vec(&value).unwrap(),
                    MESSAGE_PREFIX,
                    true,
                    "test"
                )
                .is_err()
            );
        }
    }
}

#[test]
fn identity_less_privileges_decode_but_only_owned_grant_revoke_are_internal() {
    let original = record("ALTER TABLE feeds ADD COLUMN x integer", &baseline(), 1);
    for tag in ["GRANT", "REVOKE", "ALTER DEFAULT PRIVILEGES"] {
        let mut value = serde_json::to_value(&original).unwrap();
        value["tag"] = json!(tag);
        let command = &mut value["commands"][0];
        command["command_tag"] = json!(tag);
        command["object_type"] = json!("TABLE");
        for field in [
            "classid",
            "objid",
            "objsubid",
            "schema_name",
            "object_identity",
            "columns",
            "relation",
        ] {
            command[field] = json!(null);
        }
        let event = decoded(
            &serde_json::to_vec(&value).unwrap(),
            MESSAGE_PREFIX,
            true,
            "test",
        )
        .unwrap();
        assert_eq!(encode(&event).unwrap(), serde_json::to_vec(&event).unwrap());
        for origin in [None, Some("foreign"), Some("test")] {
            let mut event = event.clone();
            event.origin = origin.map(str::to_owned);
            let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
            let supported = matches!(
                versions.classify_transaction(&[event]),
                Classification::Supported { .. }
            );
            assert_eq!(
                supported,
                origin == Some("test") && tag != "ALTER DEFAULT PRIVILEGES"
            );
        }
        for (field, bad) in [
            ("classid", json!(1259)),
            ("objid", json!(42)),
            ("objsubid", json!(0)),
            ("schema_name", json!("public")),
            ("object_identity", json!("public.feeds")),
            ("columns", json!([])),
            ("relation", json!({})),
            ("command_tag", json!("ALTER TABLE")),
        ] {
            let mut bad_value = value.clone();
            bad_value["commands"][0][field] = bad;
            assert!(
                decoded(
                    &serde_json::to_vec(&bad_value).unwrap(),
                    MESSAGE_PREFIX,
                    true,
                    "test"
                )
                .is_err(),
                "{tag} {field}"
            );
        }
        value["tag"] = json!("ALTER TABLE");
        value["commands"][0]["command_tag"] = json!("ALTER TABLE");
        assert!(
            decoded(
                &serde_json::to_vec(&value).unwrap(),
                MESSAGE_PREFIX,
                true,
                "test"
            )
            .is_err()
        );
    }
    for field in ["classid", "objid", "objsubid", "object_identity"] {
        let mut value = serde_json::to_value(&original).unwrap();
        value["commands"][0][field] = json!(null);
        assert!(
            decoded(
                &serde_json::to_vec(&value).unwrap(),
                MESSAGE_PREFIX,
                true,
                "test"
            )
            .is_err()
        );
    }
}

#[test]
fn supported_changes_advance_descriptors_and_versions() {
    let before = baseline();
    let first = record(
        "ALTER TABLE miniflux.feeds ADD COLUMN title text DEFAULT 'a,b', ADD COLUMN count integer DEFAULT -2",
        &before,
        1,
    );
    let after = TableDescriptor::from_command(&first.commands[0]).unwrap();
    let second = record("ALTER TABLE feeds ADD COLUMN optional text", &after, 2);
    let mut tracker = SchemaVersions::new("test".into(), 0, [before]).unwrap();
    assert!(matches!(
        tracker.classify_transaction(&[first, second]),
        Classification::Supported { .. }
    ));
    assert_eq!(tracker.last_version(), 2);
    assert_eq!(tracker.table(42).unwrap().version, 2);
    assert_eq!(tracker.table(42).unwrap().descriptor.columns.len(), 4);
}

#[test]
fn unsupported_transaction_is_atomic_and_sticky() {
    let before = baseline();
    let first = record("ALTER TABLE feeds ADD COLUMN title text", &before, 1);
    let second = record("ALTER TABLE feeds DROP COLUMN id", &before, 2);
    let mut tracker = SchemaVersions::new("test".into(), 0, [before.clone()]).unwrap();
    let result = tracker.classify_transaction(&[first.clone(), second]);
    let Classification::SafeBoundaryStop(stop) = &result else {
        panic!("expected boundary");
    };
    assert!(stop.capture_continues && stop.stop_apply_before_transaction && stop.readiness_blocked);
    assert!(!stop.remediation.is_empty());
    assert_eq!(tracker.last_version(), 0);
    assert_eq!(tracker.table(42).unwrap().descriptor, before);
    assert_eq!(tracker.classify_transaction(&[first]), result);
}

#[test]
fn metadata_changes_and_version_gaps_fail_closed() {
    for mutation in 0..9 {
        let before = baseline();
        let mut ddl = record(
            "ALTER TABLE feeds ADD COLUMN title text DEFAULT 'x'",
            &before,
            1,
        );
        match mutation {
            0 => ddl.commands[0].relation.as_mut().unwrap()["filenode"] = json!(999),
            1 => ddl.commands[0].columns.as_mut().unwrap()[0].name = "renamed".into(),
            2 => ddl.commands[0].columns.as_mut().unwrap()[1].notnull = true,
            3 => ddl.commands[0].columns.as_mut().unwrap()[1].default = Some("now()".into()),
            4 => {
                ddl.commands[0].columns.as_mut().unwrap()[1].missing_value = Some(json!(["wrong"]))
            }
            5 => ddl.commands[0].in_extension = true,
            6 => ddl.version = 2,
            7 => ddl.commands.clear(),
            8 => ddl.commands[0].relation.as_mut().unwrap()["indexes"] = json!(["new index"]),
            _ => unreachable!(),
        }
        let mut tracker = SchemaVersions::new("test".into(), 0, [before]).unwrap();
        assert!(
            matches!(
                tracker.classify_transaction(&[ddl]),
                Classification::SafeBoundaryStop(_)
            ),
            "mutation {mutation}"
        );
    }
}

#[test]
fn narrow_parser_rejects_unqualified_forms() {
    for sql in [
        "ALTER TABLE feeds ADD COLUMN n integer NOT NULL DEFAULT 0",
        "ALTER TABLE feeds ADD COLUMN n serial",
        "ALTER TABLE feeds ADD COLUMN n integer DEFAULT random()",
        "ALTER TABLE feeds ADD COLUMN n integer GENERATED ALWAYS AS IDENTITY",
        "ALTER TABLE feeds ADD COLUMN n text COLLATE \"C\"",
        "ALTER TABLE feeds ADD COLUMN n hstore",
        "ALTER TABLE feeds ADD COLUMN n bool DEFAULT 'f'",
        "ALTER TABLE feeds ADD COLUMN n text; DROP TABLE feeds",
        "ALTER TABLE feeds ADD COLUMN n text DEFAULT E'escape'",
        "ALTER TABLE feeds ADD COLUMN n text -- comment",
        "ALTER TABLE feeds ADD COLUMN n integer DEFAULT 2147483648",
        "ALTER TABLE feeds ADD COLUMN n integer, DROP COLUMN id",
    ] {
        assert!(parse_additions(sql).is_err(), "{sql}");
    }
    assert_eq!(
        parse_additions(
            "ALTER TABLE \"miniflux\".\"feeds\" ADD COLUMN \"Title\" text DEFAULT 'a;''b'"
        )
        .unwrap()
        .columns[0]
            .name,
        "Title"
    );
}

#[test]
fn classifies_real_miniflux_corpus_as_text() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/spikes/s3-ddl");
    let mut files: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("miniflux-")
                && p.extension().is_some_and(|s| s == "sql")
        })
        .collect();
    files.sort();
    assert_eq!(files.len(), 27, "review any corpus addition explicitly");
    let mut supported = 0;
    let mut rejected = 0;
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let mut classified = 0;
        for statement in text.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let lower = statement.to_ascii_lowercase();
            if !(lower.starts_with("alter ")
                || lower.starts_with("create ")
                || lower.starts_with("drop "))
            {
                continue;
            }
            let expected_supported = lower
                .starts_with("alter table feeds add column scraper_rules text")
                || lower.starts_with("alter table feeds add column rewrite_rules text")
                || (lower.starts_with("alter table integrations add column wallabag_")
                    && lower.contains(" text default"));
            let mut before = baseline();
            if lower.starts_with("alter table integrations") {
                before.relation["name"] = json!("integrations");
            }
            let ddl = record(statement, &before, 1);
            let mut tracker = SchemaVersions::new("test".into(), 0, [before]).unwrap();
            let actual = matches!(
                tracker.classify_transaction(&[ddl]),
                Classification::Supported { .. }
            );
            assert_eq!(
                actual,
                expected_supported,
                "{}: {statement}",
                file.display()
            );
            if actual {
                supported += 1;
            } else {
                rejected += 1;
            }
            classified += 1;
        }
        assert!(classified > 0, "{} was not classified", file.display());
    }
    assert_eq!(supported, 21);
    assert!(rejected > 60);
    eprintln!(
        "S3 miniflux corpus: 27 files, {supported} supported statements, {rejected} safe boundaries"
    );
}

#[test]
fn fingerprint_drift_at_watermark_and_final_marker_is_sticky() {
    for point in [PollPoint::Watermark(100), PollPoint::FinalMarker(200)] {
        let base = "a".repeat(32);
        let mut profile = FingerprintProfile::new(base.clone()).unwrap();
        assert!(profile.poll(PollPoint::Watermark(1), base.clone()).is_ok());
        let blocker = profile.poll(point.clone(), "b".repeat(32)).unwrap_err();
        assert_eq!(blocker.point, point);
        assert_eq!(
            profile.poll(PollPoint::FinalMarker(300), base).unwrap_err(),
            blocker
        );
        assert!(profile.blocker().is_some());
    }
    assert!(FingerprintProfile::new("bad".into()).is_err());
    assert!(
        FingerprintProfile::new("a".repeat(32))
            .unwrap()
            .poll(PollPoint::FinalMarker(1), "bad".into())
            .is_err()
    );
}

#[test]
fn administrative_privileges_advance_only_with_exact_origin_and_continuity() {
    for tag in ["GRANT", "REVOKE"] {
        let mut event = record("privilege statement", &baseline(), 1);
        event.tag = tag.into();
        event.commands.clear();
        event.origin = Some("test".into());
        let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
        assert_eq!(
            versions.classify_transaction(&[event.clone()]),
            Classification::Supported {
                advanced_tables: Default::default(),
            }
        );
        assert_eq!(versions.last_version(), 1);
        assert_eq!(versions.table(42).unwrap().version, 0);
        assert!(versions.blocker().is_none());
        event.version = 3;
        let Classification::SafeBoundaryStop(stop) = versions.classify_transaction(&[event]) else {
            panic!("administrative events must preserve version continuity");
        };
        assert!(stop.reason.contains("gap/replay"));
        assert_eq!(versions.last_version(), 1);
    }
    for (tag, origin) in [
        ("REVOKE", None),
        ("GRANT", None),
        ("CREATE TYPE", Some("test")),
        ("DROP TABLE", Some("test")),
        ("REVOKE", Some("wrong")),
        ("GRANT", Some("wrong")),
    ] {
        let mut event = record("privilege statement", &baseline(), 1);
        event.tag = tag.into();
        event.commands.clear();
        event.origin = origin.map(str::to_owned);
        let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
        assert!(
            matches!(
                versions.classify_transaction(&[event]),
                Classification::SafeBoundaryStop(_)
            ),
            "{tag} {origin:?}"
        );
        assert_eq!(versions.last_version(), 0);
    }
}

fn internal_record(tag: &str) -> DdlRecord {
    let mut event = record("tool metadata DDL", &baseline(), 1);
    event.tag = tag.into();
    event.origin = Some("test".into());
    event.commands[0].command_tag = tag.into();
    event.commands[0].objid = Some(100);
    event.commands[0].schema_name = Some("_skedaddle".into());
    event.commands[0].object_identity = Some("_skedaddle.x".into());
    event
}

#[test]
fn matching_origin_internal_objects_advance_global_version_without_descriptors() {
    for tag in ["CREATE TABLE", "ALTER TABLE", "CREATE INDEX"] {
        let event = internal_record(tag);
        let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
        let before = versions.table(42).unwrap().clone();
        assert_eq!(
            versions.classify_transaction(&[event]),
            Classification::Supported {
                advanced_tables: Default::default(),
            }
        );
        assert_eq!(versions.last_version(), 1);
        assert_eq!(versions.table(42), Some(&before));
        assert!(versions.table(100).is_none());
    }
}

#[test]
fn internal_schema_identity_and_only_the_allowed_empty_command_tags_are_accepted() {
    for identity in ["_skedaddle", "\"_skedaddle\""] {
        let mut event = internal_record("CREATE SCHEMA");
        event.commands[0].schema_name = None;
        event.commands[0].object_type = "schema".into();
        event.commands[0].object_identity = Some(identity.into());
        let mut versions = SchemaVersions::new("test".into(), 0, []).unwrap();
        assert!(matches!(
            versions.classify_transaction(&[event]),
            Classification::Supported { .. }
        ));
    }
    for tag in [
        "CREATE SCHEMA",
        "CREATE TABLE",
        "ALTER TABLE",
        "CREATE INDEX",
        "DROP TABLE",
        "CREATE TYPE",
        "DO",
    ] {
        let mut event = internal_record(tag);
        event.commands.clear();
        let mut versions = SchemaVersions::new("test".into(), 0, []).unwrap();
        assert_eq!(
            matches!(
                versions.classify_transaction(&[event]),
                Classification::Supported { .. }
            ),
            matches!(
                tag,
                "CREATE SCHEMA" | "CREATE TABLE" | "ALTER TABLE" | "CREATE INDEX"
            ),
            "{tag}"
        );
    }
}

#[test]
fn internal_ddl_fails_closed_on_public_mixed_missing_untagged_or_wrong_origin() {
    for case in [
        "public",
        "mixed",
        "missing schema",
        "untagged",
        "wrong origin",
        "wrong case",
        "foreign schema identity",
    ] {
        let mut event = internal_record("CREATE TABLE");
        match case {
            "public" => event.commands[0].schema_name = Some("public".into()),
            "mixed" => {
                let mut public = event.commands[0].clone();
                public.schema_name = Some("public".into());
                event.commands.push(public);
            }
            "missing schema" => event.commands[0].schema_name = None,
            "untagged" => event.origin = None,
            "wrong origin" => event.origin = Some("wrong".into()),
            "wrong case" => event.commands[0].schema_name = Some("_Skedaddle".into()),
            "foreign schema identity" => {
                event.tag = "CREATE SCHEMA".into();
                event.commands[0].command_tag = "CREATE SCHEMA".into();
                event.commands[0].object_type = "schema".into();
                event.commands[0].schema_name = None;
                event.commands[0].object_identity = Some("public".into());
            }
            _ => unreachable!(),
        }
        let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
        assert!(
            matches!(
                versions.classify_transaction(&[event]),
                Classification::SafeBoundaryStop(_)
            ),
            "{case}"
        );
        assert_eq!(versions.last_version(), 0);
        assert_eq!(versions.table(42).unwrap().version, 0);
    }
}

#[test]
fn internal_ddl_cannot_hide_gaps_replays_xid_mismatch_or_later_application_ddl() {
    for case in ["gap", "replay", "xid", "application"] {
        let first = internal_record("CREATE TABLE");
        let mut second = internal_record("ALTER TABLE");
        second.version = 2;
        match case {
            "gap" => second.version = 3,
            "replay" => second.version = 1,
            "xid" => second.xid = "124".into(),
            "application" => second.commands[0].schema_name = Some("public".into()),
            _ => unreachable!(),
        }
        let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
        assert!(
            matches!(
                versions.classify_transaction(&[first, second]),
                Classification::SafeBoundaryStop(_)
            ),
            "{case}"
        );
        assert_eq!(versions.last_version(), 0);
        assert_eq!(versions.table(42).unwrap().version, 0);
    }
}

#[test]
fn optional_origin_preserves_legacy_wire_format_and_rejects_other_unknown_fields() {
    let event = record("ALTER TABLE feeds ADD COLUMN title text", &baseline(), 1);
    let legacy = encode(&event).unwrap();
    assert!(
        serde_json::from_slice::<serde_json::Value>(&legacy)
            .unwrap()
            .get("origin")
            .is_none()
    );
    let legacy_record = decoded(&legacy, MESSAGE_PREFIX, true, "test").unwrap();
    assert_eq!(legacy_record.origin, None);
    let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
    assert!(matches!(
        versions.classify_transaction(&[legacy_record]),
        Classification::Supported { .. }
    ));
    let mut tagged = event;
    tagged.origin = Some("test".into());
    assert_eq!(
        decoded(&encode(&tagged).unwrap(), MESSAGE_PREFIX, true, "test").unwrap(),
        tagged
    );
}

#[test]
fn administrative_privileges_do_not_hide_transaction_mismatch_or_later_ddl() {
    let mut revoke = record("REVOKE UPDATE ON feeds FROM app", &baseline(), 1);
    revoke.tag = "REVOKE".into();
    revoke.commands.clear();
    revoke.origin = Some("test".into());
    for mismatch in [false, true] {
        let mut later = revoke.clone();
        later.version = 2;
        if mismatch {
            later.xid = "124".into();
        } else {
            later.tag = "CREATE TYPE".into();
        }
        let mut versions = SchemaVersions::new("test".into(), 0, [baseline()]).unwrap();
        assert!(matches!(
            versions.classify_transaction(&[revoke.clone(), later]),
            Classification::SafeBoundaryStop(_)
        ));
        assert_eq!(versions.last_version(), 0);
    }
}

#[test]
fn trigger_sql_is_versioned_and_execution_is_journaled() {
    for major in 16..=18 {
        let sql = TriggerSql::new("test", major * 10000).unwrap();
        assert!(sql.install.contains("ON ddl_command_end"));
        assert!(
            sql.install
                .contains("pg_logical_emit_message(true,'skedaddle'")
        );
        assert!(sql.install.contains("'commands',details"));
        assert!(sql.install.contains("'schema_name',d.schema_name"));
        assert!(
            sql.install
                .contains("'origin',nullif(current_setting('skedaddle.admin_origin',true),'')")
        );
        for table in [
            "fence_state",
            "cutover_marker",
            "cutover_activation",
            "authority",
            "effects",
        ] {
            let provision = sql
                .install
                .find(&format!("CREATE TABLE IF NOT EXISTS _skedaddle.{table}("))
                .unwrap();
            assert!(provision < sql.install.find("CREATE EVENT TRIGGER").unwrap());
        }
        for batch in [&sql.install, &sql.remove] {
            assert!(batch.contains("set_config('skedaddle.admin_origin', installation, true)"));
            assert!(batch.contains("WHERE installation IS NOT NULL"));
            assert!(batch.find("set_config(").unwrap() < batch.find("DROP EVENT TRIGGER").unwrap());
        }
        assert!(sql.install.starts_with("BEGIN;"));
        assert!(sql.install.ends_with("COMMIT;"));
        assert!(sql.verify.contains("e.evtenabled='O'"));
        for cast in [
            "d.classid::bigint",
            "d.objid::bigint",
            "a.atttypid::bigint",
            "a.attcollation::bigint",
        ] {
            assert!(sql.install.contains(cast));
            assert!(sql.verify.contains(cast));
        }
        for intent in [TriggerIntent::Install, TriggerIntent::Remove] {
            let calls = RefCell::new(Vec::new());
            sql.run(
                intent,
                |actual, _| {
                    assert_eq!(actual, intent);
                    calls.borrow_mut().push("journal");
                    Ok(())
                },
                |_| {
                    calls.borrow_mut().push("execute");
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(*calls.borrow(), ["journal", "execute"]);
            assert!(
                sql.run(
                    intent,
                    |_, _| Err(DdlError("disk full".into())),
                    |_| panic!("must not execute without intent")
                )
                .is_err()
            );
        }
    }
    assert!(TriggerSql::new("bad';--", 160000).is_err());
    assert!(TriggerSql::new("test", 150000).is_err());
    assert!(TriggerSql::new("test", 190000).is_err());
}

// Harness opt-in uses a real privileged PG16/17/18 with wal_level=logical.
// The harness owns server lifecycle; these tests never launch Docker themselves.
// Set SKEDADDLE_PG_DDL_URL to its disposable database connection string.
type LiveResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static LIVE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
struct Live {
    client: tokio_postgres::Client,
    id: String,
    sql: TriggerSql,
}

impl Live {
    async fn open(name: &str) -> LiveResult<Option<Self>> {
        let Ok(url) = std::env::var("SKEDADDLE_PG_DDL_URL") else {
            // Bypass libtest's output capture so plain cargo test shows skips.
            writeln!(
                std::io::stderr(),
                "SKIP {name}: real-PG harness unavailable; set SKEDADDLE_PG_DDL_URL (PG16/17/18, superuser, wal_level=logical). No live qualification claimed."
            )?;
            return Ok(None);
        };
        let (client, connection) = tls_connect(&url).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("DDL test connection: {error}");
            }
        });
        let id = format!(
            "p5f_{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        let version: String = client
            .query_one("SHOW server_version_num", &[])
            .await?
            .get(0);
        let sql = TriggerSql::new(&id, version.parse()?)?;
        client.batch_execute(&format!("CREATE SCHEMA {id}; CREATE TABLE {id}.mapped(id integer); CREATE PUBLICATION {id} FOR TABLE {id}.mapped;")).await?;
        client
            .query_one(
                "SELECT slot_name FROM pg_create_logical_replication_slot($1,'pgoutput')",
                &[&id],
            )
            .await?;
        let live = Self { client, id, sql };
        live.install().await?;
        assert!(
            live.client
                .query_one(&live.sql.verify, &[])
                .await?
                .get::<_, bool>(0)
        );
        Ok(Some(live))
    }

    async fn install(&self) -> LiveResult<()> {
        self.run_intent(TriggerIntent::Install).await?;
        Ok(())
    }

    async fn run_intent(&self, intent: TriggerIntent) -> Result<(), DdlError> {
        self.sql
            .run_async(
                intent,
                |intent, sql| {
                    let root = crate::test_root::scratch_root().display().to_string();
                    let path = Path::new(&root).join(format!("{}-{intent:?}-intent.sql", self.id));
                    let mut file =
                        std::fs::File::create(path).map_err(|e| DdlError(e.to_string()))?;
                    std::io::Write::write_all(&mut file, sql.as_bytes())
                        .map_err(|e| DdlError(e.to_string()))?;
                    file.sync_all().map_err(|e| DdlError(e.to_string()))
                },
                |sql| async move {
                    self.client
                        .batch_execute(sql)
                        .await
                        .map_err(|e| DdlError(e.to_string()))
                },
            )
            .await
    }

    async fn records(&self) -> LiveResult<Vec<DdlRecord>> {
        let rows = self.client.query("SELECT data FROM pg_logical_slot_get_binary_changes($1,NULL,NULL,'proto_version','1','publication_names',$2,'messages','true')", &[&self.id, &self.id]).await?;
        let mut records = Vec::new();
        for row in rows {
            let bytes: Vec<u8> = row.get(0);
            if bytes.first() != Some(&b'M') {
                continue;
            }
            if bytes.len() < 15 {
                return Err("truncated live pgoutput Message".into());
            }
            let end = bytes[10..]
                .iter()
                .position(|b| *b == 0)
                .ok_or("missing prefix terminator")?
                + 10;
            let length_bytes: [u8; 4] = bytes
                .get(end + 1..end + 5)
                .ok_or("missing length")?
                .try_into()?;
            let length = u32::from_be_bytes(length_bytes) as usize;
            let content = bytes.get(end + 5..).ok_or("missing content")?;
            if content.len() != length {
                return Err("invalid live content length".into());
            }
            let prefix = std::str::from_utf8(&bytes[10..end])?;
            if prefix != MESSAGE_PREFIX {
                continue;
            }
            let root = crate::test_root::scratch_root().display().to_string();
            let path = Path::new(&root).join(format!("{}-wire.jsonl", self.id));
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            file.write_all(content)?;
            file.write_all(b"\n")?;
            records.push(decode(
                &Message::LogicalMessage {
                    transactional: bytes[1] == 1,
                    lsn: u64::from_be_bytes(bytes[2..10].try_into()?),
                    prefix,
                    content,
                },
                &self.id,
            )?);
        }
        Ok(records)
    }

    async fn baseline(&self) -> LiveResult<SchemaVersions> {
        self.client
            .batch_execute(&format!(
                "ALTER TABLE {}.mapped ADD COLUMN baseline integer",
                self.id
            ))
            .await?;
        let records = self.records().await?;
        let record = records.last().ok_or("missing real baseline event")?;
        Ok(SchemaVersions::new(
            self.id.clone(),
            record.version,
            [TableDescriptor::from_command(&record.commands[0])?],
        )?)
    }

    async fn close(&self) -> LiveResult<()> {
        self.run_intent(TriggerIntent::Remove).await?;
        self.client
            .query_one("SELECT pg_drop_replication_slot($1)", &[&self.id])
            .await?;
        self.client
            .batch_execute(&format!(
                "DROP PUBLICATION {}; DROP SCHEMA {} CASCADE;",
                self.id, self.id
            ))
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn live_addressed_and_identity_less_wire_shapes() -> LiveResult<()> {
    let _guard = LIVE_LOCK.lock().await;
    let Some(live) = Live::open("live_addressed_and_identity_less_wire_shapes").await? else {
        return Ok(());
    };
    let result: LiveResult<()> = async {
        for (tag, statement) in [
            (
                "ALTER TABLE",
                format!("ALTER TABLE {}.mapped ADD COLUMN wire integer", live.id),
            ),
            (
                "GRANT",
                format!("GRANT SELECT ON {}.mapped TO PUBLIC", live.id),
            ),
            (
                "REVOKE",
                format!("REVOKE SELECT ON {}.mapped FROM PUBLIC", live.id),
            ),
            (
                "ALTER DEFAULT PRIVILEGES",
                format!(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO PUBLIC",
                    live.id
                ),
            ),
        ] {
            live.client.batch_execute(&statement).await?;
            let records = live.records().await?;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].tag, tag);
            assert_eq!(records[0].commands.len(), 1);
            let command = &records[0].commands[0];
            assert_eq!(command.command_tag, tag);
            if tag == "ALTER TABLE" {
                assert_eq!(command.classid, Some(1259));
                assert!(command.objid.is_some_and(|oid| oid > 0));
                assert_eq!(command.objsubid, Some(0));
                assert!(command.object_identity.is_some());
            } else {
                assert_eq!(command.classid, None);
                assert_eq!(command.objid, None);
                assert_eq!(command.objsubid, None);
                assert_eq!(command.schema_name, None);
                assert_eq!(command.object_identity, None);
            }
        }
        Ok(())
    }
    .await;
    let cleanup = live.close().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supported_ddl_follows_boundary() -> LiveResult<()> {
    let _guard = LIVE_LOCK.lock().await;
    let Some(live) = Live::open("supported_ddl_follows_boundary").await? else {
        return Ok(());
    };
    let result: LiveResult<()> = async {
        let mut tracker = live.baseline().await?;
        live.client
            .batch_execute(&format!(
                "ALTER TABLE {}.mapped ADD COLUMN title text DEFAULT 'hello'",
                live.id
            ))
            .await?;
        let records = live.records().await?;
        assert_eq!(records.len(), 1);
        assert!(matches!(
            tracker.classify_transaction(&records),
            Classification::Supported { .. }
        ));
        let oid = records[0].commands[0].objid.unwrap();
        assert_eq!(
            tracker
                .table(oid)
                .unwrap()
                .descriptor
                .columns
                .last()
                .unwrap()
                .name,
            "title"
        );
        Ok(())
    }
    .await;
    let cleanup = live.close().await;
    result.and(cleanup)
}

#[tokio::test]
async fn unsupported_ddl_blocks_at_boundary() -> LiveResult<()> {
    let _guard = LIVE_LOCK.lock().await;
    let Some(live) = Live::open("unsupported_ddl_blocks_at_boundary").await? else {
        return Ok(());
    };
    let result: LiveResult<()> = async {
        let mut tracker = live.baseline().await?;
        live.client.batch_execute(&format!("BEGIN; INSERT INTO {}.mapped VALUES(1,2); ALTER TABLE {}.mapped RENAME COLUMN id TO renamed; COMMIT;", live.id, live.id)).await?;
        let records = live.records().await?;
        assert_eq!(records.len(), 1);
        let Classification::SafeBoundaryStop(stop) = tracker.classify_transaction(&records) else { return Err("rename was incorrectly supported".into()); };
        assert!(stop.stop_apply_before_transaction && stop.capture_continues && stop.readiness_blocked);
        assert_eq!(stop.xid, records[0].xid);
        Ok(())
    }.await;
    let cleanup = live.close().await;
    result.and(cleanup)
}

#[tokio::test]
async fn rolled_back_ddl_emits_nothing() -> LiveResult<()> {
    let _guard = LIVE_LOCK.lock().await;
    let Some(live) = Live::open("rolled_back_ddl_emits_nothing").await? else {
        return Ok(());
    };
    let result: LiveResult<()> = async {
        let tracker = live.baseline().await?;
        live.client
            .batch_execute(&format!(
                "BEGIN; ALTER TABLE {}.mapped ADD COLUMN aborted text; ROLLBACK;",
                live.id
            ))
            .await?;
        assert!(live.records().await?.is_empty());
        let version: i64 = live
            .client
            .query_one(
                &format!(
                    "SELECT version FROM _skedaddle.skedaddle_ddl_{}_version",
                    live.id
                ),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(version as u64, tracker.last_version());
        Ok(())
    }
    .await;
    let cleanup = live.close().await;
    result.and(cleanup)
}

#[tokio::test]
async fn no_event_trigger_profile_blocks_on_drift() -> LiveResult<()> {
    let _guard = LIVE_LOCK.lock().await;
    let Some(live) = Live::open("no_event_trigger_profile_blocks_on_drift").await? else {
        return Ok(());
    };
    let result: LiveResult<()> = async {
        live.client
            .batch_execute(&format!(
                "ALTER EVENT TRIGGER skedaddle_ddl_{} DISABLE",
                live.id
            ))
            .await?;
        assert!(
            !live
                .client
                .query_one(&live.sql.verify, &[])
                .await?
                .get::<_, bool>(0)
        );
        let fingerprint: String = live.client.query_one(FINGERPRINT_SQL, &[]).await?.get(0);
        let mut profile = FingerprintProfile::new(fingerprint.clone())?;
        profile
            .poll(PollPoint::Watermark(1), fingerprint)
            .map_err(|_| "unexpected initial drift")?;
        live.client
            .batch_execute(&format!(
                "ALTER TABLE {}.mapped ADD COLUMN drift text",
                live.id
            ))
            .await?;
        let changed: String = live.client.query_one(FINGERPRINT_SQL, &[]).await?.get(0);
        assert!(profile.poll(PollPoint::FinalMarker(2), changed).is_err());
        assert!(live.records().await?.is_empty());
        Ok(())
    }
    .await;
    let cleanup = live.close().await;
    result.and(cleanup)
}

// The harness CA is explicit; missing trust is a failure once a DB gate is set.
fn tls_config(postgres: tokio_postgres::Config) -> skedaddle::pg::tls::Config {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("TLS harness config required");
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read TLS harness config"))
            .expect("TLS harness JSON");
    let ca_pem = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .expect("harness ca_pem or replication.ca_pem required");
    skedaddle::pg::tls::Config::new(postgres, ca_pem)
}

async fn tls_connect(
    dsn: &str,
) -> std::result::Result<
    (tokio_postgres::Client, skedaddle::pg::tls::Driver),
    skedaddle::pg::tls::Error,
> {
    let config = tls_config(dsn.parse().expect("harness PostgreSQL configuration"));
    let (client, driver, transport) =
        skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required).await?;
    assert!(transport.encrypted());
    assert!(transport.warnings.is_empty());
    Ok((client, driver))
}

#[tokio::test]
async fn scoped_operational_drift_is_sticky() -> LiveResult<()> {
    let _guard = LIVE_LOCK.lock().await;
    let Some(live) = Live::open("scoped_operational_drift_is_sticky").await? else {
        return Ok(());
    };
    let result: LiveResult<()> = async {
        let scope = vec![skedaddle::pg::chunk::QualifiedName {
            schema: live.id.clone(),
            name: "mapped".into(),
        }];
        let before = observe(&live.client, &scope).await?;
        assert!(before.hazards.is_empty());
        let mut state = OperationalSchema {
            baseline: Some(before.fingerprint),
            blocker: None,
        };
        live.client
            .batch_execute(&format!(
                "ALTER TABLE {}.mapped ADD COLUMN drift text",
                live.id
            ))
            .await?;
        assert!(
            state
                .poll(
                    PollPoint::Watermark(1),
                    observe(&live.client, &scope).await?
                )
                .is_err()
        );
        live.client
            .batch_execute(&format!("ALTER TABLE {}.mapped DROP COLUMN drift", live.id))
            .await?;
        let mut restarted: OperationalSchema =
            serde_json::from_str(&serde_json::to_string(&state)?)?;
        assert!(
            restarted
                .poll(
                    PollPoint::FinalMarker(2),
                    observe(&live.client, &scope).await?
                )
                .is_err()
        );
        assert_eq!(state, restarted);
        Ok(())
    }
    .await;
    let cleanup = live.close().await;
    result.and(cleanup)
}

#[tokio::test]
async fn invalid_concurrent_index_blocks_source_preflight() -> LiveResult<()> {
    let _guard = LIVE_LOCK.lock().await;
    let Some(mut live) = Live::open("invalid_concurrent_index_blocks_source_preflight").await?
    else {
        return Ok(());
    };
    let result: LiveResult<()> = async {
        live.client
            .batch_execute(&format!("INSERT INTO {}.mapped VALUES (1),(1)", live.id))
            .await?;
        assert!(
            live.client
                .batch_execute(&format!(
                    "CREATE UNIQUE INDEX CONCURRENTLY broken ON {}.mapped(id)",
                    live.id
                ))
                .await
                .is_err()
        );
        let scope = vec![skedaddle::pg::chunk::QualifiedName {
            schema: live.id.clone(),
            name: "mapped".into(),
        }];
        assert!(
            observe(&live.client, &scope)
                .await?
                .hazards
                .iter()
                .any(|h| h.kind == "invalid-or-unready-index")
        );
        use skedaddle::{
            definition::{ObjectKind, ObjectSelector},
            pg::preflight,
        };
        let objects = vec![ObjectSelector {
            schema: live.id.clone(),
            name: "mapped".into(),
            kind: ObjectKind::Table,
            exclude_columns: vec![],
        }];
        let snapshot = preflight::collect(&mut live.client, &objects).await?;
        let evidence = snapshot
            .observations
            .iter()
            .find(|o| o.check == preflight::Check::SplitTransactionDdl)
            .unwrap();
        assert!(evidence.error.is_none());
        assert!(!evidence.rows.is_empty());
        let report =
            preflight::classify("test", &snapshot, &preflight::Options::default(), |_| true);
        assert!(!report.blocking().is_empty());
        Ok(())
    }
    .await;
    let cleanup = live.close().await;
    result.and(cleanup)
}
