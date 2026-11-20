#[path = "support/root.rs"]
mod test_root;

use std::fs;

use serde_json::json;
use skedaddle::{
    pg::{
        fence::{Catalog, Grant, Grantee, Membership, Privilege, Role, Table},
        preflight::{Check, Observation, Snapshot},
    },
    security::{
        REDACTED, Redactor, RolePurpose, SecretRef, SecurityError, TlsPolicy, classify_role,
        resolve,
    },
};

fn secret_file(value: &str) -> SecretRef {
    let root = crate::test_root::scratch_root().join(format!("security-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let path = root.join("secret.txt");
    fs::write(&path, value).unwrap();
    SecretRef::parse(&format!("file:{}", path.display())).unwrap()
}

#[test]
fn secrets_never_rendered() {
    let reference = secret_file("unique-private-value-8yZ");
    let secret = resolve(&reference).unwrap();
    let mut redactor = Redactor::default();
    redactor.register(&secret);
    for rendered in [
        format!("{secret:?}"),
        format!("{secret}"),
        serde_json::to_string(&secret).unwrap(),
        format!("{redactor:?}"),
    ] {
        assert!(!rendered.contains("unique-private-value-8yZ"));
        assert!(rendered.contains(REDACTED));
    }
    assert_eq!(secret.expose_secret(), "unique-private-value-8yZ");
}

#[test]
fn only_references_accepted_and_resolution_is_runtime() {
    for input in [
        "raw-secret",
        "postgres://a:b@host/db",
        "env:",
        "env:BAD-NAME",
        "file:relative",
        "file:",
        "vault:key",
    ] {
        assert!(SecretRef::parse(input).is_err(), "accepted {input}");
    }
    let reference = secret_file("first-value");
    assert_eq!(resolve(&reference).unwrap().expose_secret(), "first-value");
    fs::write(
        reference.as_str().strip_prefix("file:").unwrap(),
        "second-value\n",
    )
    .unwrap();
    assert_eq!(
        resolve(&reference).unwrap().expose_secret(),
        "second-value\n"
    );
    // Read an existing environment variable without mutating global environment
    // during parallel tests. Its value is never printed.
    let name = if cfg!(windows) { "SystemRoot" } else { "PATH" };
    let reference = SecretRef::parse(&format!("env:{name}")).unwrap();
    assert_eq!(
        resolve(&reference).unwrap().expose_secret(),
        std::env::var(name).unwrap()
    );
}

#[test]
fn unavailable_and_empty_secrets_have_safe_errors() {
    let reference = SecretRef::parse("env:SKEDADDLE_P12A_MISSING_9BD4C823").unwrap();
    let error = resolve(&reference).unwrap_err();
    assert_eq!(error, SecurityError::SecretUnavailable);
    assert!(!error.to_string().contains(reference.as_str()));
    assert_eq!(
        resolve(&secret_file("")).unwrap_err(),
        SecurityError::EmptySecret
    );
}

#[test]
fn sql_error_echoing_password_is_redacted() {
    let mut redactor = Redactor::default();
    redactor.register(
        &resolve(&secret_file(
            "postgresql://capture:p%40ss%27Word@localhost/db",
        ))
        .unwrap(),
    );
    for message in [
        "ERROR: invalid input syntax for integer: \"p@ss'Word\" SQLSTATE 22P02",
        "DETAIL: failing row contains ('p@ss''Word', 2)",
        r#"{"error":"invalid value p@ss'Word"}"#,
        "postgresql://capture:p%40ss%27Word@localhost/db",
    ] {
        let redacted = redactor.redact(message);
        assert!(!redacted.contains("p@ss"));
        assert!(!redacted.contains("p%40ss"));
        assert!(redacted.contains(REDACTED));
    }
    assert!(
        redactor
            .redact("SQLSTATE 22P02: p@ss'Word")
            .contains("SQLSTATE 22P02")
    );
}

#[test]
fn connection_passwords_are_redacted_without_registration() {
    let redactor = Redactor::default();
    for message in [
        "connect postgres://user:unknown-secret@host/db failed",
        "postgresql://user:p%40ss@host/db?password=querysecret&sslmode=require",
        "host=localhost user=me password='a secret with spaces' dbname=test",
        r"password='quote\' and \\ slash' host=localhost",
        "Password=abcXYZ;User Id=sa;",
        "PWD={space;and}}brace};Server=localhost;",
        r#"{"password": "json-secret", "other": 5}"#,
        "password = unquoted-secret host=localhost",
        "password='unterminated-secret",
    ] {
        let redacted = redactor.redact(message);
        assert!(redacted.contains(REDACTED), "no marker for {message}");
        for value in [
            "unknown-secret",
            "p%40ss",
            "querysecret",
            "a secret with spaces",
            "quote",
            "abcXYZ",
            "space;and",
            "json-secret",
            "unquoted-secret",
            "unterminated-secret",
        ] {
            assert!(!redacted.contains(value), "leaked {value} in {redacted}");
        }
    }
    assert_eq!(
        redactor.redact("password_policy=strict; passwordless=false"),
        "password_policy=strict; passwordless=false"
    );
}

#[test]
fn escaped_connection_password_registered_for_sql_errors() {
    let mut redactor = Redactor::default();
    redactor.register_value(r"host=x password='s3cr\'et \\ value' user=u");
    assert_eq!(
        redactor.redact(r"invalid parameter: s3cr'et \ value"),
        format!("invalid parameter: {REDACTED}")
    );
}

#[test]
fn redaction_handles_unicode_and_overlapping_values() {
    let mut redactor = Redactor::default();
    for value in ["abc", "abcdef", "defghi", "雪'密", ""] {
        redactor.register_value(value);
    }
    assert_eq!(
        redactor.redact("abcdefghi 雪''密"),
        format!("{REDACTED} {REDACTED}")
    );
    assert_eq!(
        Redactor::default().redact("é password='雪秘密' fin"),
        format!("é password='{REDACTED}' fin")
    );
}

#[test]
fn plaintext_refused_by_default() {
    assert_eq!(TlsPolicy::default(), TlsPolicy::Required);
    assert_eq!(
        TlsPolicy::from_definition(None).unwrap().check(false),
        Err(SecurityError::PlaintextRefused)
    );
    assert!(TlsPolicy::Required.check(true).unwrap().is_empty());
    for invalid in ["disable", "prefer", "allow", "", "false"] {
        assert_eq!(
            TlsPolicy::from_definition(Some(invalid)),
            Err(SecurityError::InvalidTlsPolicy)
        );
    }
    let policy = TlsPolicy::from_definition(Some("disable-for-local-test")).unwrap();
    let warnings = policy.check(false).unwrap();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].message.contains("WARNING: TLS DISABLED"));
    assert!(serde_json::from_str::<TlsPolicy>("\"disable\"").is_err());
    assert_eq!(
        serde_json::from_str::<TlsPolicy>("\"disable-for-local-test\"").unwrap(),
        policy
    );
}

fn table(name: &str) -> Table {
    Table {
        schema: "public".to_owned(),
        name: name.to_owned(),
        owner: "owner".to_owned(),
    }
}

fn catalog() -> Catalog {
    Catalog {
        roles: vec![Role {
            name: "worker".to_owned(),
            login: true,
            superuser: false,
            bypass_rls: false,
        }],
        tables: vec![table("selected")],
        ..Default::default()
    }
}

fn grant(grantee: Grantee, name: &str, grant_option: bool) -> Grant {
    Grant {
        table: table(name),
        column: None,
        grantee,
        grantor: "owner".to_owned(),
        privilege: Privilege::Update,
        grant_option,
    }
}

fn snapshot(check: Check, rows: Vec<serde_json::Value>) -> Snapshot {
    Snapshot {
        observations: vec![Observation {
            check,
            rows,
            sql: String::new(),
            object: None,
            error: None,
        }],
    }
}

#[test]
fn over_broad_role_warned() {
    let mut facts = catalog();
    facts.roles[0].superuser = true;
    facts.roles[0].bypass_rls = true;
    facts.grants.push(grant(Grantee::Public, "selected", false));
    let preflight = snapshot(
        Check::ReplicationPrivilege,
        vec![json!({"login":true,"replication":true,"superuser":true})],
    );
    let warnings = classify_role(
        "worker",
        RolePurpose::Verify,
        &preflight,
        &facts,
        &[table("selected")],
    );
    for code in [
        "role_superuser",
        "role_bypass_rls",
        "unneeded_write",
        "unneeded_replication",
    ] {
        assert_eq!(warnings.iter().filter(|w| w.code == code).count(), 1);
    }
}

#[test]
fn inherited_public_and_out_of_scope_authority_warned() {
    let mut facts = catalog();
    facts.memberships = vec![
        Membership {
            member: "worker".into(),
            role: "inherited".into(),
        },
        Membership {
            member: "inherited".into(),
            role: "worker".into(),
        },
    ];
    facts.grants = vec![
        grant(Grantee::Role("inherited".into()), "outside", true),
        grant(Grantee::Public, "selected", true),
    ];
    let warnings = classify_role(
        "worker",
        RolePurpose::Apply,
        &Snapshot::default(),
        &facts,
        &[table("selected")],
    );
    assert!(warnings.iter().any(|w| w.code == "out_of_scope_write"));
    assert!(warnings.iter().any(|w| w.code == "unneeded_grant_option"));
}

#[test]
fn privilege_classifier_distinguishes_role_purposes() {
    let mut facts = catalog();
    let preflight = snapshot(
        Check::ReplicationPrivilege,
        vec![json!({"login":true,"replication":true,"superuser":false})],
    );
    assert!(
        classify_role(
            "worker",
            RolePurpose::Capture,
            &preflight,
            &facts,
            &[table("selected")]
        )
        .is_empty()
    );
    facts
        .grants
        .push(grant(Grantee::Role("worker".into()), "selected", false));
    assert!(
        classify_role(
            "worker",
            RolePurpose::Apply,
            &Snapshot::default(),
            &facts,
            &[table("selected")]
        )
        .is_empty()
    );
    assert!(
        !classify_role(
            "worker",
            RolePurpose::Verify,
            &Snapshot::default(),
            &facts,
            &[table("selected")]
        )
        .is_empty()
    );
    facts.grants[0].grant_option = true;
    facts.memberships.push(Membership {
        member: "worker".into(),
        role: "pg_signal_backend".into(),
    });
    assert!(
        classify_role(
            "worker",
            RolePurpose::Fence,
            &Snapshot::default(),
            &facts,
            &[table("selected")]
        )
        .is_empty()
    );
    assert!(
        classify_role(
            "worker",
            RolePurpose::Apply,
            &Snapshot::default(),
            &facts,
            &[table("selected")]
        )
        .iter()
        .any(|w| w.code == "broad_predefined_role")
    );
}

#[test]
fn ownership_and_incomplete_observations_warned() {
    let mut facts = catalog();
    facts.tables[0].owner = "worker".to_owned();
    let preflight = snapshot(Check::DatabaseCreate, vec![json!({"create":true})]);
    let warnings = classify_role(
        "worker",
        RolePurpose::Capture,
        &preflight,
        &facts,
        &[table("selected")],
    );
    assert!(warnings.iter().any(|w| w.code == "table_ownership"));
    assert!(
        warnings
            .iter()
            .any(|w| w.code == "unneeded_database_create")
    );
    assert!(
        classify_role(
            "missing",
            RolePurpose::Verify,
            &Snapshot::default(),
            &facts,
            &[]
        )
        .iter()
        .any(|w| w.code == "role_facts_missing")
    );
}
