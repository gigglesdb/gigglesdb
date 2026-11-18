use super::{credential::*, definition::WallClockMs};
use crate::CoreError;

fn record() -> CredentialRecord {
    CredentialRecord {
        issued_at: WallClockMs(1),
        expires_at: Some(WallClockMs(2)),
        issued_by: "a".into(),
        verifier: [0x55; 32],
    }
}
fn rejects(value: &[u8]) {
    assert!(matches!(
        decode_credential(value),
        Err(CoreError::Corrupt(_))
    ));
}
#[test]
fn credential_record_golden() {
    let mut golden = vec![
        1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, b'a',
    ];
    golden.extend([0x55; 32]);
    assert_eq!(encode_credential(&record()).unwrap(), golden);
    assert_eq!(decode_credential(&golden).unwrap(), record());
    assert_eq!(
        credential_key(1, 2),
        vec![24, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]
    );
    assert_eq!(
        decode_credential_key(&credential_key(1, 2)).unwrap(),
        (1, 2)
    );
    let mut no_expiry = record();
    no_expiry.expires_at = None;
    let encoded = encode_credential(&no_expiry).unwrap();
    assert_eq!(&encoded[9..17], &i64::MAX.to_be_bytes());
    assert_eq!(decode_credential(&encoded).unwrap(), no_expiry);
    no_expiry.issued_by = "?".repeat(31) + "x";
    assert_eq!(
        decode_credential(&encode_credential(&no_expiry).unwrap()).unwrap(),
        no_expiry
    );
}
#[test]
fn decode_rejects_bad_version() {
    let mut value = encode_credential(&record()).unwrap();
    value[0] = 2;
    rejects(&value);
}
#[test]
fn decode_rejects_short() {
    let value = encode_credential(&record()).unwrap();
    for len in 0..value.len() {
        rejects(&value[..len]);
    }
}
#[test]
fn decode_rejects_long() {
    let mut value = encode_credential(&record()).unwrap();
    value.push(0);
    rejects(&value);
}
#[test]
fn decode_rejects_zero_ids() {
    for key in [
        credential_key(0, 1),
        credential_key(1, 0),
        vec![],
        vec![24; 16],
        vec![23; 17],
        // Kind 22 is a user collation, even at the credential key length.
        vec![22; 17],
    ] {
        assert!(matches!(
            decode_credential_key(&key),
            Err(CoreError::Corrupt(_))
        ));
    }
}
#[test]
fn decode_rejects_expiry_before_issue() {
    for expires in [0i64, 1] {
        let mut value = encode_credential(&record()).unwrap();
        value[9..17].copy_from_slice(&expires.to_be_bytes());
        rejects(&value);
    }
    let mut invalid = record();
    invalid.expires_at = Some(invalid.issued_at);
    assert!(encode_credential(&invalid).is_err());
}
#[test]
fn decode_rejects_bad_str() {
    let mut value = encode_credential(&record()).unwrap();
    value[19] = 0;
    rejects(&value);
    value[19] = 0xff;
    rejects(&value);
    for len in [0u16, 64, u16::MAX] {
        value[17..19].copy_from_slice(&len.to_be_bytes());
        rejects(&value);
    }
    for issuer in [String::new(), "x".repeat(64)] {
        let mut invalid = record();
        invalid.issued_by = issuer;
        assert!(encode_credential(&invalid).is_err());
    }
}
#[test]
fn secret_format_and_entropy() {
    let (a, hash) = new_secret(0x1234).unwrap();
    let (b, _) = new_secret(0x1234).unwrap();
    assert_eq!(a.expose().len(), 87);
    assert!(a.expose().starts_with("ggtc1_0000000000001234_"));
    assert!(a.expose() != b.expose());
    let (id, random) = parse_secret(a.expose()).unwrap();
    assert_eq!(id, 0x1234);
    assert!(ct_eq(&hash, &verifier(id, &random)));
    assert!(new_secret(0).is_err());
}
#[test]
fn parse_rejects_uppercase_and_garbage() {
    let valid = format!("ggtc1_000000000000000a_{}", "ab".repeat(32));
    assert!(parse_secret(&valid).is_some());
    for bad in [
        valid.to_uppercase(),
        valid.replace('a', "A"),
        valid.replace('b', "g"),
        valid[..86].into(),
        format!("{valid}0"),
        valid.replace("000000000000000a", "0000000000000000"),
        valid.replace('_', "-"),
        "?".repeat(44),
    ] {
        assert!(parse_secret(&bad).is_none());
    }
}
#[test]
fn verifier_matches_only_its_secret() {
    let random = [0x33; 32];
    let hash = verifier(5, &random);
    // Independent SHA-256 golden for domain separator, big-endian id and random bytes.
    assert_eq!(
        hash,
        [
            6, 58, 110, 92, 160, 176, 68, 34, 67, 149, 212, 239, 50, 222, 82, 116, 126, 69, 188,
            81, 216, 247, 5, 238, 241, 140, 99, 223, 188, 159, 237, 150
        ]
    );
    assert!(ct_eq(&hash, &verifier(5, &random)));
    assert!(!ct_eq(&hash, &verifier(6, &random)));
    for i in 0..32 {
        let mut changed = random;
        changed[i] ^= 1;
        assert!(!ct_eq(&hash, &verifier(5, &changed)));
        let mut different = hash;
        different[i] ^= 1;
        assert!(!ct_eq(&hash, &different));
    }
}
#[test]
fn debug_redacts_secret() {
    let (secret, _) = new_secret(1).unwrap();
    assert_eq!(format!("{secret:?}"), "SecretText([redacted])");
}

#[cfg(feature = "engine-rocksdb")]
mod store {
    use super::*;
    use crate::{
        Admin, Engine, Limits,
        storage::{
            engine::{
                kv::{Direction, Keyspace},
                txn::Isolation,
            },
            roles::{CatalogRole, NewRole, TablePrivilege},
        },
        task::{
            catalog::{TaskCatalog, TaskDdlError},
            definition::*,
        },
    };
    use std::{ops::Bound, path::PathBuf};

    const NOW: WallClockMs = WallClockMs(1_793_898_000_000);
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct Fixture {
        admin: Admin,
        dir: Directory,
        issuer: CatalogRole,
        other: CatalogRole,
        task: TaskObjectId,
    }
    fn new_task(name: &str) -> NewTask {
        NewTask {
            name: name.into(),
            executor: "executor".into(),
            approver: None,
            schema: None,
            table: "items".into(),
            visible_rows: PredicateText("id > 0".into()),
            readable: vec!["id".into()],
            masked: vec![],
            untrusted_text: vec![],
            allowed_write: None,
            mutation_caps: MutationCaps {
                distinct_rows: 0,
                transitions: 0,
            },
            disclosure_caps: DisclosureCaps {
                rows_read: 10,
                rows_returned: 10,
                bytes_returned: 1024,
            },
            limits: LoadLimits::default(),
            approval_threshold: 0,
            expires: "2026-11-05 17:30:00+00".into(),
        }
    }
    fn fixture() -> Fixture {
        let mut random = [0; 16];
        getrandom::fill(&mut random).unwrap();
        let dir = Directory(
            std::env::temp_dir().join(format!("credential-{:032x}", u128::from_be_bytes(random))),
        );
        let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
        admin
            .sql_session()
            .execute("CREATE TABLE items (id bigint primary key)", &[], None)
            .unwrap();
        let roles = admin.roles();
        for name in ["issuer", "other", "executor"] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    ..NewRole::default()
                })
                .unwrap();
        }
        roles.set_task_only("executor", true).unwrap();
        roles
            .grant_table("items", "issuer", &[TablePrivilege::Select])
            .unwrap();
        let issuer = roles.get("issuer").unwrap().unwrap();
        let other = roles.get("other").unwrap().unwrap();
        let task = admin
            .tasks()
            .create(&issuer, new_task("one"), NOW)
            .unwrap()
            .id;
        Fixture {
            admin,
            dir,
            issuer,
            other,
            task,
        }
    }
    fn issue(f: &Fixture, expires: Option<WallClockMs>) -> IssuedCredential {
        f.admin
            .tasks()
            .issue_credential(&f.issuer, "one", expires, NOW)
            .unwrap()
    }
    fn state<T: std::fmt::Debug>(result: Result<T, TaskDdlError>, code: &str) {
        assert_eq!(result.unwrap_err().sqlstate, code);
    }
    fn raw_records(tasks: &TaskCatalog) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut txn = tasks.db().unwrap().begin(Isolation::ReadCommitted);
        txn.scan(
            Keyspace::Catalog,
            Bound::Included(&[CREDENTIAL_KIND]),
            Bound::Excluded(&[CREDENTIAL_KIND + 1]),
            Direction::Forward,
            None,
        )
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }
    #[test]
    fn issue_returns_secret_once_and_stores_only_verifier() {
        let f = fixture();
        let issued = issue(&f, None);
        let tasks = f.admin.tasks();
        let secret = issued.secret.expose();
        let records = raw_records(&tasks);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, credential_key(f.task.0, issued.id));
        let value = &records[0].1;
        assert!(!value.windows(secret.len()).any(|w| w == secret.as_bytes()));
        assert!(!value.windows(64).any(|w| w == &secret.as_bytes()[23..]));
        let (id, random) = parse_secret(secret).unwrap();
        let record = decode_credential(value).unwrap();
        assert_eq!(record.verifier, verifier(id, &random));
        assert_eq!(record.issued_at, NOW);
        assert_eq!(record.issued_by, "issuer");
        assert_eq!(issued.task.as_str(), "one");
        assert_eq!(issued.expires_at, None);
        let debug = format!("{issued:?}");
        assert!(!debug.contains(secret));
        assert!(!debug.contains(&secret[23..]));
        let matched = tasks.authenticate(secret, NOW).unwrap();
        assert_eq!(matched.task, f.task);
        assert_eq!(matched.executor, "executor");
        assert_eq!(matched.credential, issued.id);
        assert_eq!(matched.expires_at, None);
    }
    #[test]
    fn issue_refuses_non_issuer() {
        let f = fixture();
        state(
            f.admin.tasks().issue_credential(&f.other, "one", None, NOW),
            "42501",
        );
        state(
            f.admin
                .tasks()
                .issue_credential(&f.other, "missing", None, NOW),
            "42704",
        );
        assert!(raw_records(&f.admin.tasks()).is_empty());
    }
    #[test]
    fn issue_refuses_revoked_and_expired_task() {
        let f = fixture();
        let tasks = f.admin.tasks();
        let expires = tasks.get("one").unwrap().unwrap().expires_at;
        state(
            tasks.issue_credential(&f.issuer, "one", None, expires),
            "55000",
        );
        tasks.revoke(&f.issuer, "one").unwrap();
        state(tasks.issue_credential(&f.issuer, "one", None, NOW), "55000");
    }
    #[test]
    fn issue_refuses_past_expires() {
        let f = fixture();
        for expires in [NOW, WallClockMs(NOW.0 - 1)] {
            state(
                f.admin
                    .tasks()
                    .issue_credential(&f.issuer, "one", Some(expires), NOW),
                "22023",
            );
        }
    }
    #[test]
    fn live_cap_is_8() {
        let f = fixture();
        let expires = WallClockMs(NOW.0 + 1);
        let first = issue(&f, Some(expires));
        let mut ids = vec![first.id];
        for _ in 1..8 {
            ids.push(issue(&f, None).id);
        }
        let tasks = f.admin.tasks();
        assert_eq!(
            tasks
                .credentials(f.task, NOW)
                .unwrap()
                .iter()
                .map(|x| x.0)
                .collect::<Vec<_>>(),
            ids
        );
        state(tasks.issue_credential(&f.issuer, "one", None, NOW), "54000");
        assert_eq!(tasks.credentials(f.task, expires).unwrap().len(), 7);
        let next = tasks
            .issue_credential(&f.issuer, "one", None, expires)
            .unwrap();
        assert!(next.id > *ids.last().unwrap());
        assert_eq!(tasks.credentials(f.task, expires).unwrap().len(), 8);
        assert_eq!(raw_records(&tasks).len(), 8);
        assert!(
            !tasks
                .view()
                .unwrap()
                .task_credentials
                .contains_key(&first.id)
        );
    }
    #[test]
    fn revoke_deletes_and_authenticate_fails() {
        let f = fixture();
        let issued = issue(&f, None);
        let tasks = f.admin.tasks();
        tasks.revoke_credential(&f.issuer, issued.id).unwrap();
        assert!(raw_records(&tasks).is_empty());
        assert_eq!(
            tasks.authenticate(issued.secret.expose(), NOW),
            Err(CredentialRefused)
        );
        assert!(!tasks.credential_live(f.task, issued.id, NOW));
    }
    #[test]
    fn revoke_refuses_non_issuer_and_unknown() {
        let f = fixture();
        let issued = issue(&f, None);
        let tasks = f.admin.tasks();
        state(tasks.revoke_credential(&f.other, issued.id), "42501");
        let error = tasks.revoke_credential(&f.other, u64::MAX).unwrap_err();
        assert_eq!(error.sqlstate, "42704");
        assert_eq!(
            error.message,
            format!("credential {} does not exist", u64::MAX)
        );
        assert!(tasks.authenticate(issued.secret.expose(), NOW).is_ok());
    }
    #[test]
    fn own_expiry_ends_credential() {
        let f = fixture();
        let expiry = WallClockMs(NOW.0 + 10);
        let issued = issue(&f, Some(expiry));
        let tasks = f.admin.tasks();
        assert!(tasks.credential_live(f.task, issued.id, WallClockMs(expiry.0 - 1)));
        assert_eq!(
            tasks
                .authenticate(issued.secret.expose(), NOW)
                .unwrap()
                .expires_at,
            Some(expiry)
        );
        assert_eq!(
            tasks.authenticate(issued.secret.expose(), expiry),
            Err(CredentialRefused)
        );
        assert!(!tasks.credential_live(f.task, issued.id, expiry));
        assert!(tasks.credentials(f.task, expiry).unwrap().is_empty());
    }
    #[test]
    fn revoked_task_ends_credential() {
        let f = fixture();
        let issued = issue(&f, None);
        let tasks = f.admin.tasks();
        // Task expiry alone does not prevent outcome-retention authentication.
        let expiry = tasks.get("one").unwrap().unwrap().expires_at;
        assert!(tasks.authenticate(issued.secret.expose(), expiry).is_ok());
        assert!(tasks.credential_live(f.task, issued.id, expiry));
        tasks.revoke(&f.issuer, "one").unwrap();
        assert_eq!(
            tasks.authenticate(issued.secret.expose(), NOW),
            Err(CredentialRefused)
        );
        assert!(!tasks.credential_live(f.task, issued.id, NOW));
    }
    #[test]
    fn secret_for_one_task_never_matches_another() {
        let f = fixture();
        let issued = issue(&f, None);
        let tasks = f.admin.tasks();
        let other = tasks.create(&f.issuer, new_task("two"), NOW).unwrap();
        assert!(!tasks.credential_live(other.id, issued.id, NOW));
        assert_eq!(
            tasks
                .authenticate(issued.secret.expose(), NOW)
                .unwrap()
                .task,
            f.task
        );
        let second = tasks.issue_credential(&f.issuer, "two", None, NOW).unwrap();
        assert_eq!(
            tasks
                .authenticate(second.secret.expose(), NOW)
                .unwrap()
                .task,
            other.id
        );
    }
    #[test]
    fn wrong_secret_and_unknown_id_refuse_alike() {
        let f = fixture();
        let issued = issue(&f, None);
        let tasks = f.admin.tasks();
        let (wrong, _) = new_secret(issued.id).unwrap();
        let (unknown, _) = new_secret(u64::MAX).unwrap();
        for secret in [wrong.expose(), unknown.expose(), "invalid", ""] {
            assert_eq!(tasks.authenticate(secret, NOW), Err(CredentialRefused));
        }
    }
    #[test]
    fn credential_survives_reopen() {
        let f = fixture();
        let issued = issue(&f, None);
        let expected = f
            .admin
            .tasks()
            .authenticate(issued.secret.expose(), NOW)
            .unwrap();
        drop(f.admin);
        let admin = Admin::open(&f.dir.0, Limits::default()).unwrap();
        assert_eq!(
            admin
                .tasks()
                .authenticate(issued.secret.expose(), NOW)
                .unwrap(),
            expected
        );
        assert!(admin.tasks().credential_live(f.task, issued.id, NOW));
    }
}
