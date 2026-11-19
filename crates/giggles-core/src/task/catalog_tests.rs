use super::*;
use crate::{
    roles::CatalogRole,
    task::definition::{TaskOnlyViolation, check_task_only},
};

const ID: u64 = 1001;

/// The worked example of design section 5 ("inc-436"), as stored.
pub(crate) fn inc_436() -> TaskDef {
    let readable = ["id", "status", "failed_at", "attempt_count"];
    let mut columns: BTreeMap<String, ColumnMode> =
        readable.iter().map(|c| (c.to_string(), ColumnMode::Readable)).collect();
    columns.insert("customer_email".into(), ColumnMode::Masked { constant: None });
    TaskDef {
        id: TaskObjectId(ID),
        name: TaskName::new("inc-436").unwrap(),
        def_version: 1,
        issuer: "incident_controller".into(),
        executor: "repair_agent".into(),
        approver: Some("oncall_lead".into()),
        table: 16,
        visible_rows: PredicateText("merchant_id = 42".into()),
        columns,
        untrusted_allowed: BTreeSet::new(),
        allowed_write: Some(AllowedWrite {
            columns: BTreeSet::from(["status".to_string()]),
            old_row: PredicateText(
                "status = 'failed' and failed_at >= '2026-11-05 17:00:00+00' and failed_at < '2026-11-05 17:07:00+00'"
                    .into(),
            ),
            new_row: PredicateText("status = 'queued'".into()),
        }),
        mutation_caps: MutationCaps { distinct_rows: 25, transitions: 25 },
        disclosure_caps: DisclosureCaps {
            rows_read: 1000,
            rows_returned: 1000,
            bytes_returned: 2_097_152,
        },
        limits: LoadLimits {
            statement_ms: 2000,
            rows_scanned: 100_000,
            concurrent_statements: 2,
            ..LoadLimits::default()
        },
        approval_threshold: 5,
        // 2026-11-05 17:30:00+00
        expires_at: WallClockMs(1_793_899_800_000),
        revoked: false,
        grants: BTreeMap::from([("auditor".to_string(), TaskPrivileges::INSPECT)]),
    }
}

/// Independently produced (Python struct.pack) from the section 2 layout.
const GOLDEN: [&str; 9] = [
    "0100000000000000010007696e632d3433360013696e636964656e745f636f6e74726f6c6c6572000c7265706169725f",
    "6167656e7401000b6f6e63616c6c5f6c6561640000000000000010000000106d65726368616e745f6964203d20343200",
    "05000d617474656d70745f636f756e7400000e637573746f6d65725f656d61696c0100096661696c65645f6174000002",
    "6964000006737461747573000000010001000673746174757300000064737461747573203d20276661696c6564272061",
    "6e64206661696c65645f6174203e3d2027323032362d31312d30352031373a30303a30302b30302720616e6420666169",
    "6c65645f6174203c2027323032362d31312d30352031373a30373a30302b30302700000011737461747573203d202771",
    "7565756564270000000000000019000000000000001900000000000003e800000000000003e800000000002000000000",
    "0000000007d000000000000186a00000000200000000000003e80000000000100000000000080000000000000e100000",
    "000000000005000001a1acc2a5c0000001000761756469746f7204",
];

fn golden() -> Vec<u8> {
    let hex: String = GOLDEN.concat();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

fn key() -> Vec<u8> {
    task_key(ID)
}

fn is_corrupt(result: Result<TaskDef, CoreError>) -> bool {
    matches!(result, Err(CoreError::Corrupt(_)))
}

/// Byte offset of the first occurrence of `needle` in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> usize {
    hay.windows(needle.len()).position(|w| w == needle).unwrap()
}

#[test]
fn roundtrip_inc_436() {
    let def = inc_436();
    let bytes = encode_task(&def).unwrap();
    assert_eq!(decode_task(&key(), &bytes).unwrap(), def);
    assert_eq!(task_key(ID), [19, 0, 0, 0, 0, 0, 0, 3, 0xe9]);
    let mut read_only = def.clone();
    read_only.allowed_write = None;
    read_only.mutation_caps = MutationCaps::default();
    read_only.approver = None;
    read_only.revoked = true;
    read_only.def_version = 2;
    read_only.grants.clear();
    read_only.untrusted_allowed.insert("status".into());
    read_only.columns.insert("id".into(), ColumnMode::Masked { constant: Some(Value::Int(-7)) });
    read_only.columns.insert("status".into(), ColumnMode::Masked { constant: Some(Value::Text(String::new())) });
    read_only.columns.insert("failed_at".into(), ColumnMode::Masked { constant: Some(Value::Bool(true)) });
    let bytes = encode_task(&read_only).unwrap();
    assert_eq!(decode_task(&key(), &bytes).unwrap(), read_only);
}

#[test]
fn golden_bytes_inc_436() {
    let bytes = encode_task(&inc_436()).unwrap();
    assert_eq!(bytes, golden());
    assert_eq!(decode_task(&key(), &golden()).unwrap(), inc_436());
}

#[test]
fn rejects_version_2() {
    let mut bytes = golden();
    bytes[0] = 2;
    assert!(is_corrupt(decode_task(&key(), &bytes)));
    bytes[0] = 0;
    assert!(is_corrupt(decode_task(&key(), &bytes)));
    assert!(is_corrupt(decode_task(&key(), &[])));
}

#[test]
fn rejects_trailing_byte() {
    let mut bytes = golden();
    bytes.push(0);
    assert!(is_corrupt(decode_task(&key(), &bytes)));
    let bytes = golden();
    assert!(is_corrupt(decode_task(&key(), &bytes[..bytes.len() - 1])));
    // Keys: wrong kind, zero id, wrong width.
    for bad in [vec![18, 0, 0, 0, 0, 0, 0, 3, 0xe9], task_key(0), vec![19, 0, 0, 3, 0xe9]] {
        assert!(is_corrupt(decode_task(&bad, &golden())));
    }
}

#[test]
fn rejects_unsorted_columns() {
    // Swap the first two column names (same length is not available, so
    // re-encode by hand): "attempt_count" < "customer_email" -> reversed.
    let bytes = golden();
    let a = find(&bytes, b"\x00\x0dattempt_count\x00");
    let c = find(&bytes, b"\x00\x0ecustomer_email\x01");
    let first = bytes[a..c].to_vec();
    let second = bytes[c..c + 2 + 14 + 1].to_vec();
    let mut swapped = bytes[..a].to_vec();
    swapped.extend(&second);
    swapped.extend(&first);
    swapped.extend(&bytes[c + second.len()..]);
    assert_eq!(swapped.len(), bytes.len());
    assert!(is_corrupt(decode_task(&key(), &swapped)));
    // A duplicate column: columns "ia" and "id", then rename "ia" to "id".
    let mut def = inc_436();
    def.columns.insert("ia".into(), ColumnMode::Readable);
    let mut duplicate = encode_task(&def).unwrap();
    let ia = find(&duplicate, b"\x00\x02ia\x00");
    duplicate[ia + 3] = b'd';
    assert_eq!(find(&duplicate, b"\x00\x02id\x00"), ia);
    assert!(is_corrupt(decode_task(&key(), &duplicate)));
    // Unsorted grants and untrusted names fail the same way.
    let mut def = inc_436();
    def.grants.insert("zed".into(), TaskPrivileges::UNDO);
    let bytes = encode_task(&def).unwrap();
    let mut unsorted = bytes.clone();
    let z = find(&unsorted, b"\x00\x03zed");
    unsorted[z + 2..z + 5].copy_from_slice(b"aaa");
    assert!(is_corrupt(decode_task(&key(), &unsorted)));
}

#[test]
fn rejects_unknown_mode_and_privilege_bits() {
    let bytes = golden();
    let mode = find(&bytes, b"customer_email\x01") + 14;
    for bad in [4u8, 255] {
        let mut b = bytes.clone();
        b[mode] = bad;
        assert!(is_corrupt(decode_task(&key(), &b)));
    }
    let mut def = inc_436();
    def.columns.insert("customer_email".into(), ColumnMode::Masked { constant: Some(Value::Int(1)) });
    let masked = encode_task(&def).unwrap();
    let tag = find(&masked, b"customer_email\x02") + 15;
    for bad in [0u8, 4] {
        let mut b = masked.clone();
        b[tag] = bad;
        assert!(is_corrupt(decode_task(&key(), &b)));
    }
    let last = bytes.len() - 1;
    for bad in [0u8, 8, 0x84] {
        let mut b = bytes.clone();
        b[last] = bad;
        assert!(is_corrupt(decode_task(&key(), &b)));
    }
    for good in 1u8..=7 {
        let mut b = bytes.clone();
        b[last] = good;
        assert_eq!(decode_task(&key(), &b).unwrap().grants["auditor"].bits(), good);
    }
    // Other caps: a zero disclosure cap or limit, mutation caps on a read-only
    // task, and an unsupported masked constant type are all refused.
    let mut zero = inc_436();
    zero.disclosure_caps.rows_read = 0;
    assert!(matches!(encode_task(&zero), Err(CoreError::Corrupt(_))));
    let mut zero = inc_436();
    zero.limits.stored_previews = 0;
    assert!(matches!(encode_task(&zero), Err(CoreError::Corrupt(_))));
    let mut read_only = inc_436();
    read_only.allowed_write = None;
    assert!(matches!(encode_task(&read_only), Err(CoreError::Corrupt(_))));
    let mut float = inc_436();
    float.columns.insert("id".into(), ColumnMode::Masked { constant: Some(Value::Null) });
    assert!(matches!(encode_task(&float), Err(CoreError::Corrupt(_))));
    let mut zero_version = inc_436();
    zero_version.def_version = 0;
    assert!(matches!(encode_task(&zero_version), Err(CoreError::Corrupt(_))));
}

#[test]
fn hidden_mode_decodes_to_absent() {
    let mut def = inc_436();
    def.columns.insert("internal_note".into(), ColumnMode::Hidden);
    // The encoder never writes mode 3: Hidden is the unlisted default.
    let bytes = encode_task(&def).unwrap();
    assert_eq!(bytes, golden());
    // A stored mode 3 is accepted and maps to an absent entry.
    let mut b = golden();
    let mode = find(&b, b"customer_email\x01") + 14;
    b[mode] = 3;
    let decoded = decode_task(&key(), &b).unwrap();
    assert!(!decoded.columns.contains_key("customer_email"));
    assert_eq!(decoded.columns.len(), 4);
}

fn role(name: &str, superuser: bool) -> CatalogRole {
    CatalogRole {
        inherit: true,
        name: name.into(),
        oid: 16384,
        superuser,
        login: true,
        createrole: false,
        password: None,
        generation: 1,
        task_only: false,
    }
}

#[test]
fn privileges_of_executor_is_empty_even_if_granted() {
    let mut def = inc_436();
    def.grants.insert("repair_agent".into(), TaskPrivileges::ALL);
    let bytes = encode_task(&def).unwrap();
    let def = decode_task(&key(), &bytes).unwrap();
    assert!(def.privileges_of(&role("repair_agent", true)).is_empty());
    let undo_inspect = TaskPrivileges::UNDO | TaskPrivileges::INSPECT;
    assert_eq!(def.privileges_of(&role("oncall_lead", false)), TaskPrivileges::ALL);
    assert_eq!(def.privileges_of(&role("incident_controller", false)), undo_inspect);
    assert_eq!(def.privileges_of(&role("root", true)), TaskPrivileges::INSPECT);
    assert_eq!(def.privileges_of(&role("auditor", false)), TaskPrivileges::INSPECT);
    assert_eq!(def.privileges_of(&role("stranger", false)), TaskPrivileges::NONE);
    let mut granted = def.clone();
    granted.grants.insert("incident_controller".into(), TaskPrivileges::APPROVE);
    assert_eq!(
        granted.privileges_of(&role("incident_controller", false)),
        TaskPrivileges::ALL
    );
}

#[test]
fn task_name_rules() {
    for good in ["inc-436", "a", "A.b:c_d-9", &"x".repeat(63)] {
        assert_eq!(TaskName::new(good).unwrap().as_str(), good);
    }
    for bad in ["", "a b", "é", "a\0", "a;", &"x".repeat(64)] {
        assert!(TaskName::new(bad).is_err());
    }
    assert!(check_task_only(&role("r", false), false).is_ok());
    assert_eq!(
        check_task_only(&role("r", false), true),
        Err(TaskOnlyViolation::OrdinaryRoleInTask)
    );
}
