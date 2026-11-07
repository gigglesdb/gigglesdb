use super::*;
use crate::storage::replica::{Command as ReplicaCommand, Image};

fn command() -> Command {
    Command {
        identity: Identity {
            application: 1,
            authority_generation: 2,
            operation: 3,
        },
        body: Body {
            contract_version: CONTRACT_VERSION,
            domain: 4,
            catalog_epoch: 5,
            table: 6,
            operation: Operation::Read {
                key: vec![Value::Int(7)],
            },
        },
    }
}
fn authority() -> Authority {
    Authority {
        application: 1,
        generation: 2,
        domain: 4,
        catalog_epoch: 5,
        read_tables: BTreeSet::from([6]),
        write_tables: BTreeSet::new(),
    }
}

#[test]
fn application_admission_fences_and_denied_table() {
    let command = command();
    assert!(command.admit(&authority()).is_ok());
    let mut denied = authority();
    denied.read_tables.clear();
    assert!(command.admit(&denied).is_err());
    for field in 0..4 {
        let mut stale = authority();
        match field {
            0 => stale.application += 1,
            1 => stale.generation += 1,
            2 => stale.domain += 1,
            _ => stale.catalog_epoch += 1,
        }
        assert!(command.admit(&stale).is_err());
    }
    let mut write = command.clone();
    write.body.operation = Operation::GuardedWrite {
        key: vec![Value::Int(7)],
        expected_version: 1,
        assignments: vec![(8, Value::Int(9))],
    };
    assert!(write.admit(&authority()).is_err());
    let mut permitted = authority();
    permitted.write_tables.insert(6);
    assert!(write.admit(&permitted).is_ok());
}

#[test]
fn application_identity_compares_full_body() {
    let command = command();
    assert!(command.same_submission(&command.clone()));
    let mut changed = command.clone();
    changed.body.operation = Operation::Read {
        key: vec![Value::Int(8)],
    };
    assert!(!command.same_submission(&changed));
    changed = command.clone();
    changed.identity.operation += 1;
    assert!(!command.same_submission(&changed));
}

#[test]
fn application_bounds_and_versions_fail_closed() {
    for version in [0, CONTRACT_VERSION + 1] {
        let mut command = command();
        command.body.contract_version = version;
        assert!(matches!(command.validate(), Err(CoreError::Unsupported)));
    }
    for key in [
        vec![],
        vec![Value::Null; MAX_KEY_VALUES + 1],
        vec![Value::Bytes(vec![0; MAX_VALUE_BYTES + 1])],
    ] {
        let mut command = command();
        command.body.operation = Operation::Read { key };
        assert!(matches!(command.validate(), Err(CoreError::Limit)));
    }
    let mut command = command();
    command.body.operation = Operation::GuardedWrite {
        key: vec![Value::Int(7)],
        expected_version: 0,
        assignments: vec![(8, Value::Int(9))],
    };
    assert!(command.validate().is_err());
}

#[test]
fn application_dispatch_without_provisioning_denies_and_preserves_image() {
    let image = Image::new(4).unwrap();
    let before = image.bytes().to_vec();
    let command = ReplicaCommand::Application(command());
    assert!(command.validate().is_ok());
    for _ in 0..2 {
        let (next, _) = image.evaluate(&command).unwrap();
        assert_eq!(next.bytes(), before);
        assert_eq!(image.bytes(), before);
    }
    // Even malformed Image bytes cannot reach Admin construction on this path.
    let malformed = Image(vec![]);
    assert!(malformed.evaluate(&command).is_err());
}
