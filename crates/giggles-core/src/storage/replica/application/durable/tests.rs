use super::*;
use crate::relational::{ColumnDef, DataType, Prepared};
use crate::storage::replica::{Command as ReplicaCommand, Image};
use crate::storage::{self, Metadata, empty, from_records};

fn image(state: &State) -> Image {
    Image::from_bytes(encode(&records(state).unwrap()).unwrap()).unwrap()
}
fn state(image: &Image) -> State {
    storage::replica::replica_state(image.bytes()).unwrap()
}
fn fixture() -> (Image, Command, Authority) {
    let mut s = empty(123);
    let create = Prepared::create_engine(
        "items".into(),
        vec![
            ColumnDef {
                id: 0,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            },
            ColumnDef {
                id: 0,
                name: "n".into(),
                data_type: DataType::BigInt,
                not_null: true,
            },
        ],
        vec!["id".into()],
        123,
        &s.sql,
    )
    .unwrap();
    s.sql.execute(&create, &[], None).unwrap();
    let table = s.sql.tables["items"].clone();
    let insert =
        Prepared::insert_engine(&table, vec![Value::Int(1), Value::Int(10)], 123, &s.sql).unwrap();
    s.sql.execute(&insert, &[], None).unwrap();
    let authority = Authority {
        application: 7,
        generation: 1,
        domain: 123,
        catalog_epoch: s.sql.epoch,
        read_tables: BTreeSet::from([table.id]),
        write_tables: BTreeSet::from([table.id]),
    };
    let command = Command {
        identity: Identity {
            application: 7,
            authority_generation: 1,
            operation: 1,
        },
        body: Body {
            contract_version: CONTRACT_VERSION,
            domain: 123,
            catalog_epoch: s.sql.epoch,
            table: table.id,
            operation: Operation::Read {
                key: vec![Value::Int(1)],
            },
        },
    };
    let (image, _) = image(&s)
        .evaluate(&ReplicaCommand::ApplicationOwner(OwnerCommand::Provision(
            authority.clone(),
        )))
        .unwrap();
    (image, command, authority)
}
fn apply(image: &Image, command: &Command) -> (Image, Outcome) {
    image.evaluate_application(command).unwrap().unwrap()
}
fn write(image: &Image, read: &Command, operation: u128, version: u64, value: Value) -> Command {
    let mut command = read.clone();
    command.identity.operation = operation;
    command.body.operation = Operation::GuardedWrite {
        key: vec![Value::Int(1)],
        expected_version: version,
        assignments: vec![(state(image).sql.tables["items"].columns[1].id, value)],
    };
    command
}
fn version(outcome: &Outcome) -> u64 {
    let Outcome::Success(out) = outcome else {
        panic!("{outcome:?}")
    };
    out.rows[0].version
}
#[test]
fn application_read_and_guarded_write_reject_heap_images() {
    let (keyed, read, _) = fixture();
    let (_, outcome) = keyed.evaluate_application(&read).unwrap().unwrap();
    let keyed_write = write(&keyed, &read, 2, version(&outcome), Value::Int(11));
    assert!(matches!(
        keyed.evaluate_application(&keyed_write).unwrap().unwrap().1,
        Outcome::Success(_)
    ));
    let mut heap = empty(123);
    let create = Prepared::parse("CREATE TABLE items (n BIGINT)", 123, heap.sql.epoch).unwrap();
    heap.sql.execute(&create, &[], None).unwrap();
    heap.applications = state(&keyed).applications;
    let table = &heap.sql.tables["items"];
    assert!(table.hidden_rowid().is_some());
    let mut heap_read = read;
    heap_read.body.table = table.id;
    heap_read.body.catalog_epoch = heap.sql.epoch;
    let mut heap_write = heap_read.clone();
    heap_write.identity.operation = 2;
    heap_write.body.operation = Operation::GuardedWrite {
        key: vec![Value::Int(1)],
        expected_version: 1,
        assignments: vec![(table.columns[0].id, Value::Int(11))],
    };
    // Raw canonical bytes exercise the public entry even when the replica
    // format gate rejects heaps before application table admission.
    let heap = Image(encode(&records(&heap).unwrap()).unwrap());
    for command in [heap_read, heap_write] {
        assert_eq!(heap.evaluate_application(&command), Err(CoreError::Unsupported));
    }
}

#[test]
fn successful_read_cas_and_exact_replay_reopened_image() {
    let (original, read, _) = fixture();
    let before = original.bytes().to_vec();
    let (first, out) = apply(&original, &read);
    assert_eq!(original.bytes(), before);
    let write = write(&first, &read, 2, version(&out), Value::Int(11));
    let (next, result) = apply(&first, &write);
    assert!(matches!(&result, Outcome::Success(out) if out.changed == 1 && out.affected == 1));
    let reopened = Image::from_bytes(next.bytes().to_vec()).unwrap();
    let (retry, same) = apply(&reopened, &write);
    assert_eq!(same, result);
    assert_eq!(retry, next);
    assert_eq!(
        reopened.application_status(&write).unwrap(),
        Ok(Some(result))
    );
    // Read retry returns its retained pre-write row, while a new identity sees 11.
    assert_eq!(apply(&next, &read).1, out);
    let mut fresh = read.clone();
    fresh.identity.operation = 3;
    assert!(
        matches!(apply(&next, &fresh).1, Outcome::Success(out) if out.rows[0].values[1] == Value::Int(11))
    );
    // Closed generic dispatch also carries the same candidate atomically.
    assert_eq!(
        first
            .evaluate(&ReplicaCommand::Application(write))
            .unwrap()
            .0,
        next
    );
}
#[test]
fn stale_version_and_typed_failure_are_immutable_retained_decisions() {
    let (image, read, _) = fixture();
    let (_, read_out) = apply(&image, &read);
    for (id, expected, value, failure) in [
        (2, version(&read_out) + 1, Value::Int(11), Failure::Conflict),
        (3, version(&read_out), Value::Null, Failure::Type),
        (
            4,
            version(&read_out),
            Value::Text("wrong".into()),
            Failure::Type,
        ),
    ] {
        let command = write(&image, &read, id, expected, value);
        let before = image.bytes().to_vec();
        let (next, out) = apply(&image, &command);
        assert_eq!(out, Outcome::Failure(failure));
        assert_eq!(image.bytes(), before);
        assert_eq!(state(&next).sql, state(&image).sql);
        assert_eq!(apply(&next, &command), (next.clone(), out));
        assert_eq!(apply(&image, &command).0, next);
    }
}
#[test]
fn changed_body_conflicts_before_effects_or_output() {
    let (image, read, _) = fixture();
    let (image, _) = apply(&image, &read);
    let mut changed = read.clone();
    changed.body.operation = Operation::Read {
        key: vec![Value::Int(2)],
    };
    assert_eq!(
        image.evaluate_application(&changed).unwrap(),
        Err(Error::Conflict)
    );
    assert_eq!(
        image.application_status(&changed).unwrap(),
        Err(Error::Conflict)
    );
    assert!(image.application_status(&read).unwrap().unwrap().is_some());
}
#[test]
fn fresh_authority_scope_domain_catalog_generation_fences_precede_retry() {
    let (image, read, authority) = fixture();
    let (image, _) = apply(&image, &read);
    let mut variants = Vec::new();
    for n in 0..5 {
        let mut command = read.clone();
        match n {
            0 => command.identity.application += 1,
            1 => command.identity.authority_generation += 1,
            2 => command.body.domain += 1,
            3 => command.body.catalog_epoch += 1,
            _ => command.body.table += 1,
        }
        variants.push(command);
    }
    for command in variants {
        assert!(image.evaluate_application(&command).unwrap().is_err());
        assert!(image.application_status(&command).unwrap().is_err());
    }
    let revoked = image
        .evaluate_application_owner(&OwnerCommand::Revoke {
            application: 7,
            generation: 1,
        })
        .unwrap();
    assert_eq!(
        revoked.application_status(&read).unwrap(),
        Err(Error::Denied)
    );
    assert_eq!(
        revoked.evaluate_application(&read).unwrap(),
        Err(Error::Denied)
    );
    assert!(
        revoked
            .evaluate_application_owner(&OwnerCommand::Provision(authority.clone()))
            .is_err()
    );
    let mut denied = authority.clone();
    denied.generation = 2;
    denied.read_tables.clear();
    denied.write_tables.clear();
    let denied = image
        .evaluate_application_owner(&OwnerCommand::Provision(denied))
        .unwrap();
    let mut fresh = read.clone();
    fresh.identity.authority_generation = 2;
    assert_eq!(
        denied.application_status(&fresh).unwrap(),
        Err(Error::Denied)
    );
    // Canonical state changes are checked against current catalog/domain even
    // when the remote body still matches the stored authority and receipt.
    for domain in [false, true] {
        let mut stale = state(&image);
        if domain {
            stale.view.domain += 1;
            stale.applications.as_mut().unwrap().revoke_all();
        } else {
            stale.sql.epoch += 1;
        }
        let stale = super::tests::image(&stale);
        assert!(stale.application_status(&read).unwrap().is_err());
    }
}
#[test]
fn remote_claims_never_provision_and_owner_commands_are_closed() {
    let (_, read, mut authority) = fixture();
    let image = Image::new(123).unwrap();
    assert_eq!(
        image.evaluate_application(&read).unwrap(),
        Err(Error::Denied)
    );
    assert_eq!(state(&image).applications, None);
    authority.domain += 1;
    assert!(
        image
            .evaluate_application_owner(&OwnerCommand::Provision(authority))
            .is_err()
    );
    assert_eq!(state(&image).applications, None);
    assert_eq!(
        &encode(&ReplicaCommand::Application(read)).unwrap()[..4],
        &4u32.to_be_bytes()
    );
    assert_eq!(
        &encode(&ReplicaCommand::ApplicationOwner(OwnerCommand::Revoke {
            application: 7,
            generation: 1
        }))
        .unwrap()[..4],
        &5u32.to_be_bytes()
    );
}
#[test]
fn outcome_n_plus_one_capacity_fails_without_effects_replay_still_works() {
    let (mut image, read, _) = fixture();
    for n in 0..MAX_OUTCOMES {
        let mut command = read.clone();
        command.identity.operation = n as u128;
        image = apply(&image, &command).0;
    }
    let (_, out) = apply(&image, &read);
    let command = write(
        &image,
        &read,
        MAX_OUTCOMES as u128,
        version(&out),
        Value::Int(11),
    );
    let before = image.clone();
    assert_eq!(
        image.evaluate_application(&command).unwrap(),
        Err(Error::Capacity)
    );
    assert_eq!(image, before);
    assert_eq!(apply(&image, &read).0, image);
}
#[test]
fn encoded_outcome_and_business_headroom_admitted_before_adapter() {
    let (image, read, _) = fixture();
    let mut state = state(&image);
    let ledger = state.applications.as_mut().unwrap();
    // A valid retained-body payload can fill byte capacity before entry count.
    for n in 0..48 {
        let mut body = read.body.clone();
        body.operation = Operation::Read {
            key: vec![Value::Bytes(vec![0; MAX_VALUE_BYTES])],
        };
        let mut id = read.identity.clone();
        id.operation = n;
        ledger.outcomes.insert(
            (body.domain, id),
            Retained {
                body,
                outcome: Outcome::Failure(Failure::Type),
            },
        );
    }
    ledger.validate().unwrap();
    let full = super::tests::image(&state);
    let mut command = read;
    command.identity.operation = 100;
    assert_eq!(
        full.evaluate_application(&command).unwrap(),
        Err(Error::Capacity)
    );
    assert_eq!(state.sql, super::tests::state(&full).sql);
}
#[test]
fn corrupt_noncanonical_and_old_format_namespace_mismatch_reject() {
    let (image, read, _) = fixture();
    let (image, _) = apply(&image, &read);
    let original = records(&state(&image)).unwrap();
    let metadata: Metadata = decode(&original[&vec![0]]).unwrap();
    assert_eq!(metadata.format, 11);
    // Frozen old reader's acceptance gate rejects before any payload decoding.
    assert!(storage::format_policy::Features::read(metadata.format).is_none());
    for n in 0..6 {
        let mut records = original.clone();
        match n {
            0 => {
                let mut m: Metadata = decode(&records[&vec![0]]).unwrap();
                m.format = 3;
                records.insert(vec![0], encode(&m).unwrap());
            }
            1 => {
                records.remove(&vec![9]);
            }
            2 => {
                records.insert(vec![9], vec![255]);
            }
            3 => {
                records.insert(vec![10], vec![]);
            }
            4 => {
                let mut ledger = state(&image).applications.unwrap();
                ledger.version += 1;
                records.insert(vec![9], encode(&ledger).unwrap());
            }
            _ => {
                let mut m: Metadata = decode(&records[&vec![0]]).unwrap();
                m.format = 7;
                records.insert(vec![0], encode(&m).unwrap());
            }
        }
        assert!(from_records(records.clone()).is_err());
        assert!(Image::from_bytes(encode(&records).unwrap()).is_err());
    }
    // Map reordering decodes successfully, but every Image route reencodes.
    let mut reordered: Vec<_> = original.into_iter().collect();
    reordered.reverse();
    let forged = Image(encode(&reordered).unwrap());
    assert!(forged.snapshot().is_err());
    assert!(forged.application_status(&read).is_err());
    assert!(forged.evaluate_application(&read).is_err());
    let ledger = state(&image).applications.unwrap();
    let (id, authority) = ledger.authorities.iter().next().unwrap();
    let duplicate_map = encode(&(
        ledger.version,
        vec![(id, authority), (id, authority)],
        &ledger.outcomes,
    ))
    .unwrap();
    assert!(Ledger::from_bytes(&duplicate_map).is_err());
}
#[test]
fn export_restore_fresh_domain_revokes_active_authority_retains_forensic_outcomes() {
    let (image, read, authority) = fixture();
    let (image, out) = apply(&image, &read);
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
    std::fs::create_dir_all(&root).unwrap();
    let base = root.join(format!("application-{:032x}", u128::from_be_bytes(token)));
    let admin = crate::Admin::create_state(
        base.with_extension("db"),
        crate::Limits::default(),
        state(&image),
        crate::RecoveryPoint::None,
    )
    .unwrap();
    let export = base.with_extension("export");
    admin.export(&export).unwrap();
    let restored = crate::Admin::restore(
        &export,
        base.with_extension("restored"),
        crate::Limits::default(),
    )
    .unwrap();
    let s = restored.inner.read().unwrap();
    assert_ne!(s.view.domain, read.body.domain);
    let forensic = s.applications.as_ref().unwrap();
    assert_eq!(
        forensic
            .outcomes
            .get(&(read.body.domain, read.identity.clone()))
            .unwrap()
            .outcome,
        out
    );
    assert!(forensic.authorities[&authority.application].revoked);
    let restored = super::tests::image(&s);
    assert_eq!(
        restored.application_status(&read).unwrap(),
        Err(Error::Denied)
    );
    let mut fresh_authority = authority;
    fresh_authority.generation += 1;
    fresh_authority.domain = s.view.domain;
    let next = restored
        .evaluate_application_owner(&OwnerCommand::Provision(fresh_authority.clone()))
        .unwrap();
    let mut fresh = read;
    fresh.identity.authority_generation = fresh_authority.generation;
    fresh.body.domain = fresh_authority.domain;
    assert!(matches!(apply(&next, &fresh).1, Outcome::Success(_)));
}
