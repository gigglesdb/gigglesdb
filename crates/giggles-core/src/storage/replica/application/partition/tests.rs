use super::*;
use crate::relational::Prepared;
use crate::relational::{Float8, Numeric};
use crate::storage::replica::{Command as ReplicaCommand, Image};
use crate::storage::{self, State};

fn source() -> State {
    let mut state = storage::empty(123);
    let create = Prepared::create_engine(
        "items".into(),
        vec![ColumnDef {
            id: 0,
            name: "id".into(),
            data_type: DataType::BigInt,
            not_null: true,
        }],
        vec!["id".into()],
        123,
        &state.sql,
    )
    .unwrap();
    state.sql.execute(&create, &[], None).unwrap();
    state
}

fn image(state: &State) -> Image {
    Image::from_bytes(storage::encode(&storage::records(state).unwrap()).unwrap()).unwrap()
}

fn descriptor() -> ExpectedDescriptor {
    ExpectedDescriptor {
        contract_version: CONTRACT_VERSION,
        domain: 123,
        catalog_epoch: 1,
        table: TableIdentity {
            id: 1,
            name: "items".into(),
            columns: vec![ColumnIdentity {
                id: 2,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        },
        key_contract: KeyContract::RelationalEncodeKeyV1,
        range: 1,
        group: 2,
        ownership_epoch: 3,
        lower: Bound::MinusInfinity,
        upper: Bound::PlusInfinity,
        source_applied_index: 7,
        source_digest: [0; 32],
        limits: limits(),
        metadata: Metadata {
            generation: 1,
            schema: 0,
            last_time: 0,
            next_approval: 0,
            origin: None,
        },
        homes: Homes {
            catalog: 2,
            authority: 2,
            operations: OperationHome::OriginalOwningGroup,
        },
    }
}

fn limits() -> ProjectionLimits {
    ProjectionLimits {
        rows: MAX_PROJECTED_ROWS,
        bytes: MAX_PROJECTED_BYTES,
    }
}

#[test]
fn unknown_fields_are_rejected_at_every_descriptor_nesting() {
    let baseline = serde_json::to_value(descriptor()).unwrap();
    for pointer in ["", "/table", "/table/columns/0", "/metadata", "/homes"] {
        let mut value = baseline.clone();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), serde_json::json!({}));
        assert!(
            serde_json::from_value::<ExpectedDescriptor>(value).is_err(),
            "{pointer}"
        );
    }
    let key = CompleteKey {
        values: vec![Value::Int(1)],
        encoded: vec![1],
    };
    let mut value = serde_json::to_value(&key).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), serde_json::json!(null));
    assert!(serde_json::from_value::<CompleteKey>(value).is_err());
    for text in [
        r#"{"MinusInfinity":null,"PlusInfinity":null}"#,
        r#"{"Unknown":null}"#,
        r#"{"Key":{"values":[],"encoded":[],"unknown":0}}"#,
    ] {
        assert!(serde_json::from_str::<Bound>(text).is_err());
    }
}

#[test]
fn exact_comparison_preserves_decimal_scale_and_float_bits() {
    let decimal = |text| Value::Numeric(Numeric::parse(text).unwrap());
    assert!(!exact(&decimal("1.0"), &decimal("1.00")).unwrap());
    for (left, right) in [
        (0, 0x8000000000000000),
        (0x7ff8000000000001, 0x7ff8000000000002),
    ] {
        assert!(
            !exact(
                &Value::Float8(Float8::from_bits(left)),
                &Value::Float8(Float8::from_bits(right))
            )
            .unwrap()
        );
    }
    let baseline = descriptor();
    let mut changed = baseline.clone();
    changed.metadata.origin = Some(1);
    assert!(!exact(&baseline, &changed).unwrap());
    changed = baseline.clone();
    changed.homes.authority += 1;
    assert!(!exact(&baseline, &changed).unwrap());
}

#[test]
fn checked_capacity_rejects_zero_overflow_and_envelope_overhead() {
    assert!(ProjectionLimits { rows: 0, bytes: 1 }.validate().is_err());
    assert!(ProjectionLimits { rows: 1, bytes: 0 }.validate().is_err());
    let mut total = usize::MAX;
    assert!(add_size(&mut total, 1, usize::MAX).is_err());
    let mut descriptor = descriptor();
    let size = envelope_base_size(&descriptor, limits()).unwrap();
    assert!(size > 100);
    descriptor.limits = ProjectionLimits {
        rows: 1,
        bytes: size - 1,
    };
    assert!(
        validate_structure(
            &descriptor,
            ProjectionLimits {
                rows: 1,
                bytes: size - 1
            }
        )
        .is_err()
    );
    descriptor.limits.bytes = size;
    assert!(
        validate_structure(
            &descriptor,
            ProjectionLimits {
                rows: 1,
                bytes: size
            }
        )
        .is_ok()
    );
}

#[test]
fn invalid_complete_bounds_and_primary_key_shapes_fail_before_binding() {
    let baseline = descriptor();
    for (lower, upper) in [
        (Bound::PlusInfinity, Bound::PlusInfinity),
        (Bound::MinusInfinity, Bound::MinusInfinity),
        (
            Bound::Key(CompleteKey {
                values: vec![],
                encoded: vec![],
            }),
            Bound::PlusInfinity,
        ),
        (
            Bound::Key(CompleteKey {
                values: vec![Value::Null],
                encoded: vec![],
            }),
            Bound::PlusInfinity,
        ),
    ] {
        let mut descriptor = baseline.clone();
        descriptor.lower = lower;
        descriptor.upper = upper;
        assert!(validate_structure(&descriptor, limits()).is_err());
    }
    for indices in [vec![], vec![1], vec![0, 0]] {
        let mut descriptor = baseline.clone();
        descriptor.table.primary_key = indices;
        assert!(validate_structure(&descriptor, limits()).is_err());
    }
}

#[test]
fn classification_retains_all_metadata_and_rejects_unrelated_catalog() {
    let mut state = source();
    state.last_time = 9;
    state.next_approval = 7;
    state.origin = Some(1234);
    let metadata = classify_source(&state).unwrap();
    assert_eq!(
        (
            metadata.generation,
            metadata.schema,
            metadata.last_time,
            metadata.next_approval,
            metadata.origin
        ),
        (state.sql.generation, state.view.schema, 9, 7, Some(1234))
    );
    state.applications = Some(super::super::Ledger::default());
    assert!(classify_source(&state).is_ok());
    let create = Prepared::create_engine(
        "unrelated".into(),
        vec![ColumnDef {
            id: 0,
            name: "id".into(),
            data_type: DataType::BigInt,
            not_null: true,
        }],
        vec!["id".into()],
        123,
        &state.sql,
    )
    .unwrap();
    state.sql.execute(&create, &[], None).unwrap();
    assert!(classify_source(&state).is_err());
}

#[test]
fn unknown_empty_namespace_and_nonempty_application_authority_fail_closed() {
    let state = source();
    let mut records = storage::records(&state).unwrap();
    records.insert(vec![10], vec![]);
    assert!(Image::from_bytes(storage::encode(&records).unwrap()).is_err());
    let table = &state.sql.tables["items"];
    let authority = super::super::Authority {
        application: 7,
        generation: 1,
        domain: 123,
        catalog_epoch: state.sql.epoch,
        read_tables: std::collections::BTreeSet::from([table.id]),
        write_tables: std::collections::BTreeSet::new(),
    };
    let (candidate, _) = image(&state)
        .evaluate(&ReplicaCommand::ApplicationOwner(
            super::super::OwnerCommand::Provision(authority),
        ))
        .unwrap();
    let candidate = storage::replica::replica_state(candidate.bytes()).unwrap();
    assert!(classify_source(&candidate).is_err());
}

fn wire() -> UntrustedProjection {
    let descriptor = descriptor();
    let mut wire = UntrustedProjection {
        metadata: descriptor.metadata.clone(),
        descriptor,
        namespace_manifest: NAMESPACE_MANIFEST,
        limits: limits(),
        rows: vec![ProjectedRow {
            key: crate::relational::encode_key(&[Value::Int(1)]).unwrap(),
            values: vec![Value::Int(1)],
            version: 1,
        }],
        digest: [0; 32],
    };
    wire.digest = content_digest(&wire).unwrap();
    wire
}

#[test]
fn admission_preflight_binds_descriptor_metadata_manifest_and_content() {
    let wire = wire();
    let expected = wire.descriptor.clone();
    admission_preflight(&wire, &expected).unwrap();
    for mutation in 0..5 {
        let mut bad = wire.clone();
        match mutation {
            0 => bad.descriptor.source_applied_index += 1,
            1 => bad.metadata.last_time += 1,
            2 => bad.namespace_manifest[9] = Disposition::ProjectedTable,
            3 => bad.rows[0].version += 1,
            4 => bad.descriptor.homes.catalog += 1,
            _ => unreachable!(),
        }
        assert!(admission_preflight(&bad, &expected).is_err());
    }
    let mut bad = wire.clone();
    bad.limits.rows = 1;
    bad.descriptor.limits = bad.limits;
    bad.rows.push(bad.rows[0].clone());
    assert_eq!(
        admission_preflight(&bad, &bad.descriptor),
        Err(CoreError::Limit)
    );
    let mut bad = wire.clone();
    bad.limits.bytes = serialized_size(&bad).unwrap() - 1;
    bad.descriptor.limits = bad.limits;
    assert_eq!(
        admission_preflight(&bad, &bad.descriptor),
        Err(CoreError::Limit)
    );
    let mut raised = wire.clone();
    raised.limits.rows += 1;
    assert!(admission_preflight(&raised, &expected).is_err());
}

#[test]
fn existing_insert_binder_validates_full_row_without_execution() {
    let mut wire = wire();
    wire.descriptor.table.columns.push(ColumnIdentity {
        id: 3,
        name: "payload".into(),
        data_type: DataType::Numeric,
        not_null: true,
    });
    wire.rows[0]
        .values
        .push(Value::Numeric(Numeric::parse("1.00").unwrap()));
    bind_row_values(&wire, &wire.descriptor).unwrap();
    for (values, version) in [
        (vec![Value::Text("wrong type".into())], 1),
        (vec![Value::Null], 1),
        (vec![], 1),
        (vec![Value::Int(1)], 0),
    ] {
        let mut bad = wire.clone();
        bad.rows[0].values = values;
        bad.rows[0].version = version;
        assert!(bind_row_values(&bad, &wire.descriptor).is_err());
    }
    let mut bad = wire.clone();
    bad.rows[0].values[1] = Value::Text("not numeric".into());
    assert!(bind_row_values(&bad, &wire.descriptor).is_err());
}

fn populated_source() -> State {
    let mut state = storage::empty(123);
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
                name: "payload".into(),
                data_type: DataType::Numeric,
                not_null: true,
            },
            ColumnDef {
                id: 0,
                name: "bits".into(),
                data_type: DataType::DoublePrecision,
                not_null: true,
            },
        ],
        vec!["id".into()],
        123,
        &state.sql,
    )
    .unwrap();
    state.sql.execute(&create, &[], None).unwrap();
    let table = state.sql.tables["items"].clone();
    for id in 1..=4 {
        let values = vec![
            Value::Int(id),
            Value::Numeric(Numeric::parse(&format!("{id}.00")).unwrap()),
            Value::Float8(Float8::from_bits(if id % 2 == 0 {
                0x8000000000000000
            } else {
                0x7ff8000000001234
            })),
        ];
        let insert = Prepared::insert_engine(&table, values, 123, &state.sql).unwrap();
        state.sql.execute(&insert, &[], None).unwrap();
    }
    state
}

fn request_for(state: &State, image: &Image) -> ProjectionRequest {
    let mut descriptor = descriptor();
    descriptor.table = TableIdentity::from(&state.sql.tables["items"]);
    descriptor.catalog_epoch = state.sql.epoch;
    descriptor.metadata = classify_source(state).unwrap();
    descriptor.source_digest = Sha256::digest(image.bytes()).into();
    ProjectionRequest {
        descriptor,
        limits: limits(),
    }
}

#[test]
fn public_projector_is_deterministic_preserves_raw_rows_and_adjacent_coverage() {
    let state = populated_source();
    let image = image(&state);
    let before = image.bytes().to_vec();
    let request = request_for(&state, &image);
    let whole = project(&image, &request).unwrap();
    let repeat = project(&image, &request).unwrap();
    assert!(exact(whole.as_untrusted(), repeat.as_untrusted()).unwrap());
    assert_eq!(whole.rows().len(), 4);
    validate_projection(whole.as_untrusted(), &request.descriptor).unwrap();
    for row in whole.rows() {
        let Value::Int(id) = row.values[0] else {
            panic!("integer key")
        };
        assert_eq!(
            row.version,
            state
                .sql
                .row_version_engine(&state.sql.tables["items"], &[Value::Int(id)])
                .unwrap()
        );
        assert_eq!(
            storage::encode(&row.values[1]).unwrap(),
            storage::encode(&Value::Numeric(
                Numeric::parse(&format!("{id}.00")).unwrap()
            ))
            .unwrap()
        );
        let Value::Float8(bits) = row.values[2] else {
            panic!("float payload")
        };
        assert_eq!(
            bits.to_bits(),
            if id % 2 == 0 {
                0x8000000000000000
            } else {
                0x7ff8000000001234
            }
        );
    }
    let mut left = request.clone();
    left.descriptor.upper = int_bound(3);
    let mut right = request.clone();
    right.descriptor.lower = int_bound(3);
    let left = project(&image, &left).unwrap();
    let right = project(&image, &right).unwrap();
    assert_eq!((left.rows().len(), right.rows().len()), (2, 2));
    let union: Vec<_> = left.rows().iter().chain(right.rows()).cloned().collect();
    assert!(exact(&union, &whole.rows().to_vec()).unwrap());
    assert_eq!(image.bytes(), before);
}

#[test]
fn public_admission_rejects_each_forged_descriptor_and_raw_row_condition() {
    let state = populated_source();
    let image = image(&state);
    let request = request_for(&state, &image);
    let good = project(&image, &request).unwrap();
    for mutation in 0..11 {
        let mut bad = good.as_untrusted().clone();
        match mutation {
            0 => bad.descriptor.domain += 1,
            1 => bad.descriptor.catalog_epoch += 1,
            2 => bad.descriptor.table.columns[1].data_type = DataType::Text,
            3 => bad.descriptor.group += 1,
            4 => bad.descriptor.range += 1,
            5 => bad.descriptor.ownership_epoch += 1,
            6 => bad.descriptor.source_digest[0] ^= 1,
            7 => bad.descriptor.metadata.schema += 1,
            8 => bad.rows[0].values[1] = Value::Text("forged".into()),
            9 => bad.rows[0].version = 0,
            10 => bad.rows[0].key.push(0),
            _ => unreachable!(),
        }
        bad.digest = content_digest(&bad).unwrap();
        assert!(
            validate_projection(&bad, &request.descriptor).is_err(),
            "mutation {mutation}"
        );
    }
    let mut bad = good.as_untrusted().clone();
    bad.rows.swap(0, 1);
    bad.digest = content_digest(&bad).unwrap();
    assert!(validate_projection(&bad, &request.descriptor).is_err());
    let mut bad = good.as_untrusted().clone();
    bad.rows[1] = bad.rows[0].clone();
    bad.digest = content_digest(&bad).unwrap();
    assert!(validate_projection(&bad, &request.descriptor).is_err());
    let mut narrow = request.clone();
    narrow.descriptor.lower = int_bound(2);
    let mut bad = good.as_untrusted().clone();
    bad.descriptor = narrow.descriptor.clone();
    bad.digest = content_digest(&bad).unwrap();
    assert!(validate_projection(&bad, &narrow.descriptor).is_err());
}

#[test]
fn public_projection_rejects_bad_bounds_fences_and_capacity_without_mutation() {
    let state = populated_source();
    let image = image(&state);
    let before = image.bytes().to_vec();
    let request = request_for(&state, &image);
    for mutation in 0..10 {
        let mut bad = request.clone();
        match mutation {
            0 => bad.descriptor.lower = int_bound(3),
            1 => bad.descriptor.domain += 1,
            2 => bad.descriptor.catalog_epoch += 1,
            3 => bad.descriptor.table.columns[1].name.push('x'),
            4 => bad.descriptor.source_digest[0] ^= 1,
            5 => bad.descriptor.metadata.generation += 1,
            6 => {
                bad.limits.rows = 3;
                bad.descriptor.limits = bad.limits;
            }
            7 => {
                bad.limits.bytes = envelope_base_size(&bad.descriptor, bad.limits).unwrap();
                bad.descriptor.limits = bad.limits;
            }
            8 => {
                bad.descriptor.lower = Bound::Key(CompleteKey {
                    values: vec![Value::Text("wrong".into())],
                    encoded: vec![],
                })
            }
            9 => {
                bad.descriptor.lower = Bound::Key(CompleteKey {
                    values: vec![Value::Int(1)],
                    encoded: vec![],
                })
            }
            _ => unreachable!(),
        }
        if mutation == 0 {
            bad.descriptor.upper = int_bound(3);
        }
        assert!(project(&image, &bad).is_err(), "mutation {mutation}");
        assert_eq!(image.bytes(), before);
    }
}

#[test]
fn public_projection_enforces_exact_encoded_capacity_boundary() {
    let state = populated_source();
    let image = image(&state);
    let mut request = request_for(&state, &image);
    let baseline = project(&image, &request).unwrap();
    let size = serialized_size(baseline.as_untrusted()).unwrap();
    request.limits = ProjectionLimits {
        rows: 4,
        bytes: size,
    };
    request.descriptor.limits = request.limits;
    let exact = project(&image, &request).unwrap();
    assert_eq!(serialized_size(exact.as_untrusted()).unwrap(), size);
    validate_projection(exact.as_untrusted(), &request.descriptor).unwrap();
    request.limits.bytes -= 1;
    request.descriptor.limits = request.limits;
    assert!(matches!(project(&image, &request), Err(CoreError::Limit)));
}

#[test]
fn numeric_pk_equivalence_preserves_submitted_decimal_representation() {
    let mut state = storage::empty(123);
    let create = Prepared::create_engine(
        "items".into(),
        vec![ColumnDef {
            id: 0,
            name: "id".into(),
            data_type: DataType::Numeric,
            not_null: true,
        }],
        vec!["id".into()],
        123,
        &state.sql,
    )
    .unwrap();
    state.sql.execute(&create, &[], None).unwrap();
    let table = state.sql.tables["items"].clone();
    let original = Value::Numeric(Numeric::parse("1.00").unwrap());
    let insert = Prepared::insert_engine(&table, vec![original.clone()], 123, &state.sql).unwrap();
    state.sql.execute(&insert, &[], None).unwrap();
    let image = image(&state);
    let mut request = request_for(&state, &image);
    let values = vec![Value::Numeric(Numeric::parse("1.0").unwrap())];
    request.descriptor.lower = Bound::Key(CompleteKey {
        encoded: crate::relational::encode_key(&values).unwrap(),
        values,
    });
    let projection = project(&image, &request).unwrap();
    assert_eq!(projection.rows().len(), 1);
    assert!(exact(&projection.rows()[0].values[0], &original).unwrap());
    validate_projection(projection.as_untrusted(), &request.descriptor).unwrap();
    // Canonical key equivalence cannot change the independently selected bound's
    // submitted scale when checking the exact descriptor identity.
    let mut changed = projection.as_untrusted().clone();
    if let Bound::Key(key) = &mut changed.descriptor.lower {
        key.values[0] = original;
    }
    changed.digest = content_digest(&changed).unwrap();
    assert!(validate_projection(&changed, &request.descriptor).is_err());
}

#[test]
fn wire_unknown_fields_are_rejected_in_rows_limits_and_outer_envelope() {
    let wire = wire();
    let baseline = serde_json::to_value(wire).unwrap();
    for pointer in ["", "/rows/0", "/limits", "/descriptor/limits"] {
        let mut value = baseline.clone();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), serde_json::json!(null));
        assert!(serde_json::from_value::<UntrustedProjection>(value).is_err());
    }
}

#[test]
fn complete_map_includes_rows_beyond_the_query_page_cap() {
    let mut state = source();
    let table = state.sql.tables["items"].clone();
    for id in 1..=crate::relational::MAX_ROWS + 1 {
        let insert =
            Prepared::insert_engine(&table, vec![Value::Int(id as i64)], 123, &state.sql).unwrap();
        state.sql.execute(&insert, &[], None).unwrap();
    }
    let image = image(&state);
    let mut request = request_for(&state, &image);
    assert!(matches!(project(&image, &request), Err(CoreError::Limit)));
    request.descriptor.lower = int_bound((crate::relational::MAX_ROWS + 1) as i64);
    let tail = project(&image, &request).unwrap();
    assert_eq!(tail.rows().len(), 1);
    assert!(exact(&tail.rows()[0].values, &vec![Value::Int(1025)]).unwrap());
}

#[test]
fn composite_keys_require_complete_tuples_and_follow_engine_byte_order() {
    let mut state = storage::empty(123);
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
                name: "bucket".into(),
                data_type: DataType::Text,
                not_null: true,
            },
        ],
        vec!["id".into(), "bucket".into()],
        123,
        &state.sql,
    )
    .unwrap();
    state.sql.execute(&create, &[], None).unwrap();
    let table = state.sql.tables["items"].clone();
    for (id, bucket) in [(1, "a"), (1, "b"), (2, "a")] {
        let insert = Prepared::insert_engine(
            &table,
            vec![Value::Int(id), Value::Text(bucket.into())],
            123,
            &state.sql,
        )
        .unwrap();
        state.sql.execute(&insert, &[], None).unwrap();
    }
    let image = image(&state);
    let mut request = request_for(&state, &image);
    request.descriptor.lower = int_bound(1);
    assert!(project(&image, &request).is_err());
    let values = vec![Value::Int(1), Value::Text("b".into())];
    request.descriptor.lower = Bound::Key(CompleteKey {
        encoded: crate::relational::encode_key(&values).unwrap(),
        values,
    });
    let projected = project(&image, &request).unwrap();
    assert_eq!(projected.rows().len(), 2);
    assert!(
        exact(
            &projected.rows()[0].values,
            &vec![Value::Int(1), Value::Text("b".into())]
        )
        .unwrap()
    );
    validate_projection(projected.as_untrusted(), &request.descriptor).unwrap();
}

#[test]
fn oversized_deserialized_image_is_rejected_before_hashing_or_decoding() {
    let state = source();
    let image = image(&state);
    let request = request_for(&state, &image);
    // Bincode reconstitutes the existing Image tuple without its checked ctor.
    let oversized: Image = bincode::deserialize(
        &bincode::serialize(&vec![0u8; storage::MAX_BYTES as usize + 1]).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        project(&oversized, &request),
        Err(CoreError::Limit)
    ));
}

#[test]
fn public_projection_rejects_repair_task_accounting_and_empty_native_sidecars() {
    use crate::{
        Column, Grant, Key, Policy, Principal, Row, RowId, Status, TaskId, TaskState, TenantId,
    };
    use std::collections::BTreeSet;
    let base = source();
    let base_image = image(&base);
    let base_request = request_for(&base, &base_image);
    for class in 0..4 {
        let mut state = base.clone();
        if class == 0 {
            state.view.rows.insert(
                Key {
                    tenant: TenantId(1),
                    row: RowId(1),
                },
                Row {
                    status: Status::Failed,
                    failed_at: 1,
                    attempt_count: 0,
                    version: 1,
                },
            );
        }
        if class == 1 || class == 2 {
            state.view.tasks.insert(
                TaskId(1),
                TaskState {
                    grant: Grant {
                        task: TaskId(1),
                        issuer: Principal(1),
                        executor: Principal(2),
                        approver: Principal(3),
                        policy: Policy {
                            tenant: TenantId(1),
                            columns: BTreeSet::from([Column::Status]),
                            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                            incident: (0, 10),
                            distinct_limit: 1,
                            transition_limit: 1,
                            approval_above: 1,
                            valid_from: 0,
                            expires: 10,
                        },
                    },
                    revoked: false,
                    touched: BTreeSet::new(),
                    transitions: 0,
                },
            );
        }
        if class == 2 {
            state.accounting.insert(
                TaskId(1),
                crate::TaskAccounting {
                    limits: Default::default(),
                    used: Default::default(),
                },
            );
        }
        if class == 3 {
            state.native_outcomes = Some(Default::default());
        }
        // The source is a valid core State. Native presence is deliberately
        // outside replica Image support; deserialize tests its entry fence.
        storage::validate(&state).unwrap();
        let bytes = storage::encode(&storage::records(&state).unwrap()).unwrap();
        let source: Image = bincode::deserialize(&bincode::serialize(&bytes).unwrap()).unwrap();
        let mut request = base_request.clone();
        request.descriptor.source_digest = Sha256::digest(&bytes).into();
        assert!(project(&source, &request).is_err(), "sidecar class {class}");
        assert_eq!(source.bytes(), bytes);
    }
}

#[test]
fn public_apis_are_independent_of_already_cancelled_ambient_scope() {
    let state = populated_source();
    let image = image(&state);
    let request = request_for(&state, &image);
    let before = image.bytes().to_vec();
    let baseline = project(&image, &request).unwrap();
    let cancel = crate::Cancellation::default();
    assert!(cancel.cancel());
    let (projected, admitted) = cancel.scope(|| {
        let admitted = validate_projection(baseline.as_untrusted(), &request.descriptor);
        let projected = project(&image, &request);
        assert_eq!(crate::cancellation::check(), Err(CoreError::Cancelled));
        (projected, admitted)
    });
    assert_eq!(cancel.check(), Err(CoreError::Cancelled));
    assert_eq!(image.bytes(), before);
    assert!(exact(admitted.unwrap().as_untrusted(), baseline.as_untrusted()).unwrap());
    // This assertion deliberately exposes an ambient dependency anywhere in
    // the complete canonical source-validation path. Never reset that scope.
    assert!(exact(projected.unwrap().as_untrusted(), baseline.as_untrusted()).unwrap());
}

#[test]
fn explicit_canonical_ancestors_preserve_ambient_wrappers_and_inputs() {
    let state = populated_source();
    let records = storage::records(&state).unwrap();
    let bytes = storage::encode(&records).unwrap();
    let before = bytes.clone();
    let cancel = crate::Cancellation::default();
    assert!(cancel.cancel());
    cancel.scope(|| {
        let decoded = storage::from_records_with_cancel(records.clone(), &mut || false).unwrap();
        assert_eq!(storage::records(&decoded).unwrap(), records);
        let decoded = storage::replica::replica_state_with_cancel(&bytes, &mut || false).unwrap();
        assert_eq!(storage::records(&decoded).unwrap(), records);
        assert!(matches!(
            storage::from_records_with_cancel(records.clone(), &mut || true),
            Err(CoreError::Cancelled)
        ));
        assert!(matches!(
            storage::replica::replica_state_with_cancel(&bytes, &mut || true),
            Err(CoreError::Cancelled)
        ));
        assert!(matches!(
            storage::from_records(records.clone()),
            Err(CoreError::Cancelled)
        ));
        assert!(matches!(
            storage::replica::replica_state(&bytes),
            Err(CoreError::Cancelled)
        ));
        assert_eq!(crate::cancellation::check(), Err(CoreError::Cancelled));
    });
    assert_eq!(bytes, before);
    assert_eq!(cancel.check(), Err(CoreError::Cancelled));
}

#[test]
fn explicit_ancestor_keeps_malformed_unknown_feature_and_roundtrip_gates() {
    let state = source();
    let records = storage::records(&state).unwrap();
    let cancel = crate::Cancellation::default();
    assert!(cancel.cancel());
    cancel.scope(|| {
        let mut calls = 0;
        let mut callback = || { calls += 1; false };
        let mut unknown = records.clone();
        unknown.insert(vec![255], vec![]);
        assert!(matches!(storage::from_records_with_cancel(unknown, &mut callback), Err(CoreError::Corrupt(_))));
        assert_eq!(calls, 0);
        let mut malformed = records.clone();
        malformed.insert(vec![0], vec![255]);
        assert!(matches!(storage::from_records(malformed), Err(CoreError::Corrupt(_))));
        let mut future = records.clone();
        let mut metadata: storage::Metadata = storage::decode(&future[&vec![0]]).unwrap();
        metadata.format = u32::MAX;
        future.insert(vec![0], storage::encode(&metadata).unwrap());
        assert!(matches!(storage::from_records_with_cancel(future, &mut || false), Err(CoreError::Unsupported)));
        let reversed: Vec<_> = records.clone().into_iter().rev().collect();
        let noncanonical = storage::encode(&reversed).unwrap();
        assert!(matches!(storage::replica::replica_state_with_cancel(&noncanonical, &mut || false), Err(CoreError::Corrupt(message)) if message == "noncanonical image"));
        let oversized = vec![0; storage::MAX_BYTES as usize + 1];
        assert!(matches!(storage::replica::replica_state_with_cancel(&oversized, &mut || false), Err(CoreError::Limit)));
    });
    assert_eq!(storage::records(&state).unwrap(), records);
}

fn indexed_source() -> State {
    let mut state = storage::empty(123);
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
                name: "tag".into(),
                data_type: DataType::Text,
                not_null: true,
            },
        ],
        vec!["id".into()],
        123,
        &state.sql,
    )
    .unwrap();
    state.sql.execute(&create, &[], None).unwrap();
    let table = state.sql.tables["items"].clone();
    let insert = Prepared::insert_engine(
        &table,
        vec![Value::Int(1), Value::Text("a".into())],
        123,
        &state.sql,
    )
    .unwrap();
    state.sql.execute(&insert, &[], None).unwrap();
    state
        .sql
        .secondary_configure_test(table.id, "by_tag", &[table.columns[1].id], || false)
        .unwrap();
    state
}

#[test]
fn explicit_index_restore_and_error_mapping_never_read_ambient_cancellation() {
    let state = indexed_source();
    let records = storage::records(&state).unwrap();
    let bytes = storage::encode(&records).unwrap();
    let cancel = crate::Cancellation::default();
    assert!(cancel.cancel());
    let restored = cancel.scope(|| {
        let mut total = 0;
        let restored = storage::from_records_with_cancel(records.clone(), &mut || {
            total += 1;
            false
        })
        .unwrap();
        assert!(total > 3);
        assert!(matches!(
            storage::replica::replica_state_with_cancel(&bytes, &mut || false),
            Err(CoreError::Unsupported)
        ));
        for stop in [1, total / 2, total] {
            let mut current = 0;
            assert!(matches!(
                storage::from_records_with_cancel(records.clone(), &mut || {
                    current += 1;
                    current == stop
                }),
                Err(CoreError::Cancelled)
            ));
        }
        let mut malformed = records.clone();
        malformed.insert(vec![8], vec![255]);
        assert!(matches!(
            storage::from_records_with_cancel(malformed, &mut || false),
            Err(CoreError::Corrupt(_))
        ));
        assert!(matches!(
            storage::secondary_index::sql_error_with_cancel(
                crate::relational::SqlError::Type,
                &mut || false
            ),
            CoreError::Corrupt(_)
        ));
        assert_eq!(
            storage::secondary_index::sql_error_with_cancel(
                crate::relational::SqlError::Aborted,
                &mut || false
            ),
            CoreError::Cancelled
        );
        assert_eq!(
            storage::secondary_index::sql_error_with_cancel(
                crate::relational::SqlError::Type,
                &mut || true
            ),
            CoreError::Cancelled
        );
        let mut combined = records.clone();
        combined.insert(vec![9], super::super::Ledger::default().bytes().unwrap());
        assert!(matches!(
            storage::from_records_with_cancel(combined, &mut || false),
            Err(CoreError::Unsupported)
        ));
        restored
    });
    assert_eq!(storage::records(&restored).unwrap(), records);
    assert_eq!(storage::records(&state).unwrap(), records);
    assert_eq!(cancel.check(), Err(CoreError::Cancelled));
}

fn int_bound(value: i64) -> Bound {
    let values = vec![Value::Int(value)];
    Bound::Key(CompleteKey {
        encoded: crate::relational::encode_key(&values).unwrap(),
        values,
    })
}

#[test]
fn encoded_bounds_are_half_open_and_reject_equal_inverted_or_forged_bytes() {
    // This tests byte interval mechanics only, not owner PK type binding.
    let bind = |values: &[Value]| crate::relational::encode_key(values).map_err(CoreError::Sql);
    let mut descriptor = descriptor();
    descriptor.lower = int_bound(1);
    descriptor.upper = int_bound(3);
    let bounds = bind_bounds(&descriptor, bind).unwrap();
    for (value, included) in [(0, false), (1, true), (2, true), (3, false)] {
        assert_eq!(
            bounds.contains(&crate::relational::encode_key(&[Value::Int(value)]).unwrap()),
            included
        );
    }
    for upper in [int_bound(1), int_bound(0)] {
        descriptor.upper = upper;
        assert!(bind_bounds(&descriptor, bind).is_err());
    }
    descriptor.upper = Bound::PlusInfinity;
    if let Bound::Key(key) = &mut descriptor.lower {
        key.encoded.push(0);
    }
    assert!(bind_bounds(&descriptor, bind).is_err());
    descriptor.lower = Bound::MinusInfinity;
    assert!(bind_bounds(&descriptor, bind).unwrap().contains(&[]));
}
