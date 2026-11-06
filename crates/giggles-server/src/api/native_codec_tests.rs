use super::*;
use crate::api::{json, proto as p};
use prost::Message;
use serde_json::{Value as J, json as j};

fn decode(v: J) -> R<codec::Invocation> {
    let wire = json::decode_execute(&serde_json::to_vec(&v).unwrap())?;
    let bytes = wire.encode_to_vec();
    let wire = p::ExecuteRequest::decode(bytes.as_slice()).unwrap();
    Ok(codec::decode_execute(wire)?.request)
}
fn operation(v: J) -> R<o::Request> {
    match decode(j!({"operation":{"request":v}}))? {
        codec::Invocation::Operation { request, .. } => Ok(request),
        _ => panic!("native operation expected"),
    }
}
fn literal(n: i64) -> J {
    j!({"value":{"literal":{"intValue":n.to_string()}}})
}
fn field() -> J {
    j!({"field":"id"})
}
fn read() -> J {
    j!({"read":{"table":"app"}})
}
fn native_expr(v: J) -> o::Expr {
    let o::Request::Read {
        filter: Some(e), ..
    } = operation(j!({"read":{"table":"app","filter":v}})).unwrap()
    else {
        panic!()
    };
    e
}
fn case(depth: usize) -> J {
    let mut e = literal(7);
    for _ in 0..depth {
        e = j!({"caseExpr":{"arms":[{"when":{"value":{"literal":{"boolValue":true}}},"then":e}]}});
    }
    e
}
fn wrapped(e: J) -> J {
    j!({"returning":{"request":{"onConflict":{"request":{"insertMany":{"table":"app","columns":["id"],"rows":[{"values":[e]}],"cap":{"literal":{"intValue":"1"}}}},"target":["id"],"action":{"nothing":{}}}},"projection":[]}})
}

#[test]
fn all_ten_requests_and_three_controls_preserve_typed_meaning() {
    let filter = o::Expr::Field("id".into());
    let value = o::ValueExpr::Parameter("id".into());
    let assignment = j!({"column":"id","value":{"parameter":"id"}});
    let expression = j!({"column":"id","expression":{"field":"id"}});
    let insert = o::Request::Insert {
        table: "app".into(),
        values: vec![("id".into(), value.clone())],
    };
    let vectors = [
        (
            j!({"read":{"table":"app","filter":field(),"order":[{"field":"id","descending":false}],"limit":{"parameter":"id"},"projection":["id"]}}),
            o::Request::Read {
                table: "app".into(),
                filter: Some(filter.clone()),
                order: vec![o::Sort {
                    field: "id".into(),
                    descending: false,
                }],
                limit: Some(value.clone()),
                projection: vec!["id".into()],
            },
        ),
        (
            j!({"insert":{"table":"app","values":[assignment.clone()]}}),
            insert.clone(),
        ),
        (
            j!({"update":{"table":"app","filter":field(),"guard":field(),"values":[assignment],"intent":{"one":{}}}}),
            o::Request::Update {
                table: "app".into(),
                filter: filter.clone(),
                guard: Some(filter.clone()),
                values: vec![("id".into(), value.clone())],
                intent: o::Intent::One,
            },
        ),
        (
            j!({"delete":{"table":"app","filter":field(),"cap":{"parameter":"id"}}}),
            o::Request::Delete {
                table: "app".into(),
                filter: filter.clone(),
                guard: None,
                cap: Some(value.clone()),
            },
        ),
        (
            j!({"updateExpressions":{"table":"app","filter":field(),"values":[expression.clone()],"intent":{"many":{"cap":{"parameter":"id"}}}}}),
            o::Request::UpdateExpressions {
                table: "app".into(),
                filter: filter.clone(),
                guard: None,
                values: vec![("id".into(), filter.clone())],
                intent: o::Intent::Many { cap: value.clone() },
            },
        ),
        (
            j!({"readExpressions":{"table":"app","projection":[{"expression":field(),"alias":"x"}]}}),
            o::Request::ReadExpressions {
                table: "app".into(),
                filter: None,
                order: vec![],
                limit: None,
                projection: vec![o::Projection {
                    expression: filter.clone(),
                    alias: Some("x".into()),
                }],
            },
        ),
        (
            j!({"returning":{"request":{"insert":{"table":"app","values":[{"column":"id","value":{"parameter":"id"}}]}}}}),
            o::Request::Returning {
                request: Box::new(insert.clone()),
                projection: vec![],
            },
        ),
        (
            j!({"insertExpressions":{"table":"app","values":[expression]}}),
            o::Request::InsertExpressions {
                table: "app".into(),
                values: vec![("id".into(), filter.clone())],
            },
        ),
        (
            j!({"insertMany":{"table":"app","columns":["id"],"rows":[{"values":[field()]}],"cap":{"parameter":"id"}}}),
            o::Request::InsertMany {
                table: "app".into(),
                columns: vec!["id".into()],
                rows: vec![vec![filter]],
                cap: value,
            },
        ),
        (
            j!({"onConflict":{"request":{"insert":{"table":"app","values":[{"column":"id","value":{"parameter":"id"}}]}},"target":["id"],"action":{"nothing":{}}}}),
            o::Request::OnConflict {
                request: Box::new(insert),
                target: vec!["id".into()],
                action: o::ConflictAction::Nothing,
            },
        ),
    ];
    for (wire, expected) in vectors {
        assert_eq!(operation(wire).unwrap(), expected);
    }
    assert!(matches!(
        decode(j!({"begin":{}})).unwrap(),
        codec::Invocation::Begin
    ));
    assert!(matches!(
        decode(j!({"commit":{}})).unwrap(),
        codec::Invocation::Commit
    ));
    assert!(matches!(
        decode(j!({"rollback":{}})).unwrap(),
        codec::Invocation::Rollback
    ));
}

#[test]
fn all_expression_arms_and_enum_variants_are_explicit() {
    let f = o::Expr::Field("id".into());
    let b = || Box::new(f.clone());
    assert_eq!(
        native_expr(literal(i64::MIN)),
        o::Expr::Value(o::ValueExpr::Literal(o::Value::Int(i64::MIN)))
    );
    assert_eq!(native_expr(field()), f);
    for (wire, op) in [
        ("EQ", o::Compare::Eq),
        ("NE", o::Compare::Ne),
        ("LT", o::Compare::Lt),
        ("LE", o::Compare::Le),
        ("GT", o::Compare::Gt),
        ("GE", o::Compare::Ge),
    ] {
        assert_eq!(
            native_expr(
                j!({"compare":{"left":field(),"comparison":format!("NATIVE_COMPARISON_{wire}"),"right":field()}})
            ),
            o::Expr::Compare(b(), op, b())
        );
    }
    assert_eq!(
        native_expr(j!({"isNull":{"expression":field(),"negated":false}})),
        o::Expr::IsNull(b(), false)
    );
    assert_eq!(native_expr(j!({"notExpr":field()})), o::Expr::Not(b()));
    assert_eq!(
        native_expr(j!({"andExpr":{"left":field(),"right":field()}})),
        o::Expr::And(b(), b())
    );
    assert_eq!(
        native_expr(j!({"orExpr":{"left":field(),"right":field()}})),
        o::Expr::Or(b(), b())
    );
    assert_eq!(native_expr(j!({"negate":field()})), o::Expr::Negate(b()));
    for (wire, op) in [
        ("ADD", o::Arithmetic::Add),
        ("SUBTRACT", o::Arithmetic::Subtract),
        ("MULTIPLY", o::Arithmetic::Multiply),
        ("DIVIDE", o::Arithmetic::Divide),
        ("REMAINDER", o::Arithmetic::Remainder),
    ] {
        assert_eq!(
            native_expr(
                j!({"arithmetic":{"left":field(),"operator":format!("NATIVE_ARITHMETIC_OPERATOR_{wire}"),"right":field()}})
            ),
            o::Expr::Arithmetic(b(), op, b())
        );
    }
    for (wire, local) in [
        (
            j!({"field":{"local":"r","field":"id"}}),
            o::LocalRef::Field {
                local: "r".into(),
                field: "id".into(),
            },
        ),
        (
            j!({"status":"r"}),
            o::LocalRef::Status { local: "r".into() },
        ),
        (
            j!({"changed":"r"}),
            o::LocalRef::Changed { local: "r".into() },
        ),
        (j!({"count":"r"}), o::LocalRef::Count { local: "r".into() }),
    ] {
        assert_eq!(native_expr(j!({"local":wire})), o::Expr::Local(local));
    }
    for (wire, function) in [
        ("COALESCE", o::ScalarFunction::Coalesce),
        ("ABS", o::ScalarFunction::Abs),
        ("LENGTH", o::ScalarFunction::Length),
    ] {
        assert_eq!(
            native_expr(
                j!({"call":{"function":format!("NATIVE_FUNCTION_{wire}"),"arguments":[field()]}})
            ),
            o::Expr::Call {
                function,
                arguments: vec![f.clone()]
            }
        );
    }
    assert_eq!(
        native_expr(
            j!({"caseExpr":{"operand":field(),"arms":[{"when":field(),"then":field()}],"elseExpr":field()}})
        ),
        o::Expr::Case {
            operand: Some(b()),
            arms: vec![(f.clone(), f.clone())],
            else_expr: Some(b())
        }
    );
    for (wire, source) in [
        ("EXISTING", o::ConflictSource::Existing),
        ("INCOMING", o::ConflictSource::Incoming),
    ] {
        assert_eq!(
            native_expr(
                j!({"conflictField":{"source":format!("NATIVE_CONFLICT_SOURCE_{wire}"),"field":"id"}})
            ),
            o::Expr::ConflictField {
                source,
                field: "id".into()
            }
        );
    }
}

#[test]
fn case_depth_24_survives_both_protocols_inside_all_wrappers() {
    assert!(operation(wrapped(case(24))).is_ok());
    assert!(operation(wrapped(case(25))).is_err());
    for depth in [24, 25] {
        let input = j!({"transaction":{"transaction":{"readOnly":false,"steps":[{"letBinding":{"name":"r","request":wrapped(case(depth))}}],"emit":{"values":[{"scalar":case(depth)}]}}}});
        assert_eq!(decode(input).is_ok(), depth == 24);
    }
    // Exercise Prost's decoder independently of the stricter JSON tree decoder.
    fn pe(depth: usize) -> p::NativeExpr {
        if depth == 0 {
            return p::NativeExpr {
                expression: Some(p::native_expr::Expression::Field("id".into())),
            };
        }
        p::NativeExpr {
            expression: Some(p::native_expr::Expression::CaseExpr(Box::new(
                p::NativeCase {
                    operand: None,
                    arms: vec![p::NativeCaseArm {
                        when: Some(pe(0)),
                        then: Some(pe(depth - 1)),
                    }],
                    else_expr: None,
                },
            ))),
        }
    }
    for depth in [24, 25] {
        let mut wire = json::decode_execute(
            &serde_json::to_vec(&j!({"operation":{"request":wrapped(field())}})).unwrap(),
        )
        .unwrap();
        let p::execute_request::Request::Operation(op) = wire.request.as_mut().unwrap() else {
            panic!()
        };
        let p::native_request::Request::Returning(r) =
            op.request.as_mut().unwrap().request.as_mut().unwrap()
        else {
            panic!()
        };
        let p::native_request::Request::OnConflict(c) =
            r.request.as_mut().unwrap().request.as_mut().unwrap()
        else {
            panic!()
        };
        let p::native_request::Request::InsertMany(i) =
            c.request.as_mut().unwrap().request.as_mut().unwrap()
        else {
            panic!()
        };
        i.rows[0].values[0] = pe(depth);
        let bytes = wire.encode_to_vec();
        let decoded = p::ExecuteRequest::decode(bytes.as_slice())
            .expect("Prost default recursion accepts the remote boundary");
        assert_eq!(codec::decode_execute(decoded).is_ok(), depth == 24);
    }
}

#[test]
fn aggregate_expression_nodes_are_bounded_across_the_entire_block() {
    let wide = |n| j!({"call":{"function":"NATIVE_FUNCTION_COALESCE","arguments":vec![field();n]}});
    let mut projection = vec![j!({"expression":wide(63)}); 64]; // 64 * (root + 63) = 4096
    assert!(operation(j!({"readExpressions":{"table":"app","projection":projection}})).is_ok());
    projection[0] = j!({"expression":wide(64)});
    assert!(operation(j!({"readExpressions":{"table":"app","projection":projection}})).is_err());
    let request =
        j!({"readExpressions":{"table":"app","projection":vec![j!({"expression":wide(63)});32]}});
    assert!(decode(j!({"transaction":{"transaction":{"readOnly":true,"steps":[{"execute":request.clone()},{"execute":request.clone()}]}}})).is_ok());
    assert!(decode(j!({"transaction":{"transaction":{"readOnly":true,"steps":[{"execute":request.clone()},{"execute":request}],"emit":{"values":[{"scalar":field()}]}}}})).is_err());
}

#[test]
fn bindings_lists_rows_and_block_item_limits_are_checked_before_conversion() {
    let bindings: Vec<_> = (0..256)
        .map(|n| j!({"name":format!("p{n}"),"value":{"intValue":"0"}}))
        .collect();
    assert!(decode(j!({"operation":{"request":read(),"bindings":bindings}})).is_ok());
    let mut too_many = bindings.clone();
    too_many.push(j!({"name":"extra","value":{"intValue":"0"}}));
    assert!(decode(j!({"operation":{"request":read(),"bindings":too_many}})).is_err());
    assert!(decode(j!({"operation":{"request":read(),"bindings":[bindings[0].clone(),bindings[0].clone()]}})).is_err());
    for size in [65_536, 65_537] {
        assert_eq!(decode(j!({"operation":{"request":read(),"bindings":[{"name":"p","value":{"textValue":"a".repeat(size)}}]}})).is_ok(),size==65_536);
    }
    for n in [64, 65] {
        assert_eq!(
            operation(j!({"read":{"table":"app","projection":vec!["id";n]}})).is_ok(),
            n == 64
        );
    }
    for n in [1024, 1025] {
        assert_eq!(operation(j!({"insertMany":{"table":"app","columns":["id"],"rows":vec![j!({"values":[field()]});n],"cap":{"literal":{"intValue":"1024"}}}})).is_ok(),n==1024);
    }
    assert!(operation(j!({"insertMany":{"table":"app","columns":["id"],"rows":[{"values":[]}],"cap":{"literal":{"intValue":"1"}}}})).is_err());
    for n in [63, 64] {
        assert_eq!(decode(j!({"transaction":{"transaction":{"readOnly":true,"steps":vec![j!({"execute":read()});n],"emit":{"values":[{"scalar":field()}]}}}})).is_ok(),n==63);
    }
}

#[test]
fn strict_native_presence_oneofs_enums_and_wrapper_shape_reject_ambiguity() {
    let bad = [
        j!({"read":{}}),
        j!({"read":{"table":"app","order":[{"field":"id"}]}}),
        j!({"read":{"table":"app","filter":{"isNull":{"expression":field()}}}}),
        j!({"read":{"table":"app","filter":{"compare":{"left":field(),"right":field(),"comparison":"NATIVE_COMPARISON_UNSPECIFIED"}}}}),
        j!({"read":{"table":"app","filter":{"field":"id","value":{"literal":{"intValue":"0"}}}}}),
        j!({"read":{"table":null}}),
        j!({"returning":{"request":read()}}),
        j!({"onConflict":{"request":{"returning":{"request":{"insert":{"table":"app"}}}},"action":{"nothing":{}}}}),
    ];
    for input in bad {
        assert!(operation(input.clone()).is_err(), "accepted {input}");
    }
    for bytes in [
        br#"{"operation":{"request":{"read":{"table":"a","table":"b"}}}}"#.as_slice(),
        br#"{"begin":{"unknown":1}}"#.as_slice(),
        br#"{"transaction":{"transaction":{"read_only":false}}}"#.as_slice(),
    ] {
        assert!(json::decode_execute(bytes).is_err());
    }
    let mut wire =
        json::decode_execute(br#"{"operation":{"request":{"read":{"table":"app"}}}}"#).unwrap();
    let p::execute_request::Request::Operation(op) = wire.request.as_mut().unwrap() else {
        panic!()
    };
    let p::native_request::Request::Read(r) =
        op.request.as_mut().unwrap().request.as_mut().unwrap()
    else {
        panic!()
    };
    r.table = None;
    assert!(codec::decode_execute(wire).is_err());
}

#[test]
fn direct_protobuf_rejects_missing_presence_unknown_enums_and_excess_collections() {
    fn direct(request: p::NativeRequest) -> R<codec::DecodedExecute> {
        let input = p::ExecuteRequest {
            request: Some(p::execute_request::Request::Operation(
                p::NativeOperationRequest {
                    request: Some(request),
                    bindings: vec![],
                },
            )),
            budget_ms: None,
        };
        // This path does not pass through the JSON decoder or its array checks.
        let wire = input.encode_to_vec();
        codec::decode_execute(p::ExecuteRequest::decode(wire.as_slice()).unwrap())
    }
    let read = |filter, order, projection| p::NativeRequest {
        request: Some(p::native_request::Request::Read(p::NativeRead {
            table: Some("app".into()),
            filter,
            order,
            limit: None,
            projection,
        })),
    };
    for (order, projection) in [
        (
            vec![p::NativeSort {
                field: Some("id".into()),
                descending: None,
            }],
            vec![],
        ),
        (vec![], vec!["id".into(); 65]),
    ] {
        assert!(direct(read(None, order, projection)).is_err());
    }
    for enum_value in [0, 7, i32::MAX, -1] {
        let leaf = || {
            Some(Box::new(p::NativeExpr {
                expression: Some(p::native_expr::Expression::Field("id".into())),
            }))
        };
        let filter = p::NativeExpr {
            expression: Some(p::native_expr::Expression::Compare(Box::new(
                p::NativeCompare {
                    left: leaf(),
                    right: leaf(),
                    comparison: enum_value,
                },
            ))),
        };
        assert!(direct(read(Some(filter), vec![], vec![])).is_err());
    }
    let invalid_empty_expr = p::NativeExpr { expression: None };
    assert!(direct(read(Some(invalid_empty_expr), vec![], vec![])).is_err());
    let missing_transaction = p::ExecuteRequest {
        request: Some(p::execute_request::Request::Transaction(
            p::NativeTransactionRequest {
                transaction: Some(p::NativeTransaction {
                    read_only: None,
                    steps: vec![],
                    emit: None,
                }),
                bindings: vec![],
            },
        )),
        budget_ms: None,
    };
    assert!(codec::decode_execute(missing_transaction).is_err());
    let binding = p::NativeBinding {
        name: Some("same".into()),
        value: Some(codec::encode_value(o::Value::Int(1))),
    };
    let duplicates = p::ExecuteRequest {
        request: Some(p::execute_request::Request::Operation(
            p::NativeOperationRequest {
                request: Some(read(None, vec![], vec![])),
                bindings: vec![binding.clone(), binding],
            },
        )),
        budget_ms: None,
    };
    assert!(codec::decode_execute(duplicates).is_err());
}

#[test]
fn transaction_preserves_present_empty_emit_and_all_step_and_emit_arms() {
    let codec::Invocation::Transaction {
        transaction: without,
        ..
    } = decode(j!({"transaction":{"transaction":{"readOnly":false}}})).unwrap()
    else {
        panic!()
    };
    let codec::Invocation::Transaction {
        transaction: empty, ..
    } = decode(j!({"transaction":{"transaction":{"readOnly":false,"emit":{}}}})).unwrap()
    else {
        panic!()
    };
    assert_eq!(without.emit, None);
    assert_eq!(empty.emit, Some(vec![]));
    let codec::Invocation::Transaction {transaction,..}=decode(j!({"transaction":{"transaction":{"readOnly":false,"steps":[{"execute":read()},{"letBinding":{"name":"r","request":read()}},{"require":field()}],"emit":{"values":[{"scalar":field()},{"row":"r"},{"rows":"r"}]}}}})).unwrap() else {panic!()};
    assert!(
        matches!(&transaction.steps[..],[o::TransactionStep::Execute(_),o::TransactionStep::Let{name,..},o::TransactionStep::Require(_)] if name=="r")
    );
    assert_eq!(
        transaction.emit,
        Some(vec![
            o::EmitExpr::Scalar(o::Expr::Field("id".into())),
            o::EmitExpr::Row("r".into()),
            o::EmitExpr::Rows("r".into())
        ])
    );
}

#[test]
fn all_eighteen_native_errors_are_fixed_and_redact_internal_details() {
    let errors = [
        o::Error::Syntax { offset: usize::MAX },
        o::Error::Unsupported,
        o::Error::Limit,
        o::Error::Bind("PRIVATE_BIND".into()),
        o::Error::Type,
        o::Error::NotFound,
        o::Error::AmbiguousMatch,
        o::Error::ConditionFailed,
        o::Error::TooManyMatches,
        o::Error::ClaimNotConsumed,
        o::Error::Conflict,
        o::Error::Core("PRIVATE_CORE".into()),
        o::Error::Unknown,
        o::Error::Overflow,
        o::Error::DivisionByZero,
        o::Error::RequirementFailed,
        o::Error::DuplicateTarget,
        o::Error::PlanRequired,
    ];
    for (index, error) in errors.into_iter().enumerate() {
        let v = encode_error(error);
        assert_eq!(v.kind, index as i32 + 1);
        let json = serde_json::to_value(v).unwrap();
        assert!(!json.to_string().contains("PRIVATE"));
        assert_eq!(json.get("syntaxOffset").is_some(), index == 0);
        assert!(
            json["kind"]
                .as_str()
                .unwrap()
                .starts_with("NATIVE_ERROR_CLASS_")
        );
    }
}

#[test]
fn native_results_preserve_counts_versions_types_conflicts_and_empty_collections() {
    let conflict = || o::ConflictTupleOutcome {
        input_index: usize::MAX,
        status: o::ConflictStatus::Skipped,
        changed: false,
        returned_row: Some(0),
    };
    let row = || o::SqlRow {
        values: vec![
            o::Value::Int(i64::MIN),
            o::Value::Null,
            o::Value::Bytes(vec![0, 255]),
        ],
        version: u64::MAX,
    };
    let outcome = encode_outcome(o::Outcome {
        columns: vec!["id".into()],
        rows: vec![row()],
        matched: usize::MAX,
        affected: 1,
        changed: 0,
        complete: false,
        conflict_results: Some(vec![conflict()]),
    })
    .unwrap();
    let v = serde_json::to_value(&outcome).unwrap();
    assert_eq!(v["matched"], usize::MAX.to_string());
    assert_eq!(v["rows"][0]["version"], u64::MAX.to_string());
    assert_eq!(v["rows"][0]["values"][0]["intValue"], i64::MIN.to_string());
    assert_eq!(v["rows"][0]["values"][2]["bytesValue"], "AP8=");
    assert_eq!(v["conflictResults"]["items"][0]["returnedRow"], "0");
    let types = [
        DataType::Boolean,
        DataType::SmallInt,
        DataType::Integer,
        DataType::BigInt,
        DataType::Text,
        DataType::Bytes,
    ];
    let columns: Vec<_> = types
        .into_iter()
        .enumerate()
        .map(|(n, data_type)| ColumnDef {
            id: n as u64,
            name: format!("c{n}"),
            data_type,
            not_null: n == 0,
        })
        .collect();
    let outcome = encode_transaction_outcome(o::TransactionOutcome {
        summaries: vec![o::StepSummary {
            item_index: 2,
            matched: 1,
            affected: 1,
            changed: 0,
            complete: true,
            conflict_results: Some(vec![conflict()]),
        }],
        emitted: vec![
            o::EmittedValue::Scalar(o::Value::Bool(false)),
            o::EmittedValue::Row {
                columns: columns.clone(),
                row: row(),
            },
            o::EmittedValue::Rows {
                columns,
                rows: vec![],
                complete: true,
            },
        ],
    })
    .unwrap();
    let v = serde_json::to_value(outcome).unwrap();
    assert_eq!(v["summaries"][0]["itemIndex"], "2");
    assert_eq!(v["emitted"][0]["scalar"]["boolValue"], false);
    assert_eq!(v["emitted"][2]["rows"]["rows"], j!([]));
    for (n, name) in [
        "BOOLEAN",
        "SMALL_INT",
        "INTEGER",
        "BIG_INT",
        "TEXT",
        "BYTES",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            v["emitted"][1]["row"]["columns"][n]["dataType"],
            format!("NATIVE_DATA_TYPE_{name}")
        );
    }
}
