//! Public contract checks use independent schema vectors, generated descriptors,
//! and the same published HTTP examples available to API consumers.
use giggles_server::api::{codec, json, proto};
use prost::Message;

#[test]
fn descriptor_publishes_v1_methods_and_result_stream() {
    let descriptors = prost_types::FileDescriptorSet::decode(proto::FILE_DESCRIPTOR_SET).unwrap();
    let file = descriptors
        .file
        .iter()
        .find(|f| f.package.as_deref() == Some("giggles.v1"))
        .unwrap();
    assert_eq!(file.syntax.as_deref(), Some("proto3"));
    assert_eq!(file.service.len(), 1);
    let service = &file.service[0];
    assert_eq!(service.name.as_deref(), Some("Giggles"));
    assert_eq!(
        service
            .method
            .iter()
            .map(|m| m.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["OpenSession", "Execute", "ExecuteStream", "CloseSession"]
    );
    assert!(
        service
            .method
            .iter()
            .all(|m| !m.client_streaming.unwrap_or(false)
                && m.server_streaming.unwrap_or(false) == (m.name.as_deref() == Some("ExecuteStream")))
    );
    let request = file
        .message_type
        .iter()
        .find(|m| m.name.as_deref() == Some("ExecuteRequest"))
        .unwrap();
    assert_eq!(
        request
            .field
            .iter()
            .map(|f| (f.name.as_deref().unwrap(), f.number.unwrap()))
            .collect::<Vec<_>>(),
        [
            ("ping", 1),
            ("sql", 2),
            ("preview", 3),
            ("execute", 4),
            ("repair_sql", 5),
            ("status", 6),
            ("operation", 7),
            ("transaction", 8),
            ("begin", 9),
            ("commit", 10),
            ("rollback", 11),
            ("text", 12),
            ("typed_read", 13),
            ("search_path", 14),
            ("savepoint", 15),
            ("rollback_to", 16),
            ("release", 17),
            ("catalog", 18),
            ("sequence", 19),
            ("budget_ms", 20)
        ]
    );
    let budget = request
        .field
        .iter()
        .find(|f| f.name.as_deref() == Some("budget_ms"))
        .unwrap();
    assert_eq!(budget.proto3_optional, Some(true));
    for (message, fields) in [
        ("NativeSavepointRequest", vec!["name"]),
        ("TaskRequest", vec!["operation"]),
        ("RepairSqlRequest", vec!["operation", "approval"]),
        ("StatusRequest", vec!["operation"]),
        ("Key", vec!["tenant", "row"]),
        ("Predicate", vec!["tenant", "from", "until"]),
    ] {
        let descriptor = file
            .message_type
            .iter()
            .find(|m| m.name.as_deref() == Some(message))
            .unwrap();
        for name in fields {
            let field = descriptor
                .field
                .iter()
                .find(|f| f.name.as_deref() == Some(name))
                .unwrap();
            assert_eq!(field.proto3_optional, Some(true), "{message}.{name}");
        }
    }
}

#[test]
fn published_http_examples_pass_the_runtime_codec() {
    let api: serde_json::Value =
        serde_json::from_str(include_str!("../docs/http-api.openapi.json")).unwrap();
    assert_eq!(api["openapi"], "3.1.1");
    let examples = api["paths"]["/v1/session/execute"]["post"]["requestBody"]["content"]["application/json"]["examples"].as_object().unwrap();
    assert!(examples.len() >= 12);
    for (name, example) in examples {
        let wire = serde_json::to_vec(&example["value"]).unwrap();
        let request = json::decode_execute(&wire).unwrap_or_else(|error| panic!("{name}: {error}"));
        codec::decode_execute(request).unwrap_or_else(|error| panic!("{name}: {error}"));
    }
    json::decode_empty(b"{}").unwrap();
    assert_eq!(
        api["components"]["securitySchemes"]["mutualTLS"]["type"],
        "mutualTLS"
    );
    assert_eq!(
        api["components"]["securitySchemes"]["sessionHandle"]["name"],
        "x-giggles-session"
    );
}

#[test]
fn published_proto_can_construct_the_generated_client() {
    fn accepts_generated_client(
        _: Option<proto::giggles_client::GigglesClient<tonic::transport::Channel>>,
    ) {
    }
    accepts_generated_client(None);
    let schema = include_str!("../proto/giggles/v1/giggles.proto");
    assert!(schema.contains("package giggles.v1;"));
    assert!(schema.contains("optional uint32 budget_ms = 20;"));
    assert!(!schema.contains("rpc Begin"));
}

#[test]
fn native_descriptor_has_complete_typed_arms_and_explicit_presence() {
    let descriptors = prost_types::FileDescriptorSet::decode(proto::FILE_DESCRIPTOR_SET).unwrap();
    let file = descriptors
        .file
        .iter()
        .find(|f| f.package.as_deref() == Some("giggles.v1"))
        .unwrap();
    for (message, names) in [
        (
            "NativeRequest",
            vec![
                "read",
                "insert",
                "update",
                "delete",
                "update_expressions",
                "read_expressions",
                "returning",
                "insert_expressions",
                "insert_many",
                "on_conflict",
            ],
        ),
        (
            "NativeExpr",
            vec![
                "value",
                "field",
                "compare",
                "is_null",
                "not_expr",
                "and_expr",
                "or_expr",
                "negate",
                "arithmetic",
                "local",
                "call",
                "case_expr",
                "conflict_field",
            ],
        ),
        (
            "NativeTransactionStep",
            vec!["execute", "let_binding", "require"],
        ),
        ("NativeEmitExpr", vec!["scalar", "row", "rows"]),
        ("NativeError", vec!["kind", "syntax_offset", "sqlstate"]),
    ] {
        let descriptor = file
            .message_type
            .iter()
            .find(|m| m.name.as_deref() == Some(message))
            .unwrap();
        assert_eq!(descriptor.field.len(), names.len());
        for (index, name) in names.iter().enumerate() {
            let field = &descriptor.field[index];
            assert_eq!(field.name.as_deref(), Some(*name));
            assert_eq!(field.number, Some(index as i32 + 1));
        }
    }
    for (message, field) in [
        ("NativeRead", "table"),
        ("NativeBinding", "name"),
        ("NativeSort", "descending"),
        ("NativeIsNull", "negated"),
        ("NativeTransaction", "read_only"),
        ("NativeError", "sqlstate"),
    ] {
        let message = file
            .message_type
            .iter()
            .find(|m| m.name.as_deref() == Some(message))
            .unwrap();
        let field = message
            .field
            .iter()
            .find(|f| f.name.as_deref() == Some(field))
            .unwrap();
        assert_eq!(field.proto3_optional, Some(true));
    }
    let error = file
        .enum_type
        .iter()
        .find(|e| e.name.as_deref() == Some("NativeErrorClass"))
        .unwrap();
    assert_eq!(error.value.len(), 21); // sentinel plus twenty stable core classes
    assert_eq!(
        error.value.last().unwrap().name.as_deref(),
        Some("NATIVE_ERROR_CLASS_SQL")
    );
    assert_eq!(error.value[19].name.as_deref(), Some("NATIVE_ERROR_CLASS_DUPLICATE"));
    assert_eq!(error.value[19].number, Some(19));
    assert_eq!(error.value[20].number, Some(20));
    let reply = file
        .message_type
        .iter()
        .find(|m| m.name.as_deref() == Some("ExecuteReply"))
        .unwrap();
    for (name, number) in [
        ("operation_outcome", 8),
        ("transaction_outcome", 9),
        ("native_error", 10),
        ("catalog_outcome", 11),
    ] {
        let field = reply
            .field
            .iter()
            .find(|f| f.name.as_deref() == Some(name))
            .unwrap();
        assert_eq!(field.number, Some(number));
        assert_eq!(
            reply.oneof_decl[field.oneof_index.unwrap() as usize]
                .name
                .as_deref(),
            Some("native_result")
        );
    }
}

#[test]
fn catalog_descriptor_frozen_fields_and_presence() {
    let set = prost_types::FileDescriptorSet::decode(proto::FILE_DESCRIPTOR_SET).unwrap();
    let file = set.file.iter().find(|f| f.package.as_deref() == Some("giggles.v1")).unwrap();
    for (message, fields, optional, oneof) in [
        ("NativeCatalogRequest", "create_table create_index drop_table drop_index alter_table add_constraint drop_constraint create_view drop_view create_sequence drop_sequence", "", Some("request")),
        ("NativeCreateSequence", "name", "name", None),
        ("NativeDropSequence", "name cascade", "name cascade", None),
        ("NativeSequenceCall", "nextval currval setval", "", Some("call")),
        ("NativeSequenceName", "name", "name", None),
        ("NativeSetval", "name value is_called", "name value is_called", None),
        ("NativeCreateView", "name read", "name", None),
        ("NativeDropView", "name cascade", "name cascade", None),
        ("NativeAddConstraint", "table name unique check foreign_key", "table name", Some("constraint")),
        ("NativeUniqueConstraint", "columns", "", None),
        ("NativeCheckConstraint", "expression", "", None),
        ("NativeForeignKey", "columns ref_table ref_columns ref_columns_set on_delete on_update", "ref_table ref_columns_set on_delete on_update", None),
        ("NativeDropConstraint", "table name cascade", "table name cascade", None),
        ("NativeCatalogColumn", "name data_type not_null default generated", "name data_type not_null generated", None),
        ("NativeColumnDefault", "value clock nextval", "", Some("kind")),
        ("NativeClockDefault", "kind precision", "kind precision", None),
        ("NativeCreateTable", "table columns primary_key", "table", None),
        ("NativeCreateIndex", "table name columns", "table name", None),
        ("NativeDropTable", "table cascade", "table cascade", None),
        ("NativeDropIndex", "name cascade", "name cascade", None),
        ("NativeAlterTable", "table add_column drop_column rename_column rename_table", "table", Some("action")),
        ("NativeDropColumn", "name cascade", "name cascade", None),
        ("NativeRenameColumn", "from to", "from to", None),
        ("NativeRenameTable", "to", "to", None),
        ("NativeCatalogOutcome", "catalog_epoch table columns primary_key index_id", "index_id", None),
    ] {
        let m = file.message_type.iter().find(|m| m.name.as_deref() == Some(message)).unwrap();
        assert_eq!(m.field.len(), fields.split_whitespace().count());
        for (i, name) in fields.split_whitespace().enumerate() {
            let f = &m.field[i]; assert_eq!(f.name.as_deref(), Some(name)); assert_eq!(f.number, Some(i as i32 + 1));
            assert_eq!(f.proto3_optional.unwrap_or(false), optional.split_whitespace().any(|n| n == name));
            if let Some(oneof) = oneof.filter(|_| !optional.split_whitespace().any(|n| n == name)) { assert_eq!(m.oneof_decl[f.oneof_index.unwrap() as usize].name.as_deref(), Some(oneof)); }
        }
    }
    for (message, field, number) in [("NativeOperationRequest", "page", 3), ("NativeOutcome", "next_page", 8), ("NativePage", "after", 1)] {
        let m = file.message_type.iter().find(|m| m.name.as_deref() == Some(message)).unwrap();
        let f = m.field.iter().find(|f| f.name.as_deref() == Some(field)).unwrap();
        assert_eq!(f.number, Some(number), "{message}.{field}");
    }
    let api: serde_json::Value = serde_json::from_str(include_str!("../docs/http-api.openapi.json")).unwrap();
    let s = &api["components"]["schemas"];
    assert_eq!(s["NativeOperationRequest"]["properties"]["page"]["$ref"], "#/components/schemas/NativePage");
    assert_eq!(s["NativeOutcome"]["properties"]["nextPage"]["$ref"], "#/components/schemas/NativePage");
    assert_eq!(s["NativePage"]["properties"].as_object().unwrap().keys().collect::<Vec<_>>(), ["after"]);
    let m = file.message_type.iter().find(|m| m.name.as_deref() == Some("ExecuteRequest")).unwrap();
    let f = m.field.iter().find(|f| f.name.as_deref() == Some("sequence")).unwrap();
    assert_eq!(f.number, Some(19));
    assert_eq!(m.oneof_decl[f.oneof_index.unwrap() as usize].name.as_deref(), Some("request"));
    for (message, keys) in [("NativeCreateSequence", "name"), ("NativeDropSequence", "name cascade"), ("NativeSequenceCall", "nextval currval setval"),
        ("NativeSequenceName", "name"), ("NativeSetval", "name value isCalled")] {
        let props = s[message]["properties"].as_object().unwrap();
        assert_eq!(props.len(), keys.split_whitespace().count(), "{message}");
        for key in keys.split_whitespace() { assert!(props.contains_key(key), "{message}.{key}"); }
    }
    assert_eq!(api["components"]["schemas"]["ExecuteRequest"]["properties"]["sequence"]["$ref"], "#/components/schemas/NativeSequenceCall");
    let m = file.message_type.iter().find(|m| m.name.as_deref() == Some("ExecuteRequest")).unwrap();
    let f = m.field.iter().find(|f| f.name.as_deref() == Some("catalog")).unwrap();
    assert_eq!(m.oneof_decl[f.oneof_index.unwrap() as usize].name.as_deref(), Some("request"));
}

#[test]
fn catalog_default_json_schema_contract() {
    let schema: serde_json::Value = serde_json::from_str(include_str!("../docs/http-api.openapi.json")).unwrap();
    let s = &schema["components"]["schemas"];
    assert_eq!(s["NativeCatalogColumn"]["properties"]["default"]["$ref"], "#/components/schemas/NativeColumnDefault");
    assert_eq!(s["NativeCatalogColumn"]["properties"]["generated"]["enum"], serde_json::json!(["IdentityAlways", "IdentityByDefault", "Serial"]));
    for (i, name) in ["Value", "Nextval", "Clock"].into_iter().enumerate() {
        assert_eq!(s["NativeColumnDefault"]["oneOf"][i]["required"], serde_json::json!([name]));
    }
    assert_eq!(s["NativeClockDefault"]["oneOf"][1]["properties"]["precision"]["maximum"], u32::MAX);
    assert!(s["NativeAlterTable"]["properties"]["addColumn"]["allOf"][1]["not"].is_object());
    assert!(s["NativeCatalogOutcome"]["properties"]["columns"]["items"]["allOf"][1]["not"].is_object());
}

#[test]
fn constraint_json_keys_and_malformed_shapes() {
    use serde_json::json;
    for body in [json!({"table":"t","unique":{"columns":["v"]}}),
        json!({"table":"t","name":"ck","check":{"expression":{"field":"v"}}}),
        json!({"table":"t","name":"fk","foreignKey":{"columns":["v"],"refTable":"p","refColumns":["id"],"refColumnsSet":true,"onDelete":"Cascade","onUpdate":"Restrict"}})] {
        let bytes=serde_json::to_vec(&json!({"catalog":{"addConstraint":body}})).unwrap();
        codec::decode_execute(json::decode_execute(&bytes).unwrap()).unwrap();
    }
    let api:serde_json::Value=serde_json::from_str(include_str!("../docs/http-api.openapi.json")).unwrap();
    for (message,keys) in [("NativeAddConstraint","table name unique check foreignKey"),("NativeUniqueConstraint","columns"),("NativeCheckConstraint","expression"),("NativeForeignKey","columns refTable refColumns refColumnsSet onDelete onUpdate"),("NativeDropConstraint","table name cascade"),("NativeCreateView","name read"),("NativeDropView","name cascade")] {
        let props=api["components"]["schemas"][message]["properties"].as_object().unwrap();
        assert_eq!(props.len(),keys.split_whitespace().count()); for key in keys.split_whitespace() {assert!(props.contains_key(key));}
    }
    for body in [json!({"table":"t"}),json!({"table":"","unique":{}}),json!({"table":"t","name":"","unique":{}}),
        json!({"table":"t","foreignKey":{"refTable":"p","onDelete":"cascade"}}),json!({"table":"t","unique":{"unknown":1}})] {
        let bytes=serde_json::to_vec(&json!({"catalog":{"addConstraint":body}})).unwrap();
        assert!(json::decode_execute(&bytes).and_then(codec::decode_execute).is_err());
    }
}

#[test]
fn foreign_key_presence_and_action_decoding() {
    use serde_json::json;
    use giggles_core::operations::{CatalogRequest,ConstraintSpec,ForeignKeyAction as A};
    for (action,expected) in [("NoAction",A::NoAction),("Restrict",A::Restrict),("Cascade",A::Cascade),("SetNull",A::SetNull),("SetDefault",A::SetDefault)] {
        for set in [None,Some(false),Some(true)] {
            let mut body=json!({"columns":["v"],"refTable":"p","refColumns":["id"],"onDelete":action,"onUpdate":action});
            if let Some(set)=set {body["refColumnsSet"]=json!(set);}
            let input=json!({"catalog":{"addConstraint":{"table":"t","foreignKey":body}}});
            let decoded=codec::decode_execute(json::decode_execute(&serde_json::to_vec(&input).unwrap()).unwrap()).unwrap();
            let codec::Invocation::Catalog(request)=decoded.request else {panic!()};
            let CatalogRequest::AddConstraint {name,constraint:ConstraintSpec::ForeignKey{ref_columns,on_delete,on_update,..},..}=*request else {panic!()};
            assert!(name.is_none()); assert_eq!(on_delete,expected); assert_eq!(on_update,expected);
            assert_eq!(ref_columns,set.filter(|s|*s).map(|_|vec!["id".to_owned()]));
        }
    }
}

#[test]
fn constraint_check_depth_is_bounded_before_encoding() {
    let mut e=proto::NativeExpr {expression:Some(proto::native_expr::Expression::Field("v".into()))};
    for _ in 0..26 {e=proto::NativeExpr {expression:Some(proto::native_expr::Expression::NotExpr(Box::new(e)))};}
    let request=proto::ExecuteRequest {request:Some(proto::execute_request::Request::Catalog(proto::NativeCatalogRequest {request:Some(proto::native_catalog_request::Request::AddConstraint(proto::NativeAddConstraint {table:Some("t".into()),constraint:Some(proto::native_add_constraint::Constraint::Check(proto::NativeCheckConstraint {expression:Some(e)})),..Default::default()}))})),..Default::default()};
    assert!(codec::decode_execute(request).is_err());
}
