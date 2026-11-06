mod common;
use giggles_core::{Admin, Limits, operations::*, relational::Value};

fn fixture() -> (Admin, Engine, Request) {
    let admin = Admin::create(
        common::fresh("read-explain"),
        Limits {
            admitted: 7,
            selected_keys: 19,
        },
    )
    .unwrap();
    admin
        .sql_session()
        .execute(
            "create table items (id bigint primary key, secret text)",
            &[],
            None,
        )
        .unwrap();
    let engine = Engine::new(admin.clone());
    let request = Request::Read {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    };
    (admin, engine, request)
}

#[test]
fn truthful_plan_and_current_catalog_without_publication() {
    let (admin, engine, request) = fixture();
    let before = engine
        .inspect(&InspectionRequest::Table {
            table: "items".into(),
        })
        .unwrap();
    let plan = engine.explain_read(&request, &Bindings::new()).unwrap();
    let InspectionResult::Table(table) = &before.result else {
        panic!()
    };
    assert_eq!(plan.database_domain, before.database_domain);
    assert_eq!(plan.catalog_epoch, before.catalog_epoch);
    assert_eq!(plan.table_id, table.id);
    assert_eq!(
        plan.projection_column_ids,
        table.columns.iter().map(|c| c.id).collect::<Vec<_>>()
    );
    assert_eq!(
        (plan.limits.input_rows, plan.limits.output_rows),
        (1024, 100)
    );
    assert_eq!((plan.limits.admitted, plan.limits.selected_keys), (7, 19));
    assert_eq!(plan.estimated_rows, None);
    assert_eq!(plan.estimated_cost, None);
    assert!(
        plan.stages
            .contains(&ReadExplainStage::ReversePrimaryKeyCompletenessCertification)
    );
    assert_eq!(
        before,
        engine
            .inspect(&InspectionRequest::Table {
                table: "items".into()
            })
            .unwrap()
    );
    admin
        .sql_session()
        .execute("create table other (id bigint primary key)", &[], None)
        .unwrap();
    assert!(
        engine
            .explain_read(&request, &Bindings::new())
            .unwrap()
            .catalog_epoch
            > plan.catalog_epoch
    );
}

#[test]
fn actual_bindings_are_checked_and_redacted() {
    let (_, engine, mut request) = fixture();
    let Request::Read {
        filter,
        limit,
        order,
        projection,
        ..
    } = &mut request
    else {
        panic!()
    };
    *filter = Some(Expr::Compare(
        Box::new(Expr::Field("secret".into())),
        Compare::Eq,
        Box::new(Expr::Value(ValueExpr::Parameter("value".into()))),
    ));
    *limit = Some(ValueExpr::Parameter("cap".into()));
    order.push(Sort {
        field: "id".into(),
        descending: true,
    });
    projection.push("secret".into());
    let mut bindings = Bindings::from([
        ("value".into(), Value::Text("private-payload".into())),
        ("cap".into(), Value::Int(1024)),
    ]);
    let plan = engine.explain_read(&request, &bindings).unwrap();
    assert_eq!(plan.limits.output_rows, 1024);
    assert!(plan.has_filter);
    assert!(plan.order[0].descending);
    let json = serde_json::to_string(&plan).unwrap();
    assert!(!json.contains("private-payload"));
    assert!(!format!("{plan:?}").contains("private-payload"));
    assert_eq!(
        serde_json::from_str::<ReadExplainOutcome>(&json).unwrap(),
        plan
    );
    bindings.insert("cap".into(), Value::Int(1025));
    assert_eq!(engine.explain_read(&request, &bindings), Err(Error::Type));
    bindings.insert("cap".into(), Value::Int(1));
    bindings.insert("value".into(), Value::Bool(true));
    assert_eq!(engine.explain_read(&request, &bindings), Err(Error::Type));
    assert!(engine.explain_read(&request, &Bindings::new()).is_err());
}

#[test]
fn unsupported_rejected_before_request_binding() {
    let (_, engine, _) = fixture();
    let request = Request::Insert {
        table: "missing".into(),
        values: vec![],
    };
    assert_eq!(
        engine.explain_read(&request, &Bindings::new()),
        Err(Error::Unsupported)
    );
    let request = Request::ReadExpressions {
        table: "missing".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    };
    assert_eq!(
        engine.explain_read(&request, &Bindings::new()),
        Err(Error::Unsupported)
    );
}

#[test]
fn domain_is_canonical_decimal_string() {
    let (_, engine, request) = fixture();
    let mut plan = engine.explain_read(&request, &Bindings::new()).unwrap();
    plan.database_domain = u128::MAX;
    let mut json = serde_json::to_value(&plan).unwrap();
    assert_eq!(json["database_domain"], u128::MAX.to_string());
    assert_eq!(
        serde_json::from_value::<ReadExplainOutcome>(json.clone()).unwrap(),
        plan
    );
    for bad in [
        "",
        "01",
        "+1",
        " 1",
        "340282366920938463463374607431768211456",
    ] {
        json["database_domain"] = bad.into();
        assert!(serde_json::from_value::<ReadExplainOutcome>(json.clone()).is_err());
    }
    json["database_domain"] = serde_json::json!(1);
    assert!(serde_json::from_value::<ReadExplainOutcome>(json).is_err());
}
