mod common;
use giggles_core::{Admin, Limits, Engine as Backend, operations::*, relational::Value};
fn field(s: &str) -> Expr { Expr::Field(s.into()) }
fn int(n: i64) -> Expr { Expr::Value(ValueExpr::Literal(Value::Int(n))) }
fn cmp(a: Expr, op: Compare, b: Expr) -> Expr { Expr::Compare(Box::new(a), op, Box::new(b)) }
fn arithmetic(a: Expr, op: Arithmetic, b: Expr) -> Expr { Expr::Arithmetic(Box::new(a), op, Box::new(b)) }
fn cases() -> Vec<(Expr, &'static str)> {
    let less = || cmp(field("a"), Compare::Lt, field("b"));
    let positive = || cmp(field("a"), Compare::Gt, int(0));
    vec![
        (less(), "a < b"),
        (cmp(field("a"), Compare::Gt, int(-5)), "a > -5"),
        (Expr::And(Box::new(less()), Box::new(Expr::And(Box::new(positive()), Box::new(less())))), "a < b AND (a > 0 AND a < b)"),
        (cmp(arithmetic(field("a"), Arithmetic::Add, int(1)), Compare::Lt, field("b")), "a + 1 < b"),
        (Expr::And(Box::new(less()), Box::new(positive())), "a < b AND a > 0"),
        (Expr::Or(Box::new(less()), Box::new(positive())), "a < b OR a > 0"),
        (Expr::Not(Box::new(cmp(field("a"), Compare::Ge, field("b")))), "NOT a >= b"),
        (Expr::IsNull(Box::new(field("a")), true), "a IS NOT NULL"),
        (cmp(Expr::Negate(Box::new(field("a"))), Compare::Lt, int(0)), "-a < 0"),
        (cmp(Expr::Call { function: ScalarFunction::Coalesce, arguments: vec![field("a"), int(0)] }, Compare::Gt, int(0)), "coalesce(a, 0) > 0"),
        (Expr::Case { operand: None, arms: vec![(less(), positive())], else_expr: Some(Box::new(Expr::Value(ValueExpr::Literal(Value::Bool(false))))) }, "CASE WHEN a < b THEN a > 0 ELSE false END"),
        (cmp(field("b"), Compare::Gt, arithmetic(int(5), Arithmetic::Subtract, arithmetic(field("a"), Arithmetic::Subtract, int(1)))), "b > 5 - (a - 1)"),
        (Expr::And(Box::new(Expr::Or(Box::new(less()), Box::new(positive()))), Box::new(Expr::IsNull(Box::new(field("a")), true))), "(a < b OR a > 0) AND a IS NOT NULL"),
    ]
}
fn fixture(backend: Backend) -> Admin {
    let admin = Admin::create_engine(common::fresh("parity-native-check"), Limits::default(), backend).unwrap();
    admin.sql_session().execute("CREATE TABLE t(id BIGINT PRIMARY KEY,a BIGINT,b BIGINT)", &[], None).unwrap();
    admin
}
fn request(expression: Expr) -> CatalogRequest { CatalogRequest::AddConstraint { table: "t".into(), name: Some("rule".into()), constraint: ConstraintSpec::Check { expression } } }
fn same(left: &Admin, right: &Admin) {
    assert_eq!(left.catalog().unwrap().1, right.catalog().unwrap().1);
    let (mut a, mut b) = (left.catalog_presentation().unwrap(), right.catalog_presentation().unwrap());
    a.epoch = 0; b.epoch = 0;
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}
#[test]
fn recursive_checks_match_sql_catalog_and_enforcement() {
    let mut backends = vec![Backend::Legacy];
    if cfg!(feature = "engine-rocksdb") { backends.push(Backend::V2); }
    for backend in backends { for (expression, text) in cases() {
        let (native, sql) = (fixture(backend), fixture(backend));
        Engine::new(native.clone()).execute_catalog(&request(expression)).unwrap();
        sql.sql_session().execute(&format!("ALTER TABLE t ADD CONSTRAINT rule CHECK ({text})"), &[], None).unwrap();
        same(&native, &sql);
        for admin in [&native, &sql] {
            admin.sql_session().execute("INSERT INTO t VALUES (1, 1, 10)", &[], None).unwrap();
            let bad = if text == "a IS NOT NULL" { "NULL" } else { "-10" };
            let error = admin.sql_session().execute(&format!("INSERT INTO t VALUES (2, {bad}, -20)"), &[], None).unwrap_err();
            assert_eq!(giggles_core::sqlstate(&error), "23514", "{text}");
        }
    } }
}
#[test]
fn populated_v2_checks_validate_existing_rows() {
    if !cfg!(feature = "engine-rocksdb") { return; }
    for (expression, text) in cases() {
        let (native, sql) = (fixture(Backend::V2), fixture(Backend::V2));
        let bad = if text == "a IS NOT NULL" { "NULL" } else { "-10" };
        for admin in [&native, &sql] { admin.sql_session().execute(&format!("INSERT INTO t VALUES (1, {bad}, -20)"), &[], None).unwrap(); }
        assert_eq!(Engine::new(native.clone()).execute_catalog(&request(expression)).unwrap_err().sqlstate(), Some("23514"), "{text}");
        assert_eq!(giggles_core::sqlstate(&sql.sql_session().execute(&format!("ALTER TABLE t ADD CONSTRAINT rule CHECK ({text})"), &[], None).unwrap_err()), "23514");
        same(&native, &sql);
    }
}
#[test]
fn sql_validation_and_native_binding_rejections_remain_authoritative() {
    let admin = fixture(Backend::Legacy);
    let engine = Engine::new(admin.clone());
    let simple = Expr::Case { operand: Some(Box::new(field("a"))), arms: vec![(int(1), int(1))], else_expr: Some(Box::new(int(0))) };
    let expected = giggles_core::sqlstate(&admin.sql_session().execute("ALTER TABLE t ADD CHECK (CASE a WHEN 1 THEN 1 ELSE 0 END)", &[], None).unwrap_err());
    assert_eq!(engine.execute_catalog(&request(simple)).unwrap_err().sqlstate(), Some(expected));
    for expression in [Expr::Local(LocalRef::Count { local: "x".into() }), Expr::Value(ValueExpr::Parameter("p".into())), Expr::ConflictField { source: ConflictSource::Incoming, field: "a".into() }] {
        assert_eq!(engine.execute_catalog(&request(expression)), Err(Error::Unsupported));
    }
}
