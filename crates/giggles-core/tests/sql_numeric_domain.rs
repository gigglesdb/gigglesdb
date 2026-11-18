//! PostgreSQL 18.6 oracle primary messages and SQLSTATEs.
mod common;
use common::fresh;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn numeric_domain_diagnostics() {
    let admin = Admin::create(fresh("numeric-domain"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for (expr, code, message) in [
        ("sqrt(-1::numeric)", "2201F", "cannot take square root of a negative number"),
        ("sqrt(-0.5::numeric)", "2201F", "cannot take square root of a negative number"),
        ("sqrt('-Infinity'::numeric)", "2201F", "cannot take square root of a negative number"),
        ("ln(0::numeric)", "2201E", "cannot take logarithm of zero"),
        ("ln(-1::numeric)", "2201E", "cannot take logarithm of a negative number"),
        ("log(-1::numeric)", "2201E", "cannot take logarithm of a negative number"),
        ("log(10::numeric,-1::numeric)", "2201E", "cannot take logarithm of a negative number"),
        ("log(0::numeric,10::numeric)", "2201E", "cannot take logarithm of zero"),
        ("log(0::numeric,'-Infinity'::numeric)", "2201E", "cannot take logarithm of a negative number"),
        ("log('-0'::numeric,'-Infinity'::numeric)", "2201E", "cannot take logarithm of a negative number"),
        ("log(0::numeric,-1::numeric)", "2201E", "cannot take logarithm of zero"),
        ("log(-1::numeric,0::numeric)", "2201E", "cannot take logarithm of a negative number"),
        ("power(0::numeric,-1::numeric)", "2201F", "zero raised to a negative power is undefined"),
        ("power(-1::numeric,0.5::numeric)", "2201F", "a negative number raised to a non-integer power yields a complex result"),
    ] {
        let error = s.execute(&format!("SELECT {expr}"), &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), code, "{expr}");
        assert_eq!(take_sql_error_message().as_deref(), Some(message), "{expr}");
    }
    let result=s.execute("SELECT sqrt('NaN'::numeric)", &[], None).unwrap();
    assert_eq!(result.rows[0].values, vec![Value::Numeric(giggles_pg_types::Numeric::parse("NaN").unwrap())]);
}
