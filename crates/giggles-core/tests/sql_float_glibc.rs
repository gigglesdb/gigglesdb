//! PostgreSQL 18.6 postgres:18.6 oracle; exact float8send bits.
mod common;
use common::fresh;
use giggles_core::{Admin, CoreError, Limits, relational::*};
#[test]
fn image_bits() {
    let admin = Admin::create(fresh("glibc-bits"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE float_input (id bigint primary key)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO float_input VALUES (1)", &[], None)
        .unwrap();
    for (expr, expected) in [
        ("cbrt('-1000'::float8)", 0xc024000000000000_u64),
        ("cbrt('-100'::float8)", 0xc01290fca9c761f7_u64),
        ("cbrt('-27'::float8)", 0xc008000000000001_u64),
        ("cbrt('-10'::float8)", 0xc0013c484138704e_u64),
        ("cbrt('-3.141592653589793'::float8)", 0xbff76ef7e73104b7_u64),
        ("cbrt('-2'::float8)", 0xbff428a2f98d728c_u64),
        ("cos('-1000'::float8)", 0x3fe1ff026793f1bb_u64),
        ("cos('-100'::float8)", 0x3feb981dbf665fdf_u64),
        ("cos('-27'::float8)", 0xbfd2b266f959ded5_u64),
        ("cos('-10'::float8)", 0xbfead9ac890c6b1f_u64),
        ("cos('-3.141592653589793'::float8)", 0xbff0000000000000_u64),
        ("cos('-2'::float8)", 0xbfdaa22657537205_u64),
        ("atan('-1000'::float8)", 0xbff91de2c0e658bd_u64),
        ("atan('-100'::float8)", 0xbff8f905eb2def22_u64),
        ("atan('-27'::float8)", 0xbff88a58ec949d14_u64),
        ("atan('-10'::float8)", 0xbff789bd2c160054_u64),
        ("atan('-3.141592653589793'::float8)", 0xbff433b8a322ddd3_u64),
        ("atan('-2'::float8)", 0xbff1b6e192ebbe44_u64),
        ("cosh('-100'::float8)", 0x48e3494a9b171bf5_u64),
        ("cosh('-27'::float8)", 0x424ef822f7f6731d_u64),
        ("cosh('-10'::float8)", 0x40c5829dd053712e_u64),
        ("cosh('-3.141592653589793'::float8)", 0x40272f147fee4000_u64),
        ("cosh('-2'::float8)", 0x400e18fa0df2d9bc_u64),
        ("cosh('-1'::float8)", 0x3ff8b07551d9f550_u64),
        ("tanh('-1000'::float8)", 0xbff0000000000000_u64),
        ("tanh('-100'::float8)", 0xbff0000000000000_u64),
        ("tanh('-27'::float8)", 0xbff0000000000000_u64),
        ("tanh('-10'::float8)", 0xbfeffffffdc96f35_u64),
        ("tanh('-3.141592653589793'::float8)", 0xbfefe175fa292810_u64),
        ("tanh('-2'::float8)", 0xbfeed9505e1bc3d4_u64),
        ("asinh('-1000'::float8)", 0xc01e67530a363e15_u64),
        ("asinh('-100'::float8)", 0xc0153180a93d3c2a_u64),
        ("asinh('-27'::float8)", 0xc00fea242c2068a3_u64),
        ("asinh('-10'::float8)", 0xc007fc5c506d2bdb_u64),
        (
            "asinh('-3.141592653589793'::float8)",
            0xbffdcbf69f10006d_u64,
        ),
        ("asinh('-2'::float8)", 0xbff719218313d087_u64),
        ("acosh('1'::float8)", 0x0000000000000000_u64),
        ("acosh('1.0000001'::float8)", 0x3f3d4effc065cae4_u64),
        ("acosh('1.1'::float8)", 0x3fdc636c1a882f2c_u64),
        ("acosh('2'::float8)", 0x3ff5124271980434_u64),
        ("acosh('3'::float8)", 0x3ffc34366179d427_u64),
        ("acosh('8'::float8)", 0x40062636e408c33b_u64),
        ("atanh('-0.99'::float8)", 0xc0052c581997cd85_u64),
        ("atanh('-0.5'::float8)", 0xbfe193ea7aad030a_u64),
        ("atanh('-0.1'::float8)", 0xbfb9af93cd234412_u64),
        ("atanh('0'::float8)", 0x0000000000000000_u64),
        ("atanh('0.1'::float8)", 0x3fb9af93cd234412_u64),
        ("atanh('0.25'::float8)", 0x3fd058aefa811452_u64),
        ("exp('-100'::float8)", 0x36ea8c1f14e2af5d_u64),
        ("exp('-27'::float8)", 0x3d80885298767e9a_u64),
        ("exp('-10'::float8)", 0x3f07cd79b5647c9b_u64),
        ("exp('-3.141592653589793'::float8)", 0x3fa620227b598efa_u64),
        ("exp('-2'::float8)", 0x3fc152aaa3bf81cc_u64),
        ("exp('-1'::float8)", 0x3fd78b56362cef38_u64),
        ("ln('0.1'::float8)", 0xc0026bb1bbb55515_u64),
        ("ln('0.25'::float8)", 0xbff62e42fefa39ef_u64),
        ("ln('0.5'::float8)", 0xbfe62e42fefa39ef_u64),
        ("ln('0.99'::float8)", 0xbf8495453e6fd4bc_u64),
        ("ln('1'::float8)", 0x0000000000000000_u64),
        ("ln('1.0000001'::float8)", 0x3e7ad7f2847b6492_u64),
        ("log('0.1'::float8)", 0xbff0000000000000_u64),
        ("log('0.25'::float8)", 0xbfe34413509f79ff_u64),
        ("log('0.5'::float8)", 0xbfd34413509f79ff_u64),
        ("log('0.99'::float8)", 0xbf71e0d4874f92f2_u64),
        ("log('1'::float8)", 0x0000000000000000_u64),
        ("log('1.0000001'::float8)", 0x3e6750e5ca0b1098_u64),
        ("cbrt(27::float8)", 0x4008000000000001_u64),
        ("atanh(0.5::float8)", 0x3fe193ea7aad030a_u64),
        ("acosh(2::float8)", 0x3ff5124271980434_u64),
        ("cosh(1::float8)", 0x3ff8b07551d9f550_u64),
        ("cos(1::float8)", 0x3fe14a280fb5068c_u64),
        ("tanh(1::float8)", 0x3fe85efab514f394_u64),
        ("atan(1::float8)", 0x3fe921fb54442d18_u64),
        ("asinh(1::float8)", 0x3fec34366179d427_u64),
        ("exp(1::float8)", 0x4005bf0a8b145769_u64),
        ("ln(2::float8)", 0x3fe62e42fefa39ef_u64),
        ("log(2::float8)", 0x3fd34413509f79ff_u64),
        ("power(2::float8,0.5::float8)", 0x3ff6a09e667f3bcd_u64),
        ("erf(0::float8)", 0x0000000000000000_u64),
        ("erf(1::float8)", 0x3feaf767a741088b_u64),
        ("erf(0.5::float8)", 0x3fe0a7ef5c18edd2_u64),
        ("erf(-0.5::float8)", 0xbfe0a7ef5c18edd2_u64),
        ("erf(-1.5::float8)", 0xbfeeea5557137ae0_u64),
        ("erf('NaN'::float8)", 0x7ff8000000000000_u64),
        ("erf('Infinity'::float8)", 0x3ff0000000000000_u64),
        ("erf('-Infinity'::float8)", 0xbff0000000000000_u64),
        ("erfc(0::float8)", 0x3ff0000000000000_u64),
        ("erfc(1::float8)", 0x3fc4226162fbddd5_u64),
        ("erfc(0.5::float8)", 0x3fdeb02147ce245c_u64),
        ("erfc(-0.5::float8)", 0x3ff853f7ae0c76e9_u64),
        ("erfc(-1.5::float8)", 0x3fff752aab89bd70_u64),
        ("erfc('NaN'::float8)", 0x7ff8000000000000_u64),
        ("erfc('Infinity'::float8)", 0x0000000000000000_u64),
        ("erfc('-Infinity'::float8)", 0x4000000000000000_u64),
        ("gamma(1::float8)", 0x3ff0000000000000_u64),
        ("gamma(0.5::float8)", 0x3ffc5bf891b4ef6b_u64),
        ("gamma(-0.5::float8)", 0xc00c5bf891b4ef6a_u64),
        ("gamma(-1.5::float8)", 0x4002e7fb0bcdf4f2_u64),
        ("gamma('NaN'::float8)", 0x7ff8000000000000_u64),
        ("gamma('Infinity'::float8)", 0x7ff0000000000000_u64),
        ("lgamma(1::float8)", 0x0000000000000000_u64),
        ("lgamma(0.5::float8)", 0x3fe250d048e7a1bd_u64),
        ("lgamma(-0.5::float8)", 0x3ff43f89a3f0edd6_u64),
        ("lgamma(-1.5::float8)", 0x3feb858151820f85_u64),
        ("lgamma('NaN'::float8)", 0x7ff8000000000000_u64),
        ("lgamma('Infinity'::float8)", 0x7ff0000000000000_u64),
        ("lgamma('-Infinity'::float8)", 0x7ff0000000000000_u64),
        ("gamma(-170.5::float8)", 0x8017d2374dfcda79_u64),
        ("gamma(-171.5::float8)", 0x0000238ee05c879e_u64),
        ("erfc(30::float8)", 0x0000000000000000_u64),
        ("erf('-0'::float8)", 0x8000000000000000_u64),
    ] {
        let result = s
            .execute(&format!("SELECT {expr} FROM float_input"), &[], None)
            .unwrap();
        let Value::Float8(value) = result.rows[0].values[0] else {
            panic!("{expr}: float8 required")
        };
        if f64::from_bits(expected).is_nan() {
            assert!(value.value().is_nan(), "{expr}");
        } else {
            assert_eq!(value.value().to_bits(), expected, "{expr}");
        }
    }
}
#[test]
fn image_range_errors() {
    let admin = Admin::create(fresh("glibc-errors"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE float_input (id bigint primary key)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO float_input VALUES (1)", &[], None)
        .unwrap();
    for (expr, message) in [
        ("gamma(0::float8)", "value out of range: overflow"),
        ("gamma('-0'::float8)", "value out of range: overflow"),
        ("gamma(-1::float8)", "value out of range: overflow"),
        ("gamma('-Infinity'::float8)", "value out of range: overflow"),
        ("gamma(172::float8)", "value out of range: overflow"),
        ("gamma(-180.5::float8)", "value out of range: underflow"),
        ("lgamma(0::float8)", "value out of range: overflow"),
        ("lgamma(-1::float8)", "value out of range: overflow"),
        ("lgamma(1e308::float8)", "value out of range: overflow"),
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {expr} FROM float_input"), &[], None),
            Err(CoreError::Sql(SqlError::Overflow)),
            "{expr}"
        );
        assert_eq!(take_sql_error_message().as_deref(), Some(message), "{expr}");
    }
}

#[test]
fn missing_library_fails_closed() {
    let empty = fresh("glibm-missing");
    std::fs::create_dir_all(&empty).unwrap();
    // Isolate executable discovery too: cargo test of the LGPL crate leaves
    // its DLL beside this test binary in the shared target/deps directory.
    let bin = empty.join("bin");
    std::fs::create_dir(&bin).unwrap();
    let child = bin.join(std::env::current_exe().unwrap().file_name().unwrap());
    std::fs::copy(std::env::current_exe().unwrap(), &child).unwrap();
    let status = std::process::Command::new(&child)
        .args([
            "--exact",
            "sql_float_glibc::missing_library_child",
            "--nocapture",
        ])
        .env("GIGGLES_GLIBM_MISSING_CHILD", "1")
        .env("GIGGLES_GLIBM_DIR", &empty)
        .env("GIGGLES_GLIBM_CACHE", &empty)
        .status()
        .unwrap();
    std::fs::remove_file(&child).unwrap();
    std::fs::remove_dir(&bin).unwrap();
    std::fs::remove_dir(&empty).unwrap();
    assert!(status.success());
}
#[test]
fn missing_library_child() {
    if std::env::var_os("GIGGLES_GLIBM_MISSING_CHILD").is_none() {
        return;
    }
    let admin = Admin::create(fresh("glibm-missing-child"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE float_input (id bigint primary key)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO float_input VALUES (1)", &[], None)
        .unwrap();
    for expr in [
        "cbrt(27::float8)",
        "sind(30::float8)",
        "erf(1::float8)",
        "gamma(1::float8)",
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {expr} FROM float_input"), &[], None),
            Err(CoreError::Sql(SqlError::Unsupported)),
            "{expr}"
        );
    }
    assert!(s.execute("SELECT sqrt(4::float8)", &[], None).is_ok());
}

#[test]
fn postgres_float_error_diagnostics() {
    let admin = Admin::create(fresh("float-diagnostics"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE float_input (id bigint primary key)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO float_input VALUES (1)", &[], None)
        .unwrap();
    for (expr, code, message) in [
        (
            "gamma('0'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "gamma('-0'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "gamma('-1'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "gamma('-Infinity'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "gamma('1e308'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "gamma('5e-324'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "lgamma('0'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "lgamma('-0'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "lgamma('-1'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "lgamma('1e308'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "sqrt('-1'::float8)",
            "2201F",
            "cannot take square root of a negative number",
        ),
        (
            "sqrt('-0.5'::float8)",
            "2201F",
            "cannot take square root of a negative number",
        ),
        (
            "sqrt('-Infinity'::float8)",
            "2201F",
            "cannot take square root of a negative number",
        ),
        (
            "exp('1e308'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        ("ln('0'::float8)", "2201E", "cannot take logarithm of zero"),
        ("ln('-0'::float8)", "2201E", "cannot take logarithm of zero"),
        (
            "ln('-1'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "ln('-0.5'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "ln('-Infinity'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        ("log('0'::float8)", "2201E", "cannot take logarithm of zero"),
        (
            "log('-0'::float8)",
            "2201E",
            "cannot take logarithm of zero",
        ),
        (
            "log('-1'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "log('-0.5'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "log('-Infinity'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "log10('0'::float8)",
            "2201E",
            "cannot take logarithm of zero",
        ),
        (
            "log10('-0'::float8)",
            "2201E",
            "cannot take logarithm of zero",
        ),
        (
            "log10('-1'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "log10('-0.5'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "log10('-Infinity'::float8)",
            "2201E",
            "cannot take logarithm of a negative number",
        ),
        (
            "radians('5e-324'::float8)",
            "22003",
            "value out of range: underflow",
        ),
        (
            "degrees('1e308'::float8)",
            "22003",
            "value out of range: overflow",
        ),
        ("sin('Infinity'::float8)", "22003", "input is out of range"),
        ("sin('-Infinity'::float8)", "22003", "input is out of range"),
        ("cos('Infinity'::float8)", "22003", "input is out of range"),
        ("cos('-Infinity'::float8)", "22003", "input is out of range"),
        ("tan('Infinity'::float8)", "22003", "input is out of range"),
        ("tan('-Infinity'::float8)", "22003", "input is out of range"),
        ("cot('Infinity'::float8)", "22003", "input is out of range"),
        ("cot('-Infinity'::float8)", "22003", "input is out of range"),
        ("asin('Infinity'::float8)", "22003", "input is out of range"),
        (
            "asin('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("asin('1e308'::float8)", "22003", "input is out of range"),
        ("acos('Infinity'::float8)", "22003", "input is out of range"),
        (
            "acos('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("acos('1e308'::float8)", "22003", "input is out of range"),
        ("acosh('0'::float8)", "22003", "input is out of range"),
        ("acosh('-0'::float8)", "22003", "input is out of range"),
        ("acosh('-1'::float8)", "22003", "input is out of range"),
        ("acosh('0.5'::float8)", "22003", "input is out of range"),
        ("acosh('-0.5'::float8)", "22003", "input is out of range"),
        (
            "acosh('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("acosh('5e-324'::float8)", "22003", "input is out of range"),
        (
            "atanh('Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        (
            "atanh('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("atanh('1e308'::float8)", "22003", "input is out of range"),
        ("sind('Infinity'::float8)", "22003", "input is out of range"),
        (
            "sind('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("cosd('Infinity'::float8)", "22003", "input is out of range"),
        (
            "cosd('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("tand('Infinity'::float8)", "22003", "input is out of range"),
        (
            "tand('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("cotd('Infinity'::float8)", "22003", "input is out of range"),
        (
            "cotd('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        (
            "asind('Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        (
            "asind('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("asind('1e308'::float8)", "22003", "input is out of range"),
        (
            "acosd('Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        (
            "acosd('-Infinity'::float8)",
            "22003",
            "input is out of range",
        ),
        ("acosd('1e308'::float8)", "22003", "input is out of range"),
        ("acos(2::float8)", "22003", "input is out of range"),
        ("asind(2::float8)", "22003", "input is out of range"),
        ("acosh(0.5::float8)", "22003", "input is out of range"),
        (
            "gamma(171.7::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "gamma(1e-320::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "gamma(-180.5::float8)",
            "22003",
            "value out of range: underflow",
        ),
        ("exp(710::float8)", "22003", "value out of range: overflow"),
        (
            "exp(-746::float8)",
            "22003",
            "value out of range: underflow",
        ),
        (
            "power(0::float8,-1::float8)",
            "2201F",
            "zero raised to a negative power is undefined",
        ),
        (
            "power(-1::float8,0.5::float8)",
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ),
        (
            "power(-8::float8,1.0::float8/3)",
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ),
        (
            "power(1e308::float8,2::float8)",
            "22003",
            "value out of range: overflow",
        ),
        (
            "power(1e-308::float8,2::float8)",
            "22003",
            "value out of range: underflow",
        ),
        (
            "power('5e-324'::float8,'1e308'::float8)",
            "22003",
            "value out of range: underflow",
        ),
    ] {
        let error = s
            .execute(&format!("SELECT {expr} FROM float_input"), &[], None)
            .unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), code, "{expr}");
        assert_eq!(take_sql_error_message().as_deref(), Some(message), "{expr}");
    }
    for (expr, expected) in [
        ("cosh(711::float8)", f64::INFINITY),
        ("sinh(-711::float8)", f64::NEG_INFINITY),
        ("cosh('1e308'::float8)", f64::INFINITY),
        ("sinh('1e308'::float8)", f64::INFINITY),
    ] {
        let result = s
            .execute(&format!("SELECT {expr} FROM float_input"), &[], None)
            .unwrap();
        let Value::Float8(value) = result.rows[0].values[0] else {
            panic!("{expr}")
        };
        assert_eq!(value.value(), expected, "{expr}");
    }
}

#[test]
fn table_free_special_functions() {
    let admin = Admin::create(fresh("table-free-special"), Limits::default()).unwrap();
    let result = admin.sql_session().execute(
        "SELECT erf(0.5), erfc(0.5), gamma(0.5), lgamma(0.5)", &[], None).unwrap();
    for (value, expected) in result.rows[0].values.iter().zip([
        0.5204998778130465_f64, 0.4795001221869535, 1.772453850905516, 0.5723649429247001
    ]) {
        let Value::Float8(value) = value else { panic!("expected float8") };
        assert_eq!(value.value().to_bits(), expected.to_bits());
    }
}
