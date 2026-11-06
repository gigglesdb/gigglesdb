use giggles_pg_types::{Numeric, temporal::*};
use std::process::Command;
fn pg(q: &str) -> String {
    use std::io::{BufRead, Write};
    use std::sync::{Mutex, OnceLock};
    struct Oracle {
        _child: std::process::Child,
        input: std::process::ChildStdin,
        output: std::io::BufReader<std::process::ChildStdout>,
    }
    static ORACLE: OnceLock<Mutex<Oracle>> = OnceLock::new();
    let oracle = ORACLE.get_or_init(|| {
        let mut c = Command::new("docker")
            .args([
                "exec",
                "-i",
                "dd68-temporal-functions-oracle",
                "psql",
                "-U",
                "postgres",
                "-Atq",
                "-v",
                "ON_ERROR_STOP=1",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let input = c.stdin.take().unwrap();
        let output = std::io::BufReader::new(c.stdout.take().unwrap());
        Mutex::new(Oracle {
            _child: c,
            input,
            output,
        })
    });
    let mut o = oracle.lock().unwrap();
    writeln!(o.input, r"{q};\echo __TEMPORAL_END__").unwrap();
    o.input.flush().unwrap();
    let mut result = String::new();
    loop {
        let mut line = String::new();
        assert!(
            o.output.read_line(&mut line).unwrap() > 0,
            "oracle terminated: {q}"
        );
        if line.trim() == "__TEMPORAL_END__" {
            break;
        }
        result.push_str(&line);
    }
    result.trim().to_owned()
}
fn fields() -> Vec<(TemporalField, &'static str)> {
    use TemporalField::*;
    vec![
        (Microseconds, "microseconds"),
        (Milliseconds, "milliseconds"),
        (Second, "second"),
        (Minute, "minute"),
        (Hour, "hour"),
        (Day, "day"),
        (Month, "month"),
        (Quarter, "quarter"),
        (Week, "week"),
        (Year, "year"),
        (Decade, "decade"),
        (Century, "century"),
        (Millennium, "millennium"),
        (IsoYear, "isoyear"),
        (Dow, "dow"),
        (IsoDow, "isodow"),
        (Doy, "doy"),
        (Julian, "julian"),
        (Epoch, "epoch"),
        (Timezone, "timezone"),
        (TimezoneHour, "timezone_hour"),
        (TimezoneMinute, "timezone_minute"),
    ]
}
fn compare(q: &str, actual: Option<Numeric>) {
    let expected = pg(q);
    assert_eq!(
        actual,
        if expected.is_empty() {
            None
        } else {
            Some(Numeric::parse(&expected).unwrap())
        },
        "{q}"
    );
}
#[test]
fn constructors_bins_and_errors() {
    assert!(make_date(0, 1, 1).is_err());
    assert!(make_date(1900, 2, 29).is_err());
    assert!(make_date(2000, 2, 29).is_ok());
    assert!(make_time(24, 0, 1).is_err());
    assert!(make_time(0, 60, 0).is_err());
    let origin = Timestamp::from_microseconds(0).unwrap();
    assert_eq!(
        Timestamp::from_microseconds(-1)
            .unwrap()
            .date_bin(Interval::new(0, 0, 10), origin)
            .unwrap()
            .microseconds(),
        -10
    );
    for stride in [
        Interval::new(1, 0, 0),
        Interval::new(0, 0, 0),
        Interval::new(0, 0, -1),
        Interval::INFINITY,
    ] {
        assert!(origin.date_bin(stride, origin).is_err());
    }
    assert!(
        origin
            .date_bin(Interval::new(0, 0, 1), Timestamp::INFINITY)
            .is_err()
    );
    assert_eq!(
        Timestamp::INFINITY.extract(TemporalField::Day).unwrap(),
        None
    );
    assert_eq!(
        Time::from_microseconds(0)
            .unwrap()
            .extract(TemporalField::Year),
        Err(TemporalFunctionError::UnsupportedField)
    );
}
#[test]
#[ignore = "requires pinned owned temporal function oracle"]
fn live_extract_and_trunc() {
    assert_eq!(pg("show server_version_num"), "180000");
    let p = ParserSettings::default();
    for s in [
        "2000-01-01 12:34:56.123456",
        "1969-12-31 23:59:59.999999",
        "2021-01-01 00:00:00",
        "0001-02-28 12:00:00 BC",
        "4714-11-24 00:00:00 BC",
        "294276-12-31 23:59:59.999999",
        "infinity",
        "-infinity",
    ] {
        let t = Timestamp::parse(s, &p).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = t.extract(f) {
                compare(&format!("select extract({name} from timestamp '{s}')"), v);
            }
            if let Ok(v) = t.date_trunc(f) {
                assert_eq!(
                    pg(&format!(
                        "select encode(timestamp_send(date_trunc('{name}',timestamp '{s}')),'hex')"
                    )),
                    hex(&v.to_binary()),
                    "{s} {name}"
                );
            }
        }
    }
    for s in [
        "2000-01-01",
        "0001-12-31 BC",
        "5874897-12-31",
        "infinity",
        "-infinity",
    ] {
        let d = Date::parse(s, &p).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = d.extract(f) {
                compare(&format!("select extract({name} from date '{s}')"), v);
            }
        }
    }
    for s in ["00:00:00", "24:00:00", "12:34:56.123456"] {
        let t = Time::parse(s).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = t.extract(f) {
                compare(&format!("select extract({name} from time '{s}')"), v);
            }
        }
        for offset in [-19845, 0, 20730] {
            let z = TimeTz::new(t, offset).unwrap();
            for (f, name) in fields() {
                if let Ok(v) = z.extract(f) {
                    compare(&format!("select extract({name} from timetz '{}')", z), v);
                }
            }
        }
    }
    for s in [
        "1 year 2 mons -40 days -49:23:45.123456",
        "-14 mons 50 days 100:00:00",
        "2147483647 mons -2147483648 days 2562047788:00:00",
        "infinity",
        "-infinity",
    ] {
        let i = Interval::parse(s).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = i.extract(f) {
                compare(&format!("select extract({name} from interval '{s}')"), v);
            }
            if let Ok(v) = i.date_trunc(f) {
                assert_eq!(
                    pg(&format!(
                        "select encode(interval_send(date_trunc('{name}',interval '{s}')),'hex')"
                    )),
                    hex(&v.to_binary()),
                    "{s} {name}"
                );
            }
        }
    }
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
#[test]
#[ignore = "requires pinned owned temporal function oracle"]
fn live_zones_and_bins() {
    let p = ParserSettings::default();
    for zone in [
        "America/New_York",
        "Australia/Lord_Howe",
        "Pacific/Apia",
        "UTC",
    ] {
        let z = SessionZone::named(zone).unwrap();
        for s in [
            "2024-03-10 07:15:12.123456",
            "2024-11-03 05:45:00",
            "2024-11-03 06:45:00",
            "2100-07-01 12:34:56",
            "0001-01-01 00:00:00 BC",
        ] {
            let t = TimestampTz::from_microseconds(Timestamp::parse(s, &p).unwrap().microseconds())
                .unwrap();
            for (f, name) in fields() {
                if let Ok(v) = t.extract(f, z) {
                    compare(
                        &format!(
                            "set timezone='{zone}';select extract({name} from timestamptz '{s}+00')"
                        ),
                        v,
                    );
                }
                if let Ok(v) = t.date_trunc(f, z) {
                    assert_eq!(
                        pg(&format!(
                            "select encode(timestamptz_send(date_trunc('{name}',timestamptz '{s}+00','{zone}')),'hex')"
                        )),
                        hex(&v.to_binary()),
                        "{zone} {s} {name}"
                    );
                }
            }
        }
    }
    for v in [
        -946684800000001,
        -1000001,
        -1,
        0,
        1,
        1000001,
        946684800000001,
    ] {
        for span in [1, 10, 999999, 86400000001] {
            let t = Timestamp::from_microseconds(v).unwrap();
            let o = Timestamp::from_microseconds(12345).unwrap();
            let result = t.date_bin(Interval::new(0, 0, span), o).unwrap();
            assert_eq!(
                pg(&format!(
                    "select encode(timestamp_send(date_bin(interval '{span} microseconds',timestamp '{t}',timestamp '{o}')),'hex')"
                )),
                hex(&result.to_binary())
            );
        }
    }
}
#[test]
#[ignore = "requires pinned owned temporal function oracle"]
fn live_justify_constructors_and_errors() {
    for m in [-14, -1, 0, 1, 14, i32::MAX] {
        for d in [-60, -1, 0, 1, 60] {
            for t in [-172800000001, -1, 0, 1, 172800000001] {
                let i = Interval::new(m, d, t);
                let q = format!(
                    "select encode(interval_send(justify_interval(make_interval(months=>{m},days=>{d})+interval '{t} microseconds')),'hex')"
                );
                if let Ok(v) = i.justify_interval() {
                    assert_eq!(pg(&q), hex(&v.to_binary()), "{m} {d} {t}");
                }
            }
        }
    }
    for (y, m, d) in [(2000, 2, 29), (-1, 1, 1), (5874897, 12, 31)] {
        let v = make_date(y, m, d).unwrap();
        assert_eq!(
            pg(&format!(
                "select encode(date_send(make_date({y},{m},{d})),'hex')"
            )),
            hex(&v.to_binary())
        );
    }
    for (h, m, s) in [(0, 0, 0), (24, 0, 0), (12, 34, 56123456)] {
        let v = make_time(h, m, s).unwrap();
        assert_eq!(
            pg(&format!(
                "select encode(time_send(make_time({h},{m},{s}::float8/1000000)),'hex')"
            )),
            hex(&v.to_binary())
        );
    }
    for q in [
        "select make_date(0,1,1)",
        "select make_date(1900,2,29)",
        "select make_time(24,0,1)",
        "select date_bin('0 seconds',timestamp '2000-01-01',timestamp '2000-01-01')",
        "select date_bin('1 month',timestamp '2000-01-01',timestamp '2000-01-01')",
        "select extract(year from time '12:00:00')",
        "select date_trunc('epoch',timestamp '2000-01-01')",
    ] {
        let o = Command::new("docker")
            .args([
                "exec",
                "dd68-temporal-functions-oracle",
                "psql",
                "-U",
                "postgres",
                "-Atq",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                q,
            ])
            .output()
            .unwrap();
        assert!(!o.status.success(), "{q}");
    }
}
#[test]
#[ignore = "requires pinned owned temporal function oracle"]
fn live_float_parts() {
    let p = ParserSettings::default();
    for s in [
        "2000-01-01 12:34:56.123456",
        "1969-12-31 23:59:59.999999",
        "294276-12-31 23:59:59.999999",
        "infinity",
        "-infinity",
    ] {
        let t = Timestamp::parse(s, &p).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = t.date_part(f) {
                compare_float(
                    &format!(
                        "select encode(float8send(date_part('{name}',timestamp '{s}')),'hex')"
                    ),
                    v,
                );
            }
        }
    }
    for s in ["-14 mons 60 days -49:23:45.123456", "infinity"] {
        let t = Interval::parse(s).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = t.date_part(f) {
                compare_float(
                    &format!("select encode(float8send(date_part('{name}',interval '{s}')),'hex')"),
                    v,
                );
            }
        }
    }
}
fn compare_float(q: &str, v: Option<giggles_pg_types::Float8>) {
    assert_eq!(
        pg(q),
        v.map(|n| hex(&n.to_binary())).unwrap_or_default(),
        "{q}"
    );
}
#[test]
#[ignore = "requires pinned owned temporal function oracle"]
fn live_other_float_types() {
    let p = ParserSettings::default();
    for s in ["24:00:00", "12:34:56.123456", "00:00:00.000001"] {
        let t = Time::parse(s).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = t.date_part(f) {
                compare_float(
                    &format!("select encode(float8send(date_part('{name}',time '{s}')),'hex')"),
                    v,
                );
            }
            for offset in [-19845, 20730] {
                let z = TimeTz::new(t, offset).unwrap();
                if let Ok(v) = z.date_part(f) {
                    compare_float(
                        &format!(
                            "select encode(float8send(date_part('{name}',timetz '{z}')),'hex')"
                        ),
                        v,
                    );
                }
            }
        }
    }
    for s in ["5874897-12-31", "0001-01-01 BC", "-infinity"] {
        let d = Date::parse(s, &p).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = d.date_part(f) {
                compare_float(
                    &format!("select encode(float8send(date_part('{name}',date '{s}')),'hex')"),
                    v,
                );
            }
        }
    }
    let t = TimestampTz::parse("2024-11-03 06:45:56.123456+00", &p).unwrap();
    for zone in ["America/New_York", "Australia/Lord_Howe"] {
        let z = SessionZone::named(zone).unwrap();
        for (f, name) in fields() {
            if let Ok(v) = t.date_part(f, z) {
                compare_float(
                    &format!(
                        "set timezone='{zone}';select encode(float8send(date_part('{name}',timestamptz '2024-11-03 06:45:56.123456+00')),'hex')"
                    ),
                    v,
                );
            }
        }
    }
}
