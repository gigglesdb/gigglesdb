use giggles_pg_types::temporal::*;
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
                "dd68-temporal-calendar-oracle",
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
    let artifact = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target-dd68-temporal-calendar/observations.tsv");
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(artifact)
        .unwrap();
    writeln!(
        log,
        "{}\t{}",
        q.replace('\n', " "),
        result.trim().replace('\n', " | ")
    )
    .unwrap();
    result.trim().to_owned()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
fn timestamp(s: &str) -> Timestamp {
    Timestamp::parse(s, &ParserSettings::default()).unwrap_or_else(|e| panic!("{s}: {e:?}"))
}
fn check_t(q: &str, v: Result<Timestamp, TemporalFunctionError>) {
    match v {
        Ok(v) => assert_eq!(
            pg(&format!(
                "select ({q})::text,encode(timestamp_send({q}),'hex'),pg_typeof({q})"
            )),
            format!("{}|{}|timestamp without time zone", v, hex(&v.to_binary())),
            "{q}"
        ),
        Err(e) => assert_eq!(
            pg(&format!("select pg_temp.state($q$select {q}$q$)")),
            if e == TemporalFunctionError::NumericOverflow {
                "22003"
            } else {
                "22008"
            },
            "{q}"
        ),
    }
}
fn check_i(q: &str, v: Result<Interval, TemporalFunctionError>) {
    match v {
        Ok(v) => {
            let line = pg(&format!(
                "select ({q})::text,encode(interval_send({q}),'hex'),pg_typeof({q})"
            ));
            let parts: Vec<_> = line.split('|').collect();
            assert_eq!(parts[1], hex(&v.to_binary()), "{q}: {line}");
            assert_eq!(parts[2], "interval");
            assert!(
                Interval::parse(parts[0]).unwrap().same_components(v),
                "{q}: {line} vs {v}"
            );
        }
        Err(e) => assert_eq!(
            pg(&format!("select pg_temp.state($q$select {q}$q$)")),
            if e == TemporalFunctionError::NumericOverflow {
                "22003"
            } else {
                "22008"
            },
            "{q}"
        ),
    }
}
fn session() -> std::sync::MutexGuard<'static, ()> {
    static GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    GATE.lock().unwrap()
}
fn setup() {
    pg(
        "create or replace function pg_temp.state(q text) returns text language plpgsql as $$ begin execute q; return 'OK'; exception when others then return sqlstate; end $$",
    );
}
#[test]
fn calendar_pg18() {
    let _session = session();
    setup();
    let dates = [
        "2000-01-31 12:34:56.123456",
        "2000-02-29 23:59:59.999999",
        "2001-03-01 00:00:00",
        "0001-01-31 00:00:00 BC",
        "4713-01-01 00:00:00 BC",
        "294276-12-31 23:59:59.999999",
        "infinity",
        "-infinity",
    ];
    let spans = [
        "1 month",
        "-1 month",
        "1 month -31 days 1 microsecond",
        "-13 months 32 days -25 hours",
        "0 days",
        "infinity",
        "-infinity",
    ];
    for a in dates {
        for i in spans {
            let t = timestamp(a);
            let span = Interval::parse(i).unwrap();
            check_t(
                &format!("timestamp '{a}'+interval '{i}'"),
                t.add_interval(span),
            );
            check_t(
                &format!("timestamp '{a}'-interval '{i}'"),
                t.sub_interval(span),
            );
        }
        for b in dates {
            check_i(
                &format!("age(timestamp '{a}',timestamp '{b}')"),
                timestamp(a).age(timestamp(b)),
            );
        }
    }
    for m in [-2147483648, -31, -1, 0, 1, 31, 2147483647] {
        for d in [-2147483648, -31, -1, 0, 1, 31, 2147483647] {
            for t in [
                -9223372036854775808i64,
                -86400000001,
                -1,
                0,
                1,
                86400000001,
                9223372036854775807,
            ] {
                let i = Interval::new(m, d, t);
                let sql = format!("interval '{m} months {d} days {t} microseconds'");
                check_i(&format!("justify_hours({sql})"), i.justify_hours());
                check_i(&format!("justify_days({sql})"), i.justify_days());
                check_i(&format!("justify_interval({sql})"), i.justify_interval());
            }
        }
    }
}
#[test]
fn constructors_pg18() {
    let _session = session();
    setup();
    for s in [
        -0.0000006,
        -0.0000005,
        -0.0000001,
        0.0,
        0.0000005,
        0.0000015,
        59.9999994,
        59.9999995,
        60.0,
        60.0000004,
        60.0000006,
        f64::MAX,
        -f64::MAX,
        9223372036854.775,
        -9223372036854.775,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        let sql = if s.is_nan() {
            "'NaN'::float8".into()
        } else if s == f64::INFINITY {
            "'Infinity'::float8".into()
        } else if s == f64::NEG_INFINITY {
            "'-Infinity'::float8".into()
        } else {
            format!("'{s}'::float8")
        };
        for h in [-1, 0, 23, 24, 25] {
            for m in [-1, 0, 59, 60] {
                let q = format!("make_time({h},{m},{sql})");
                match make_time_float8(h, m, s) {
                    Ok(t) => assert_eq!(
                        pg(&format!("select encode(time_send({q}),'hex')")),
                        hex(&t.to_binary()),
                        "{q}"
                    ),
                    Err(_) => assert_eq!(
                        pg(&format!("select pg_temp.state($q$select {q}$q$)")),
                        "22008",
                        "{q}"
                    ),
                }
                check_t(
                    &format!("make_timestamp(2000,2,29,{h},{m},{sql})"),
                    make_timestamp_float8(2000, 2, 29, h, m, s),
                );
            }
        }
        for y in [i32::MIN, -1, 0, 1, i32::MAX] {
            check_i(
                &format!("make_interval({y},-1,2,-3,4,-5,{sql})"),
                make_interval(y, -1, 2, -3, 4, -5, s),
            );
        }
    }
}
#[test]
fn zones_pg18() {
    let _session = session();
    setup();
    for zone in ["America/New_York", "Australia/Lord_Howe", "Pacific/Apia"] {
        let z = SessionZone::named(zone).unwrap();
        pg(&format!("set timezone='{zone}'"));
        for a in [
            "2021-03-13 12:00:00",
            "2021-11-06 01:30:00",
            "2021-10-02 02:15:00",
            "2021-04-03 01:45:00",
            "2011-12-29 12:00:00",
        ] {
            let t = z.to_instant(timestamp(a)).unwrap();
            for s in [
                "1 day",
                "24 hours",
                "1 month 1 day",
                "-1 day",
                "-1 month -25 hours",
            ] {
                let i = Interval::parse(s).unwrap();
                let q = format!("timestamptz '{a}'+interval '{s}'");
                let v = t.add_interval(i, z).unwrap();
                assert_eq!(
                    pg(&format!("select encode(timestamptz_send({q}),'hex')")),
                    hex(&v.to_binary()),
                    "{zone} {q}"
                );
                let other = z.to_instant(timestamp("2021-01-31 01:15:00")).unwrap();
                check_i(
                    &format!("age(timestamptz '{a}',timestamptz '2021-01-31 01:15:00')"),
                    t.age(other, z),
                );
            }
        }
    }
    pg("set timezone='UTC'");
}

#[test]
fn audit_extract_trunc_bin_pg18() {
    let _session = session();
    setup();
    use TemporalField::*;
    let fields = [
        Microseconds,
        Milliseconds,
        Second,
        Minute,
        Hour,
        Day,
        Month,
        Quarter,
        Week,
        Year,
        Decade,
        Century,
        Millennium,
        IsoYear,
        Dow,
        IsoDow,
        Doy,
        Julian,
        Epoch,
    ];
    for s in [
        "0001-12-31 23:59:59.999999 BC",
        "0011-01-01 00:00:00 BC",
        "2005-01-01 00:00:00",
        "2006-01-01 00:00:00",
        "2015-12-31 23:59:59.999999",
        "4713-01-01 00:00:00 BC",
        "294276-12-31 23:59:59.999999",
        "infinity",
        "-infinity",
    ] {
        let t = timestamp(s);
        for f in fields {
            let name = format!("{f:?}").to_ascii_lowercase();
            let q = format!("extract({name} from timestamp '{s}')");
            let v = t.extract(f).unwrap();
            let expected = pg(&format!("select ({q})::text,pg_typeof({q})"));
            let parts: Vec<_> = expected.split('|').collect();
            assert_eq!(parts[1], "numeric");
            assert_eq!(
                v,
                if parts[0].is_empty() {
                    None
                } else {
                    Some(giggles_pg_types::Numeric::parse(parts[0]).unwrap())
                },
                "{q}"
            );
            let q = format!("date_trunc('{name}',timestamp '{s}')");
            match t.date_trunc(f) {
                Ok(v) => check_t(&q, Ok(v)),
                Err(TemporalFunctionError::UnsupportedField) => assert_eq!(
                    pg(&format!("select pg_temp.state($q$select {q}$q$)")),
                    if matches!(f, IsoYear | Dow | IsoDow | Doy | Julian | Epoch) {
                        "22023"
                    } else {
                        "0A000"
                    },
                    "{q}"
                ),
                Err(_) => assert_eq!(
                    pg(&format!("select pg_temp.state($q$select {q}$q$)")),
                    "22008",
                    "{q}"
                ),
            }
        }
        for stride in [
            "1 microsecond",
            "7 days",
            "-1 second",
            "1 month",
            "infinity",
        ] {
            let i = Interval::parse(stride).unwrap();
            let q = format!("date_bin(interval '{stride}',timestamp '{s}',timestamp '2000-01-01')");
            let v = t.date_bin(i, timestamp("2000-01-01 00:00:00"));
            if let Ok(v) = v {
                check_t(&q, Ok(v));
            } else {
                let state = pg(&format!("select pg_temp.state($q$select {q}$q$)"));
                assert!(["22008", "0A000"].contains(&state.as_str()), "{q}: {state}");
            }
        }
    }
    for alias in [
        "m",
        "MONS",
        "microseconds",
        "milliseconds",
        "timezone_hour",
        "millennia",
        "qtr",
        "jd",
        "isoyear",
        "yrs",
        "usecs",
    ] {
        let f: TemporalField = alias.parse().unwrap();
        let t = timestamp("2000-01-01 01:02:03.123456");
        let q = format!("extract({alias} from timestamp '2000-01-01 01:02:03.123456')");
        if let Ok(v) = t.extract(f) {
            assert_eq!(
                v.unwrap(),
                giggles_pg_types::Numeric::parse(&pg(&format!("select {q}"))).unwrap()
            );
        }
    }

    for text in [
        "-13 months -32 days -25 hours -0.000001 seconds",
        "-1 microsecond",
        "25 months -31 days 25 hours",
        "infinity",
        "-infinity",
    ] {
        let i = Interval::parse(text).unwrap();
        for f in [
            Microseconds,
            Milliseconds,
            Second,
            Minute,
            Hour,
            Day,
            Month,
            Quarter,
            Year,
            Decade,
            Century,
            Millennium,
            Epoch,
        ] {
            let name = format!("{f:?}").to_ascii_lowercase();
            let q = format!("extract({name} from interval '{text}')");
            let expected = pg(&format!("select {q}"));
            assert_eq!(
                i.extract(f).unwrap(),
                if expected.is_empty() {
                    None
                } else {
                    Some(giggles_pg_types::Numeric::parse(&expected).unwrap())
                },
                "{q}"
            );
            if f != Epoch {
                check_i(
                    &format!("date_trunc('{name}',interval '{text}')"),
                    i.date_trunc(f),
                );
            }
        }
    }
    assert!("\u{e9}".parse::<TemporalField>().is_err());
}
