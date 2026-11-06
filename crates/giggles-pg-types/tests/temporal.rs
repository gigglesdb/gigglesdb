use giggles_pg_types::{Error, temporal::*};
use proptest::prelude::*;
use std::{
    io::Write,
    process::{Command, Stdio},
};
#[test]
fn boundaries_and_semantics() {
    let p = ParserSettings::default();
    assert_eq!(Date::parse("2000-01-01", &p).unwrap().days(), 0);
    for (s, d) in [
        ("0001-01-01 BC", -730485),
        ("4714-11-24 BC", -2451545),
        ("5874897-12-31", 2145031948),
    ] {
        let v = Date::parse(s, &p).unwrap();
        assert_eq!(v.days(), d);
        assert_eq!(v.to_string(), s);
    }
    for s in [
        "0000-01-01",
        "2001-02-29",
        "2000-13-01",
        "4714-11-23 BC",
        "5874898-01-01",
    ] {
        assert!(Date::parse(s, &p).is_err(), "{s}");
    }
    assert!(Timestamp::from_microseconds(MIN_TIMESTAMP - 1).is_err());
    assert!(Timestamp::from_microseconds(END_TIMESTAMP).is_err());
    assert_eq!(
        Time::parse("23:59:59.9999999").unwrap().microseconds(),
        USECS_PER_DAY
    );
    assert_eq!(Time::parse("00:00:00.0000005").unwrap().microseconds(), 0);
    assert_eq!(Time::parse("00:00:00.0000015").unwrap().microseconds(), 2);
    for s in [
        "24:00:00.000001",
        "-01:00",
        "12:60",
        "12:00:60",
        "12:00:00.a",
    ] {
        assert!(Time::parse(s).is_err(), "{s}");
    }
    let m = Interval::new(1, 0, 0);
    let d = Interval::new(0, 30, 0);
    assert_eq!(m, d);
    assert!(!m.same_components(d));
    assert_eq!(Interval::new(0, 1, 0), Interval::new(0, 0, USECS_PER_DAY));
    let a = TimeTz::parse("12:00+01", &p).unwrap();
    let b = TimeTz::parse("11:00+00", &p).unwrap();
    assert!(a < b);
    assert_ne!(a, b);
    let p = ParserSettings {
        date_order: DateOrder::Dmy,
        offset_east: 3600,
    };
    assert_eq!(
        Date::parse("31/12/2000", &p).unwrap().to_string(),
        "2000-12-31"
    );
    assert_eq!(
        TimestampTz::parse("2000-01-01 00:00", &p)
            .unwrap()
            .microseconds(),
        -3_600_000_000
    );
    assert_eq!(Time::parse(&"1".repeat(257)), Err(Error::Limit));
    assert!(TimestampTz::parse("2000-01-01 00:00 America/New_York", &p).is_err());
}
proptest! {
 #![proptest_config(ProptestConfig::with_cases(1024))]
 #[test] fn date_roundtrip(v in -2451545i32..2145031949) {let v=Date::from_days(v).unwrap();prop_assert_eq!(Date::parse(&v.to_string(),&ParserSettings::default()).unwrap(),v);prop_assert_eq!(Date::from_binary(&v.to_binary()).unwrap(),v);}
 #[test] fn timestamp_roundtrip(v in MIN_TIMESTAMP..END_TIMESTAMP) {let v=Timestamp::from_microseconds(v).unwrap();prop_assert_eq!(Timestamp::parse(&v.to_string(),&ParserSettings::default()).unwrap(),v);prop_assert_eq!(Timestamp::from_binary(&v.to_binary()).unwrap(),v);}
 #[test] fn interval_roundtrip(m in any::<i32>(),d in any::<i32>(),u in any::<i64>()) {let v=Interval::new(m,d,u);prop_assert!(Interval::parse(&v.to_string()).unwrap().same_components(v));prop_assert!(Interval::from_binary(&v.to_binary()).unwrap().same_components(v));}
 #[test] fn malformed(b in prop::collection::vec(any::<u8>(),0..300)) {let _=Date::from_binary(&b);let _=Time::from_binary(&b);let _=Timestamp::from_binary(&b);let _=TimestampTz::from_binary(&b);let _=TimeTz::from_binary(&b);let _=Interval::from_binary(&b);if let Ok(s)=std::str::from_utf8(&b) {let p=ParserSettings::default();let _=Date::parse(s,&p);let _=Time::parse(s);let _=TimestampTz::parse(s,&p);let _=TimeTz::parse(s,&p);let _=Interval::parse(s);}}
}
fn sql(q: &str) -> String {
    let mut c = Command::new("docker")
        .args([
            "exec",
            "-i",
            "dd68-temporal-oracle",
            "psql",
            "-U",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-Atq",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    c.stdin.take().unwrap().write_all(q.as_bytes()).unwrap();
    let o = c.wait_with_output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    String::from_utf8(o.stdout).unwrap().trim_end().into()
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}
#[test]
#[ignore = "requires owned pinned dd68-temporal-oracle container"]
fn pg180_send_and_text() {
    let image = "postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c";
    let id = |args: &[&str]| {
        let o = Command::new("docker").args(args).output().unwrap();
        assert!(o.status.success());
        o.stdout
    };
    assert_eq!(
        id(&["inspect", "dd68-temporal-oracle", "--format", "{{.Image}}"]),
        id(&["image", "inspect", image, "--format", "{{.Id}}"])
    );
    assert_eq!(sql("show server_version_num"), "180000");
    let p = ParserSettings::default();
    let mut q =
        String::from("set timezone='UTC';set datestyle='ISO,YMD';set intervalstyle='postgres';\n");
    let mut e = Vec::new();
    macro_rules! check {
        ($ty:expr,$send:expr,$s:expr,$v:expr) => {{
            let s = $s;
            let v = $v;
            q += &format!(
                "select '{s}'::{}::text,encode({}('{s}'::{}),'hex');\n",
                $ty, $send, $ty
            );
            e.push(format!("{}|{}", v, hex(&v.to_binary())));
        }};
    }
    for s in [
        "2000-01-01",
        "1999-12-31",
        "0001-01-01 BC",
        "4714-11-24 BC",
        "5874897-12-31",
        "infinity",
        "-infinity",
    ] {
        check!("date", "date_send", s, Date::parse(s, &p).unwrap());
    }
    for s in [
        "00:00",
        "24:00",
        "23:59:59.9999999",
        "00:00:00.0000005",
        "00:00:00.0000015",
        "12:34:56.123456",
    ] {
        check!("time", "time_send", s, Time::parse(s).unwrap());
    }
    for s in [
        "2000-01-01 00:00",
        "1999-12-31 23:59:59.999999",
        "0001-01-01 00:00 BC",
        "294276-12-31 23:59:59.999999",
        "4714-11-24 00:00 BC",
        "2000-01-01 24:00",
        "infinity",
        "-infinity",
    ] {
        check!(
            "timestamp",
            "timestamp_send",
            s,
            Timestamp::parse(s, &p).unwrap()
        );
        check!(
            "timestamptz",
            "timestamptz_send",
            s,
            TimestampTz::parse(s, &p).unwrap()
        );
    }
    for s in [
        "12:00+01",
        "24:00-15:59:59",
        "00:00+15:59:59",
        "12:34:56.123456-05:30",
    ] {
        check!("timetz", "timetz_send", s, TimeTz::parse(s, &p).unwrap());
    }
    // Interval display intentionally tested as a round-trip expression; PG canonical output separately.
    for s in [
        "1 mon",
        "30 days",
        "1 day",
        "24:00",
        "-1 mons 2 days -03:04:05.123456",
        "2147483647 mons 2147483647 days 00:00",
        "infinity",
        "-infinity",
    ] {
        let v = Interval::parse(s).unwrap();
        q += &format!(
            "select encode(interval_send('{s}'::interval),'hex'),encode(interval_send('{}'::interval),'hex');\n",
            v
        );
        e.push(format!("{0}|{0}", hex(&v.to_binary())));
    }
    let mut seed = 0x68a918u64;
    for _ in 0..256 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let days = (seed % 2147483494) as i64 - 2451545;
        let date = Date::from_days(days as i32).unwrap();
        check!("date", "date_send", date.to_string(), date);
        let us = (seed % ((END_TIMESTAMP as i128 - i128::from(MIN_TIMESTAMP)) as u64)) as i128
            + i128::from(MIN_TIMESTAMP);
        let stamp = Timestamp::from_microseconds(us as i64).unwrap();
        check!("timestamp", "timestamp_send", stamp.to_string(), stamp);
        let time = Time::from_microseconds((seed % USECS_PER_DAY as u64) as i64).unwrap();
        check!("time", "time_send", time.to_string(), time);
        let interval = Interval::new(
            (seed >> 40) as i32 - 8388608,
            (seed >> 32) as i32,
            seed as i64,
        );
        check!("interval", "interval_send", interval.to_string(), interval);
    }
    let actual = sql(&q);
    let lines = actual.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), e.len());
    for (i, (a, b)) in lines.iter().zip(&e).enumerate() {
        assert_eq!(*a, b, "case {i}");
    }
    println!(
        "PG18: {} text/send and interval roundtrip comparisons",
        e.len()
    );
}
#[test]
#[ignore = "requires owned pinned dd68-temporal-oracle container"]
fn pg180_binary_receive_and_comparison() {
    assert_eq!(sql("show server_version_num"), "180000");
    let p = ParserSettings::default();
    let mut count = 0;
    let mut cases: Vec<(&str, Vec<u8>, bool)> = Vec::new();
    for v in [
        i32::MIN,
        -2451546,
        -2451545,
        -1,
        0,
        2145031948,
        2145031949,
        i32::MAX,
    ] {
        cases.push(("date", v.to_be_bytes().to_vec(), Date::from_days(v).is_ok()));
    }
    for ty in ["timestamp", "timestamptz", "time"] {
        for v in [
            i64::MIN,
            MIN_TIMESTAMP - 1,
            MIN_TIMESTAMP,
            -1,
            0,
            USECS_PER_DAY,
            USECS_PER_DAY + 1,
            END_TIMESTAMP - 1,
            END_TIMESTAMP,
            i64::MAX,
        ] {
            cases.push((
                ty,
                v.to_be_bytes().to_vec(),
                if ty == "time" {
                    Time::from_microseconds(v).is_ok()
                } else {
                    Timestamp::from_microseconds(v).is_ok()
                },
            ));
        }
    }
    for off in [-57600i32, -57599, 0, 57599, 57600, i32::MIN, i32::MAX] {
        let mut b = Time::parse("12:00").unwrap().to_binary().to_vec();
        b.extend(off.to_be_bytes());
        let ok = TimeTz::from_binary(&b).is_ok();
        cases.push(("timetz", b, ok));
    }
    for v in [
        Interval::INFINITY,
        Interval::NEG_INFINITY,
        Interval::new(i32::MAX, i32::MIN, i64::MIN),
        Interval::new(1, -1, -1),
    ] {
        cases.push(("interval", v.to_binary().to_vec(), true));
    }
    for ty in [
        "date",
        "timestamp",
        "timestamptz",
        "time",
        "timetz",
        "interval",
    ] {
        cases.push((ty, vec![0; 1], false));
    }
    for (ty, b, ok) in cases {
        sql(&format!(
            "drop table if exists temporal_recv;create table temporal_recv(x {ty});"
        ));
        let mut stream = b"PGCOPY\n\xff\r\n\0".to_vec();
        stream.extend([0; 8]);
        stream.extend(1i16.to_be_bytes());
        stream.extend((b.len() as i32).to_be_bytes());
        stream.extend(&b);
        stream.extend((-1i16).to_be_bytes());
        let mut c = Command::new("docker")
            .args([
                "exec",
                "-i",
                "dd68-temporal-oracle",
                "psql",
                "-U",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-Atqc",
                "copy temporal_recv from stdin binary",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        c.stdin.take().unwrap().write_all(&stream).unwrap();
        let o = c.wait_with_output().unwrap();
        assert_eq!(
            o.status.success(),
            ok,
            "{ty} {}: {}",
            hex(&b),
            String::from_utf8_lossy(&o.stderr)
        );
        if ok {
            let send = match ty {
                "timestamptz" => "timestamptz_send",
                _ => "",
            };
            let send = if send.is_empty() {
                format!("{ty}_send")
            } else {
                send.into()
            };
            assert_eq!(
                sql(&format!(
                    "select encode({send}(x),'hex') from temporal_recv"
                )),
                hex(&b)
            );
        }
        count += 1;
    }
    let mut q = String::new();
    let mut expected = Vec::new();
    let intervals = [
        Interval::new(1, 0, 0),
        Interval::new(0, 30, 0),
        Interval::new(0, 1, 0),
        Interval::new(0, 0, USECS_PER_DAY),
        Interval::new(-1, 31, -1),
        Interval::INFINITY,
        Interval::NEG_INFINITY,
    ];
    for a in intervals {
        for b in intervals {
            q += &format!(
                "select interval_cmp('{}'::interval,'{}'::interval);\n",
                a, b
            );
            expected.push((a.cmp(&b) as i32).to_string());
        }
    }
    for a in ["12:00+01", "11:00+00", "24:00-15", "00:00+15"] {
        for b in ["12:00+01", "11:00+00", "24:00-15", "00:00+15"] {
            q += &format!("select timetz_cmp('{a}'::timetz,'{b}'::timetz);\n");
            expected.push(
                (TimeTz::parse(a, &p)
                    .unwrap()
                    .cmp(&TimeTz::parse(b, &p).unwrap()) as i32)
                    .to_string(),
            );
        }
    }
    assert_eq!(
        sql(&q).lines().collect::<Vec<_>>(),
        expected.iter().map(String::as_str).collect::<Vec<_>>()
    );
    for style in [IntervalStyle::Postgres, IntervalStyle::SqlStandard] {
        let setting = if style == IntervalStyle::Postgres {
            "postgres"
        } else {
            "sql_standard"
        };
        for s in [
            "-1 days 02:00",
            "-1 days +02:00",
            "1 mons -2 days 03:00",
            "-1 mons 2 days 03:00",
        ] {
            let v = Interval::parse_with_style(s, style).unwrap();
            assert_eq!(
                sql(&format!(
                    "set intervalstyle='{setting}';select encode(interval_send('{s}'::interval),'hex');"
                )),
                hex(&v.to_binary())
            );
        }
    }
    println!(
        "PG18: {count} COPY receive/range cases, {} comparisons, 8 sign-style cases",
        expected.len()
    );
}
