use giggles_pg_uuid::Uuid;
use std::{
    io::Write,
    process::{Command, Stdio},
};
fn corpus() -> Vec<Uuid> {
    let mut seed = 0xdd68_9562_1800_0001u64;
    let mut out = vec![Uuid::from_bytes([0; 16]), Uuid::from_bytes([255; 16])];
    for n in 0..768 {
        let mut b = [0; 16];
        for v in &mut b {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *v = seed as u8;
        }
        b[6] = (b[6] & 15) | ((n % 16) as u8) << 4;
        b[8] = (b[8] & 63) | (((n / 16) % 4) as u8) << 6;
        out.push(Uuid::from_bytes(b));
    }
    for version in [1, 7] {
        for byte in [0, 255] {
            let mut b = [byte; 16];
            b[6] = (b[6] & 15) | (version << 4);
            b[8] = (b[8] & 63) | 128;
            out.push(Uuid::from_bytes(b));
        }
    }
    out
}
fn spelling(u: Uuid, mask: u8, braces: bool) -> String {
    let hex = u
        .to_binary()
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<String>();
    let mut s = String::new();
    if braces {
        s.push('{');
    }
    for (i, c) in hex.chars().enumerate() {
        if i > 0 && i % 4 == 0 && mask & (1 << ((i / 4) - 1)) != 0 {
            s.push('-');
        }
        s.push(c);
    }
    if braces {
        s.push('}');
    }
    s
}
#[test]
fn seeded_roundtrip_order_and_spellings() {
    assert_eq!(std::mem::size_of::<Uuid>(), 16);
    let mut cases = corpus();
    for (i, u) in cases.iter().copied().enumerate() {
        assert_eq!(u.to_string().parse::<Uuid>().unwrap(), u);
        assert_eq!(Uuid::from_binary(&u.to_binary()).unwrap(), u);
        assert_eq!(spelling(u, i as u8, i % 2 == 0).parse::<Uuid>().unwrap(), u);
        let v = Uuid::v4_from_entropy(u.to_binary());
        assert_eq!(v.extract_version(), Some(4));
        assert_eq!(v.extract_timestamp(), None);
    }
    cases.sort();
    for p in cases.windows(2) {
        assert!(p[0].to_binary() <= p[1].to_binary());
        assert!(p[0].to_string() <= p[1].to_string());
    }
    for len in 0..40 {
        if len != 16 {
            assert!(Uuid::from_binary(&vec![0; len]).is_err());
        }
    }
}
#[test]
fn invalid_and_gates() {
    for s in invalid() {
        assert!(s.parse::<Uuid>().is_err(), "{s}");
    }
    for variant in 0..=255 {
        for version in 0..16 {
            let mut b = [0; 16];
            b[8] = variant;
            b[6] = version << 4;
            let u = Uuid::from_bytes(b);
            assert_eq!(
                u.extract_version(),
                if variant & 192 == 128 {
                    Some(i16::from(version))
                } else {
                    None
                }
            );
            assert_eq!(
                u.extract_timestamp().is_some(),
                variant & 192 == 128 && [1, 7].contains(&version)
            );
        }
    }
    let a: Uuid = "00000000-0000-7000-8000-000000000000".parse().unwrap();
    let b: Uuid = "00000000-0000-7fff-8000-000000000000".parse().unwrap();
    assert_eq!(a.extract_timestamp(), b.extract_timestamp());
    assert_eq!(
        a.extract_timestamp().unwrap().microseconds(),
        -946_684_800_000_000
    );
}
fn invalid() -> Vec<String> {
    [
        "",
        " ",
        "0000000000000000000000000000000",
        "000000000000000000000000000000000",
        "{00000000000000000000000000000000",
        "00000000000000000000000000000000}",
        "00000000000000000000000000000000-",
        "-00000000000000000000000000000000",
        "00-000000000000000000000000000000",
        "0000--0000000000000000000000000000",
        "0000000000000000000000000000000g",
        "urn:uuid:00000000-0000-0000-0000-000000000000",
        " 00000000000000000000000000000000",
        "00000000000000000000000000000000 ",
        "é0000000000000000000000000000000",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain([
        String::from("0").repeat(1_000_000),
        String::from("00000000000000000000000000000000\0"),
    ])
    .collect()
}
const IMAGE: &str =
    "postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c";
struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}
fn sql(c: &Container, sql: &str) -> String {
    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            &c.0,
            "psql",
            "-XAt",
            "-U",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(sql.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().replace("\r", "")
}
#[test]
#[ignore = "opt-in Docker pinned PostgreSQL 18 oracle"]
fn live_pg18() {
    let name = format!("dd68-uuid-{}", std::process::id());
    let o = Command::new("docker")
        .args([
            "run",
            "--pull=never",
            "-d",
            "--name",
            &name,
            "--network=none",
            "--read-only",
            "--tmpfs",
            "/var/lib/postgresql:rw",
            "--tmpfs",
            "/tmp:rw",
            "--tmpfs",
            "/var/run/postgresql:rw",
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            IMAGE,
        ])
        .output()
        .unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let c = Container(name);
    let mut ready = false;
    for _ in 0..100 {
        let o = Command::new("docker")
            .args(["exec", &c.0, "pg_isready", "-U", "postgres"])
            .output()
            .unwrap();
        if o.status.success() {
            ready = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(ready);
    assert_eq!(sql(&c, "SHOW server_version_num;").trim(), "180000");
    let mut queries = String::from("SET timezone='UTC';");
    let mut expected = String::from("SET\n");
    let cases = corpus();
    for (i, u) in cases.iter().copied().enumerate() {
        let s = spelling(u, i as u8, i % 2 == 0);
        let seed = if i % 2 == 0 { 0 } else { -123456789i64 };
        let prev = cases[i.saturating_sub(1)];
        queries.push_str(&format!("SELECT '{s}'::uuid::text, encode(uuid_send('{s}'),'hex'),coalesce(uuid_extract_version('{s}')::text,'NULL'),coalesce(((extract(epoch from uuid_extract_timestamp('{s}'))*1000000)::bigint-946684800000000)::text,'NULL'),uuid_hash('{s}'),uuid_hash_extended('{s}',{seed}),'{s}'::uuid < '{prev}'::uuid;"));
        let hex = u
            .to_binary()
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        expected.push_str(&format!(
            "{u}|{hex}|{}|{}|{}|{}|{}\n",
            u.extract_version()
                .map_or("NULL".to_owned(), |v| v.to_string()),
            u.extract_timestamp()
                .map_or("NULL".to_owned(), |v| v.microseconds().to_string()),
            u.pg_hash_le(),
            u.pg_hash_extended_le(seed as u64),
            if u < prev { "t" } else { "f" }
        ));
    }
    let actual = sql(&c, &queries);
    for (i, (a, b)) in actual.lines().zip(expected.lines()).enumerate() {
        assert_eq!(a, b, "row {i}");
    }
    assert_eq!(actual.lines().count(), expected.lines().count());
    for s in invalid()
        .into_iter()
        .filter(|s| !s.contains('\0') && s.len() < 1000)
    {
        assert_eq!(
            sql(
                &c,
                &format!("SELECT (pg_input_error_info('{s}','uuid')).sql_error_code;")
            )
            .trim(),
            "22P02"
        );
    }
    assert_eq!(sql(&c,"SELECT uuid_extract_version(NULL::uuid) IS NULL,uuid_extract_timestamp(NULL::uuid) IS NULL,uuid_send(NULL::uuid) IS NULL;").trim(),"t|t|t");
    sql(&c, "CREATE TABLE bin(v uuid);");
    for len in [0, 15, 16, 17, 32] {
        let mut data = b"PGCOPY\n\xff\r\n\0".to_vec();
        data.extend_from_slice(&[0; 8]);
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&(len as i32).to_be_bytes());
        data.extend(vec![0; len]);
        data.extend_from_slice(&(-1i16).to_be_bytes());
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                &c.0,
                "psql",
                "-X",
                "-U",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-v",
                "VERBOSITY=verbose",
                "-c",
                "COPY bin FROM STDIN BINARY",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&data).unwrap();
        let o = child.wait_with_output().unwrap();
        if len == 16 {
            assert!(o.status.success());
        } else {
            assert!(!o.status.success());
            assert!(
                String::from_utf8_lossy(&o.stderr).contains(if len < 16 {
                    "08P01"
                } else {
                    "22P03"
                }),
                "{}",
                String::from_utf8_lossy(&o.stderr)
            );
        }
    }
    assert_eq!(
        sql(&c, "SELECT v::text FROM bin;").trim(),
        "00000000-0000-0000-0000-000000000000"
    );
}
