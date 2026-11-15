//! Opt-in end-to-end PostgreSQL gateway worked example.
use std::{net::TcpListener, path::PathBuf, time::Duration};
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Abort<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Abort<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG pointing at harness/seed.sql database"]
async fn standing_worked_example() {
    let Ok(url) = std::env::var("NUNYA_TEST_PG") else {
        return;
    };
    let pg: tokio_postgres::Config = url.parse().unwrap();
    let host = match pg.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(host)) => host,
        _ => panic!("TCP upstream required"),
    };
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let dir = std::env::temp_dir().join(format!(
        "serve-pg-{}",
        data_encoding::HEXLOWER.encode(&nonce)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let _cleanup = Cleanup(dir.clone());
    std::fs::write(dir.join("root.key"), "ab".repeat(32)).unwrap();
    std::fs::write(dir.join("password"), pg.get_password().unwrap_or_default()).unwrap();
    let policy = include_str!("../harness/policy.toml")
        .replace("sink = \"stdout\"", "sink = \"file\"\npath = \"read.log\"");
    std::fs::write(dir.join("policy.toml"), policy).unwrap();
    let listen = port();
    let admin = port();
    let verifier = nunya::pgwire::auth::ScramVerifier::from_password(
        "standing-test",
        b"serve-test-salt",
        4096,
    )
    .to_pg_string();
    let mut upstream = toml::map::Map::new();
    for (key, value) in [
        ("host", host.as_str()),
        ("user", pg.get_user().unwrap_or("postgres")),
        ("dbname", pg.get_dbname().unwrap_or("app")),
        ("password_file", "password"),
        ("tls", "disable"),
    ] {
        upstream.insert(key.into(), toml::Value::String(value.into()));
    }
    upstream.insert(
        "port".into(),
        toml::Value::Integer(i64::from(*pg.get_ports().first().unwrap_or(&5432))),
    );
    let text = format!(
        "listen = '127.0.0.1:{listen}'\nadmin_listen = '127.0.0.1:{admin}'\npolicy = 'policy.toml'\nstate_dir = 'state'\nroot_key_file = 'root.key'\nallow_plaintext = true\n[[standing]]\nname = 'agent'\nverifier = '{verifier}'\n[upstream]\n{}",
        toml::to_string(&upstream).unwrap()
    );
    let path = dir.join("nunya.toml");
    std::fs::write(&path, text).unwrap();
    let cfg = nunya::config::Config::load(&path).unwrap();
    let server = Abort(tokio::spawn(nunya::serve::run(cfg)));
    let mut connected = None;
    for _ in 0..100 {
        if server.0.is_finished() {
            panic!("gateway stopped during startup");
        }
        let result = tokio_postgres::connect(&format!("host=127.0.0.1 port={listen} user=agent password=standing-test dbname=app sslmode=disable"), tokio_postgres::NoTls).await;
        if let Ok(pair) = result {
            connected = Some(pair);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (client, connection) = connected.expect("gateway ready");
    let _driver = Abort(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let rows = client
        .simple_query("SELECT email, phone FROM customers WHERE id = 1")
        .await;
    assert!(rows.is_ok(), "protected read succeeds");
    let rows = rows.unwrap();
    let row = rows
        .iter()
        .find_map(|r| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = r {
                Some(r)
            } else {
                None
            }
        })
        .unwrap();
    let token = row.get(0).unwrap();
    assert!(row.get(1) == Some("[hidden]"), "phone is masked");
    assert!(token != "alice@example.test", "email is tokenized");
    let marker = format!(
        "serve-worked-example-{}",
        data_encoding::HEXLOWER.encode(&nonce)
    );
    let insert = format!(
        "INSERT INTO outbox(to_address, template) VALUES ('{}', '{marker}')",
        token.replace('\'', "''")
    );
    assert!(
        client.simple_query(&insert).await.is_ok(),
        "token transfers across email domain"
    );
    let refusal = client
        .simple_query("SELECT lower(email) FROM customers")
        .await;
    assert!(refusal.is_err(), "blind expression is refused");
    let (direct, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let _direct_driver = Abort(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let row = direct.query_one("SELECT to_address = 'alice@example.test' FROM outbox WHERE template = $1 ORDER BY id DESC LIMIT 1", &[&marker]).await.unwrap();
    assert!(row.get::<_, bool>(0), "upstream stores original value");
    direct
        .execute("DELETE FROM outbox WHERE template = $1", &[&marker])
        .await
        .unwrap();
}
