//! Real PostgreSQL equivalence and optional live GigglesDB dialect qualification.
#[path = "support/root.rs"]
mod test_root;

use skedaddle::{mapping::PgType, verify::sql::SqlDialect};
use std::io::Write;
use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Server {
    child: Child,
    root: PathBuf,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        fs::remove_dir_all(&self.root).expect("remove live server data");
    }
}
async fn connect(dsn: &str) -> Result<Client> {
    let (client, driver) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = driver.await;
    });
    Ok(client)
}
async fn fixture(client: &Client) -> Result {
    client.batch_execute("CREATE TABLE n5b (a integer, b integer, c char(5), payload text, PRIMARY KEY(a,b)); INSERT INTO n5b VALUES (1,3,'é猫',NULL),(1,2,'x','hello'),(2,1,'',''),(3,1,NULL,'z'),(4,1,'é猫abc',NULL)").await?;
    Ok(())
}
fn projections(dialect: SqlDialect) -> skedaddle::verify::Result<Vec<String>> {
    [
        ("v.a", PgType::Int4),
        ("v.b", PgType::Int4),
        (
            "v.c",
            PgType::Char {
                encoding: "UTF8".into(),
            },
        ),
        (
            "v.payload",
            PgType::Text {
                encoding: "UTF8".into(),
            },
        ),
    ]
    .iter()
    .map(|(expr, ty)| dialect.size_expression(expr, ty, Some(9)))
    .collect()
}
async fn budget(client: &Client, dialect: SqlDialect, lower: &str) -> Result<(String, String)> {
    let sql = dialect.size_sql(
        &projections(dialect)?,
        " FROM n5b v WHERE v.a >= $1::text::integer",
    );
    let row = client.query_one(&sql, &[&lower]).await?;
    Ok((row.get(0), row.get(1)))
}
async fn char_values(client: &Client, dialect: SqlDialect) -> Result {
    let ty = PgType::Char {
        encoding: "UTF8".into(),
    };
    let expr = dialect.text_expression("c", &ty);
    let bytes = dialect.size_expression("c", &ty, Some(9))?;
    let rows = client
        .query(
            &format!("SELECT {expr}, octet_length({bytes})::text FROM n5b ORDER BY a ASC,b DESC"),
            &[],
        )
        .await?;
    for (row, (value, len)) in rows.iter().zip([
        (Some("é猫   "), Some("8")),
        (Some("x    "), Some("5")),
        (Some("     "), Some("5")),
        (None, None),
        (Some("é猫abc"), Some("8")),
    ]) {
        assert_eq!(row.get::<_, Option<&str>>(0), value);
        assert_eq!(row.get::<_, Option<&str>>(1), len);
    }
    assert_eq!(rows.len(), 5);
    Ok(())
}
async fn sample(client: &Client, stride: u64, cap: u64) -> Result<Option<(String, String)>> {
    let row = SqlDialect::stream_sample(
        client,
        &SqlDialect::ordered_sample_sql(
            "a::text, b::text",
            " FROM n5b WHERE a >= $1::text::integer",
            "a ASC, b DESC",
        ),
        &[&"1"],
        stride,
        cap,
    )
    .await?;
    Ok(row.map(|r| (r.get(0), r.get(1))))
}
#[tokio::test]
async fn postgres_dialect_equivalence() -> Result {
    let Ok(dsn) = std::env::var("SKEDADDLE_VERIFY_PG_URL") else {
        writeln!(
            std::io::stderr(),
            "SKIPPED PostgreSQL equivalence: set SKEDADDLE_VERIFY_PG_URL from harness PG (no DB assertions ran)"
        )?;
        return Ok(());
    };
    let client = connect(&dsn).await?;
    fixture(&client).await?;
    for lower in ["1", "2", "9"] {
        assert_eq!(
            budget(&client, SqlDialect::Postgres, lower).await?,
            budget(&client, SqlDialect::Gigglesdb, lower).await?
        );
    }
    for stride in [1, 2, 3, 4, 5, 9] {
        let sql = format!(
            "SELECT a::text,b::text FROM (SELECT a,b,row_number() OVER (ORDER BY a ASC,b DESC) AS rn FROM n5b WHERE a>=1) s WHERE rn>1 AND (rn-1)%{stride}=0 ORDER BY rn LIMIT 1"
        );
        let expected = client
            .query_opt(&sql, &[])
            .await?
            .map(|r| (r.get(0), r.get(1)));
        assert_eq!(sample(&client, stride, 18).await?, expected);
        assert!(sample(&client, stride, 17).await?.is_none());
    }
    for dialect in [SqlDialect::Postgres, SqlDialect::Gigglesdb] {
        char_values(&client, dialect).await?;
        let expr = dialect.text_expression(
            "c",
            &PgType::Char {
                encoding: "UTF8".into(),
            },
        );
        let row = client
            .query_one(&format!("SELECT {expr} FROM n5b WHERE a=1 AND b=3"), &[])
            .await?;
        assert_eq!(row.get::<_, &str>(0), "é猫   ");
    }
    client.batch_execute("DROP TABLE n5b").await?;
    Ok(())
}
#[tokio::test]
async fn live_gigglesdb_dialect() -> Result {
    let Some(binary) = std::env::var_os("GIGGLES_SERVER_BIN") else {
        writeln!(
            std::io::stderr(),
            "SKIPPED live GigglesDB: set GIGGLES_SERVER_BIN (no DB assertions ran)"
        )?;
        return Ok(());
    };
    let root = crate::test_root::scratch_root().join(format!("n5b-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let child = Command::new(binary)
        .args(["--dev", "--trust-local", "--data"])
        .arg(root.join("data"))
        .args(["--port", &port.to_string()])
        .env("GIGGLES_OWNER_PASSWORD", uuid::Uuid::new_v4().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut server = Server { child, root };
    let dsn = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let client = loop {
        if let Some(status) = server.child.try_wait()? {
            return Err(format!("server exited: {status}").into());
        }
        if let Ok(Ok(client)) = tokio::time::timeout(Duration::from_secs(1), connect(&dsn)).await {
            break client;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("server startup timeout".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    fixture(&client).await?;
    char_values(&client, SqlDialect::Gigglesdb).await?;
    assert_eq!(
        budget(&client, SqlDialect::Gigglesdb, "1").await?,
        ("5".into(), "202".into())
    );
    assert_eq!(
        budget(&client, SqlDialect::Gigglesdb, "9").await?,
        ("0".into(), "0".into())
    );
    for (stride, expected) in [
        (1, Some(("1", "2"))),
        (2, Some(("2", "1"))),
        (4, Some(("4", "1"))),
        (9, None),
    ] {
        assert_eq!(
            sample(&client, stride, 18).await?,
            expected.map(|(a, b)| (a.into(), b.into()))
        );
        assert!(sample(&client, stride, 17).await?.is_none());
    }
    let row = client
        .query_one("SELECT c FROM n5b WHERE a=1 AND b=3", &[])
        .await?;
    assert_eq!(row.get::<_, &str>(0), "é猫   ");
    Ok(())
}
