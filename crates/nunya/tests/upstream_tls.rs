//! Opt-in TLS-only upstream qualification; never prints database values or credentials.
use super::common::{ServeHarness, ServeResult, file_sink_policy};
use nunya::state::model::Mode;
use std::process::{Command, Output};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct Env {
    ssl: String,
    plain: String,
    ca: String,
    wrong_ca: String,
    python: String,
}
impl Env {
    fn load() -> ServeResult<Self> {
        fn required(name: &str) -> ServeResult<String> {
            std::env::var(name)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("required live TLS variable {name} is unset").into())
        }
        Ok(Self {
            ssl: required("NUNYA_TEST_PGSSL")?,
            plain: required("NUNYA_TEST_PG")?,
            ca: required("NUNYA_TEST_PGSSL_CA")?,
            wrong_ca: required("NUNYA_TEST_PGSSL_WRONG_CA")?,
            python: required("NUNYA_TEST_PYTHON")?,
        })
    }
    fn harness(&self, mode: &str, ca: Option<&str>, host: &str) -> ServeResult<ServeHarness> {
        let mut h = ServeHarness::new(&file_sink_policy(), Some(&self.ssl), None, None, "")?;
        h.set_upstream(&self.ssl, mode, ca, Some(host))?;
        Ok(h)
    }
}

#[derive(Default)]
struct Checks {
    pass: usize,
    fail: usize,
}
impl Checks {
    fn check(&mut self, name: &str, ok: bool) {
        if ok {
            self.pass += 1;
        } else {
            self.fail += 1;
        }
        eprintln!("{} {name}", if ok { "PASS" } else { "FAIL" });
    }
    fn finish(&self, name: &str) {
        eprintln!("SUMMARY {name} pass={} fail={}", self.pass, self.fail);
        assert_eq!(self.fail, 0, "live TLS qualification failed");
    }
}
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn cells(client: &Client, sql: &str) -> ServeResult<Vec<Vec<String>>> {
    let mut rows = vec![];
    for message in client.simple_query(sql).await? {
        if let SimpleQueryMessage::Row(row) = message {
            rows.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned).ok_or("unexpected NULL"))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(rows)
}
async fn scalar(client: &Client, sql: &str) -> ServeResult<String> {
    cells(client, sql)
        .await?
        .first()
        .and_then(|r| r.first())
        .cloned()
        .ok_or_else(|| "expected one cell".into())
}
fn python(env: &Env, script: &str, app: &str) -> ServeResult<Output> {
    Ok(Command::new(&env.python)
        .args(["-I", "-c", script])
        .env("TLS_TEST_URL", &env.ssl)
        .env("TLS_TEST_APP", app)
        .output()?)
}

#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_plaintext_refused_by_server() -> ServeResult<()> {
    let env = Env::load()?;
    let result = tokio_postgres::connect(&env.ssl, NoTls).await;
    let mut c = Checks::default();
    let refused = match result {
        Err(e) => e.code().is_some_and(|code| code.code() == "28000"),
        Ok((_, connection)) => {
            drop(connection);
            false
        }
    };
    c.check("plaintext-refused-by-server", refused);
    c.finish("live_tls_plaintext_refused_by_server");
    Ok(())
}

async fn served(env: &Env, mode: &str, c: &mut Checks) -> ServeResult<()> {
    let mut h = env.harness(
        mode,
        (mode == "verify-full").then_some(env.ca.as_str()),
        "localhost",
    )?;
    h.start().await?;
    let (session, secret, _) = h
        .acquire_session("tls-agent", "postgres", Mode::ReadWrite)
        .await?;
    let (client, driver) = h.pg_connect(&session, &secret, "app").await?;
    let _driver = Driver(driver);
    let rows = cells(&client, "SELECT email, phone FROM customers WHERE id = 1").await?;
    let row = rows.first().ok_or("missing protected row")?;
    let token = row.first().ok_or("missing protected cell")?;
    c.check(
        "protected-read",
        token.starts_with("tok_") && row.get(1).is_some_and(|v| v == "[hidden]"),
    );
    let marker = h
        .dir()
        .file_name()
        .ok_or("missing fixture name")?
        .to_string_lossy();
    let insert = format!(
        "INSERT INTO outbox(to_address, template) VALUES ('{}', '{marker}')",
        token.replace('\'', "''")
    );
    let write = client.simple_query(&insert).await;
    c.check("blind-write", write.is_ok());
    let read = scalar(
        &client,
        &format!(
            "SELECT to_address FROM outbox WHERE template = '{marker}' ORDER BY id DESC LIMIT 1"
        ),
    )
    .await;
    c.check("blind-read-back", read.is_ok_and(|v| v == *token));
    // Prefer observing the actual connection, otherwise use a uniquely named backend.
    let tls = scalar(
        &client,
        "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
    )
    .await;
    let tls_ok = match tls {
        Ok(value) => {
            eprintln!("TLS-PROOF gateway-query");
            value == "t" || value == "true"
        }
        Err(_) => {
            eprintln!("TLS-PROOF direct-psycopg-other-backends");
            let output = python(
                env,
                r#"import os, psycopg
with psycopg.connect(os.environ['TLS_TEST_URL'], sslmode='require', gssencmode='disable', connect_timeout=5, options='-c statement_timeout=5000') as c:
    row = c.execute("SELECT count(*) > 0 AND bool_and(s.ssl) FROM pg_stat_ssl s JOIN pg_stat_activity a USING (pid) WHERE a.usename = current_user AND a.pid <> pg_backend_pid() AND a.application_name = %s", (os.environ['TLS_TEST_APP'],)).fetchone()
    print('OK' if row[0] is True else 'FAIL')
"#,
                &marker,
            )?;
            output.status.success() && output.stdout == b"OK\r\n"
                || output.status.success() && output.stdout == b"OK\n"
        }
    };
    c.check("upstream-used-tls", tls_ok);
    client
        .simple_query(&format!("DELETE FROM outbox WHERE template = '{marker}'"))
        .await?;
    Ok(())
}
#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_require_serves() -> ServeResult<()> {
    let env = Env::load()?;
    let mut c = Checks::default();
    let result = served(&env, "require", &mut c).await;
    c.check("require-serves", result.is_ok());
    c.finish("live_tls_require_serves");
    Ok(())
}
#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_verify_full_serves() -> ServeResult<()> {
    let env = Env::load()?;
    let mut c = Checks::default();
    let result = served(&env, "verify-full", &mut c).await;
    c.check("verify-full-serves", result.is_ok());
    c.finish("live_tls_verify_full_serves");
    Ok(())
}

async fn refusal(
    env: &Env,
    name: &str,
    mode: &str,
    ca: Option<&str>,
    host: &str,
    expected_class: &str,
) -> ServeResult<()> {
    let mut c = Checks::default();
    let mut h = env.harness(mode, ca, host)?;
    let startup = tokio::time::timeout(std::time::Duration::from_secs(20), h.start())
        .await
        .map_err(|_| "refusal startup timed out")?;
    // Management may become ready just before the catalog connection refuses startup.
    for _ in 0..40 {
        if h.has_exited()? {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let mut nothing_served = h.has_exited()?;
    if startup.is_ok() && !nothing_served {
        let credentials = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            h.acquire_session("refused-agent", "postgres", Mode::ReadWrite),
        )
        .await;
        match credentials {
            Ok(Ok((session, secret, _))) => {
                // A successful login is itself a failure, even if a query later refuses.
                nothing_served = h.pg_connect(&session, &secret, "app").await.is_err();
            }
            _ => nothing_served = h.has_exited()?,
        }
    }
    h.stop()?;
    let mut output = String::new();
    for entry in std::fs::read_dir(h.dir())? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "log") {
            output.push_str(&std::fs::read_to_string(path)?);
        }
    }
    let config: tokio_postgres::Config = env.ssl.parse().map_err(|_| "invalid test URL")?;
    let password = config.get_password().ok_or("test URL needs password")?;
    let password = std::str::from_utf8(password)?;
    let safe = !output.contains(password)
        && !output.to_lowercase().contains("alice")
        && !output.contains("BEGIN CERTIFICATE")
        && !output.contains("END CERTIFICATE")
        && !output.contains("tok_");
    // Check base64 certificate bodies as well as PEM delimiters.
    let mut no_certificate_bytes = true;
    for file in [&env.ca, &env.wrong_ca] {
        for line in std::fs::read_to_string(file)?
            .lines()
            .filter(|l| !l.starts_with('-') && !l.is_empty())
        {
            no_certificate_bytes &= !output.contains(line);
        }
    }
    c.check(name, nothing_served);
    let expected = format!("upstream schema connection failed: {expected_class}");
    c.check("classified-startup-refusal", output.lines().any(|line| line == expected));
    c.check(
        "refusal-output-data-free",
        safe && no_certificate_bytes && !output.trim().is_empty(),
    );
    if safe && no_certificate_bytes {
        std::fs::write(h.dir().join(format!("{name}.txt")), &output)?;
    }
    c.finish(name);
    Ok(())
}
#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_verify_full_wrong_ca_refused() -> ServeResult<()> {
    let env = Env::load()?;
    refusal(
        &env,
        "verify-full-wrong-ca-refused",
        "verify-full",
        Some(&env.wrong_ca),
        "localhost",
        "tls certificate not trusted",
    )
    .await
}
#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_verify_full_hostname_mismatch_refused() -> ServeResult<()> {
    let env = Env::load()?;
    refusal(
        &env,
        "verify-full-hostname-mismatch-refused",
        "verify-full",
        Some(&env.ca),
        "127.0.0.1",
        "tls hostname mismatch",
    )
    .await
}
#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_disable_refused_no_fallback() -> ServeResult<()> {
    let env = Env::load()?;
    refusal(
        &env,
        "disable-refused-no-fallback",
        "disable",
        None,
        "localhost",
        "server refused login sqlstate 28000",
    )
    .await
}

async fn identity(env: &Env, c: &mut Checks) -> ServeResult<()> {
    let mut h = ServeHarness::new(&file_sink_policy(), Some(&env.plain), None, None, "")?;
    h.start().await?;
    let (session, secret, _) = h
        .acquire_session("same-scope", "postgres", Mode::ReadWrite)
        .await?;
    let (client, driver) = h.pg_connect(&session, &secret, "app").await?;
    let driver = Driver(driver);
    let sql = "SELECT email FROM customers WHERE id = 1";
    let explicit = "SELECT nunya_tokenize('email', 'tls-fixed@example.test')";
    let before = scalar(&client, sql).await?;
    let explicit_before = scalar(&client, explicit).await?;
    drop(client);
    drop(driver);
    h.stop()?;
    h.set_upstream(&env.ssl, "verify-full", Some(&env.ca), Some("localhost"))?;
    h.start().await?;
    let (client, driver) = h.pg_connect(&session, &secret, "app").await?;
    let _driver = Driver(driver);
    let after = scalar(&client, sql).await?;
    let explicit_after = scalar(&client, explicit).await?;
    c.check(
        "tokens-identical-tls-vs-plain",
        before.starts_with("tok_") && before == after,
    );
    c.check(
        "explicit-tokens-identical",
        explicit_before.starts_with("tok_") && explicit_before == explicit_after,
    );
    Ok(())
}
#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_tokens_identical_tls_vs_plain() -> ServeResult<()> {
    let env = Env::load()?;
    let mut c = Checks::default();
    let result = identity(&env, &mut c).await;
    c.check("same-session-resumed", result.is_ok());
    c.finish("live_tls_tokens_identical_tls_vs_plain");
    Ok(())
}

async fn exec(env: &Env) -> ServeResult<bool> {
    let mut h = env.harness("verify-full", Some(&env.ca), "localhost")?;
    h.start().await?;
    let (_, _, principal_secret) = h
        .acquire_session("exec-agent", "postgres", Mode::ReadWrite)
        .await?;
    let config: toml::Value =
        toml::from_str(&std::fs::read_to_string(h.dir().join("nunya.toml"))?)?;
    let management = config["management"]["listen"]
        .as_str()
        .ok_or("missing management address")?;
    let script = r#"import psycopg
with psycopg.connect(dbname='app', sslmode='disable') as c:
    token = c.execute('SELECT email FROM customers WHERE id = 1').fetchone()[0]
    row = c.execute('SELECT email FROM customers WHERE email = %s', (token,)).fetchone()
    if not token.startswith('tok_') or row is None or row[0] != token:
        raise SystemExit(1)
    print('OK', token.startswith('tok_'))
"#;
    let output = Command::new(env!("CARGO_BIN_EXE_nunya"))
        .args([
            "exec",
            "--server",
            &format!("http://{management}"),
            "--principal",
            "exec-agent",
            "--context",
            "tls-exec",
            "--route",
            "postgres",
            "--pg-host",
            "127.0.0.1",
            "--pg-port",
            &h.pg_port().to_string(),
            "--",
            &env.python,
            "-I",
            "-c",
            script,
        ])
        .env("NUNYA_HOME", h.dir().join("helper"))
        .env("NUNYA_PRINCIPAL_SECRET", principal_secret.secret.as_str())
        .output()?;
    Ok(output.status.success()
        && matches!(output.stdout.as_slice(), b"OK True\r\n" | b"OK True\n")
        && output.stderr.is_empty())
}
#[tokio::test]
#[ignore = "requires live TLS upstream environment"]
async fn live_tls_psycopg_exec_over_tls_upstream() -> ServeResult<()> {
    let env = Env::load()?;
    let mut c = Checks::default();
    c.check(
        "psycopg-exec-over-tls-upstream",
        exec(&env).await.is_ok_and(|ok| ok),
    );
    c.finish("live_tls_psycopg_exec_over_tls_upstream");
    Ok(())
}
