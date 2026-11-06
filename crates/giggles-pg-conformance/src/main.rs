use giggles_pg_conformance::*;
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

type Error = Box<dyn std::error::Error>;
#[derive(Serialize)]
struct Report {
    schema: u32,
    scope: String,
    identities: Vec<Identity>,
    observed_repository_sources: Vec<Identity>,
    profiles: Vec<Profile>,
    errors: Vec<String>,
    verdict: Status,
    summary: Summary,
}
#[derive(Default, Serialize)]
struct Summary {
    profiles: usize,
    pass: usize,
    fail: usize,
    untested: usize,
}
#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|s| s == "--tokio-probe") {
        if args.len() != 3 {
            std::process::exit(2);
        }
        if let Err(e) = tokio_probe(Path::new(&args[1]), &args[2]).await {
            eprintln!("{e}");
            std::process::exit(1);
        }
        return;
    }
    if args.len() != 1 {
        eprintln!("usage: giggles-pg-conformance ABSOLUTE-MANIFEST.json");
        std::process::exit(2);
    }
    match execute(Path::new(&args[0])).await {
        Ok((status, path)) => {
            println!("verdict={status:?} report={}", path.display());
            if status != Status::Pass {
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("harness error: {e}");
            std::process::exit(2);
        }
    }
}
async fn execute(path: &Path) -> Result<(Status, PathBuf), Error> {
    let m = Manifest::read(path)?;
    // Exclusive creation refuses old output and any attempt to place results into a fixture.
    let fixture = m.fixture.canonicalize()?;
    let parent = m
        .output
        .parent()
        .ok_or("output needs parent")?
        .canonicalize()?;
    if parent.starts_with(&fixture) {
        return Err("output cannot be inside supplied fixture".into());
    }
    std::fs::create_dir(&m.output)?;
    // Java's classpath parser does not accept Windows extended-length \\?\ prefixes.
    let output = external_path(&m.output.canonicalize()?);
    std::fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&serde_json::from_slice::<serde_json::Value>(&bounded(
            path, 65536,
        )?)?)?,
    )?;
    let mut report=Report{schema:2,scope:"Actual driver cases only; no DataGrip GUI or production qualification. Repository sources are runtime observations, not build provenance. No PostgreSQL comparative control. UNTESTED prevents a supported verdict.".into(),identities:vec![],observed_repository_sources:vec![],profiles:vec![],errors:vec![],verdict:Status::Untested,summary:Summary::default()};
    if let Err(e) = matrix(&m, &output, &mut report).await {
        report.errors.push(e.to_string());
    }
    report.verdict =
        if !report.errors.is_empty() || report.profiles.iter().any(|p| p.verdict == Status::Fail) {
            Status::Fail
        } else if report.profiles.is_empty()
            || report
                .profiles
                .iter()
                .any(|p| p.verdict == Status::Untested)
        {
            Status::Untested
        } else {
            Status::Pass
        };
    report.summary.profiles = report.profiles.len();
    for case in report.profiles.iter().flat_map(|p| &p.cases) {
        match case.status {
            Status::Pass => report.summary.pass += 1,
            Status::Fail => report.summary.fail += 1,
            Status::Untested => report.summary.untested += 1,
        }
    }
    let dest = output.join("report.json");
    std::fs::write(&dest, serde_json::to_vec_pretty(&report)?)?;
    Ok((report.verdict, dest))
}
fn external_path(path: &Path) -> PathBuf {
    #[cfg(windows)]
    if let Some(text) = path.to_str() {
        if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{rest}"));
        }
        if let Some(rest) = text.strip_prefix(r"\\?\") {
            return PathBuf::from(rest);
        }
    }
    path.to_path_buf()
}

fn freeze(
    src: &Path,
    dest: &Path,
    expected: Option<&str>,
    report: &mut Report,
) -> Result<(), Error> {
    let before = identity(src)?;
    if expected.is_some_and(|s| s != before.sha256) {
        return Err(format!("identity mismatch for {}", src.display()).into());
    }
    std::fs::copy(src, dest)?;
    if identity(dest)?.sha256 != before.sha256 || identity(src)?.sha256 != before.sha256 {
        return Err("binary changed while freezing".into());
    }
    report.identities.push(before);
    report.identities.push(identity(dest)?);
    Ok(())
}
async fn matrix(m: &Manifest, output: &Path, report: &mut Report) -> Result<(), Error> {
    report.identities.push(identity(&std::env::current_exe()?)?);
    for (name, bytes) in [
        (
            "PgIntroQueries.sql",
            include_bytes!("../probes/datagrip-2026.2.5/PgIntroQueries.sql").as_slice(),
        ),
        (
            "datagrip-inventory.json",
            include_bytes!("../probes/datagrip-2026.2.5/inventory.json").as_slice(),
        ),
    ] {
        let path = output.join(name);
        std::fs::write(&path, bytes)?;
        report.identities.push(identity(&path)?);
    }

    let source = Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in [
        "Cargo.toml",
        "Cargo.lock",
        "src/lib.rs",
        "src/main.rs",
        "probes/JdbcProbe.java",
    ] {
        report
            .observed_repository_sources
            .push(identity(&source.join(file))?);
    }
    let server = output.join(if cfg!(windows) {
        "server.exe"
    } else {
        "server"
    });
    freeze(
        &m.server,
        &server,
        m.expected_server_sha256.as_deref(),
        report,
    )?;
    let timeout = Duration::from_secs(m.timeout_seconds);
    if let (Some(java), Some(javac), Some(jar)) = (&m.java, &m.javac, &m.jdbc_jar) {
        let jar_copy = output.join("postgresql.jar");
        freeze(jar, &jar_copy, m.expected_jar_sha256.as_deref(), report)?;
        report.identities.push(identity(java)?);
        report.identities.push(identity(javac)?);
        let version = run(Command::new(java).arg("-version"), timeout).await?;
        retain(output, "java-version", &version)?;
        if version.code != Some(0) || version.timed_out || version.truncated {
            return Err("java version command failed".into());
        }
        let probe = output.join("JdbcProbe.java");
        report.identities.push(write_embedded_jdbc_probe(&probe)?);
        let compiled = run(
            Command::new(javac)
                .arg("-cp")
                .arg(&jar_copy)
                .arg("-d")
                .arg(output)
                .arg(&probe),
            timeout,
        )
        .await?;
        retain(output, "javac", &compiled)?;
        if compiled.code != Some(0) || compiled.timed_out || compiled.truncated {
            return Err("JDBC probe compilation failed".into());
        }
        report.identities.extend(jdbc_class_identities(output)?);
        for mode in ["default", "configured", "datagrip-discovery"] {
            let required = if mode == "datagrip-discovery" {
                DISCOVERY_CASES
            } else {
                JDBC_CASES
            };
            let dir = output.join(format!("jdbc-{mode}"));
            std::fs::create_dir(&dir)?;
            report.identities.extend(isolate(m, &dir)?);
            let mut owned = Managed::spawn(
                Command::new(&server)
                    .arg(dir.join("server.json"))
                    .current_dir(&dir),
            )?;
            let result = async {
                let port = owned
                    .port(Duration::from_secs(10))
                    .await
                    .map_err(std::io::Error::other)?;
                let cp = std::env::join_paths([output, &jar_copy])?;
                let captured = run(
                    Command::new(java)
                        .arg(format!("-Dfixture={}", dir.display()))
                        .arg("-cp")
                        .arg(cp)
                        .arg("JdbcProbe")
                        .arg(port.to_string())
                        .arg(mode),
                    timeout,
                )
                .await?;
                retain(&dir, "probe", &captured)?;
                Ok::<_, Error>(evaluate("pgJDBC", mode, "42.7.3", required, &captured))
            }
            .await;
            let shutdown = owned.finish(Duration::from_secs(10), true).await?;
            retain(&dir, "server", &shutdown)?;
            if shutdown.code != Some(0) || shutdown.timed_out || shutdown.truncated {
                report
                    .errors
                    .push(format!("jdbc-{mode} owned server failed shutdown"));
            }
            match result {
                Ok(p) => report.profiles.push(p),
                Err(e) => {
                    let reason = format!("jdbc-{mode}: {e}");
                    report
                        .profiles
                        .push(failed("pgJDBC", mode, required, &reason));
                    report.errors.push(reason);
                }
            }
        }
    } else {
        for mode in ["default", "configured", "datagrip-discovery"] {
            let required = if mode == "datagrip-discovery" {
                DISCOVERY_CASES
            } else {
                JDBC_CASES
            };
            report.profiles.push(untested(
                "pgJDBC",
                mode,
                required,
                "supply explicit java, javac and jdbc_jar paths; runtime unavailable/unconfigured",
            ));
        }
    }
    let dir = output.join("tokio");
    std::fs::create_dir(&dir)?;
    report.identities.extend(isolate(m, &dir)?);
    let mut owned = Managed::spawn(
        Command::new(&server)
            .arg(dir.join("server.json"))
            .current_dir(&dir),
    )?;
    let result = async {
        let port = owned
            .port(Duration::from_secs(10))
            .await
            .map_err(std::io::Error::other)?;
        let capture = run(
            Command::new(std::env::current_exe()?)
                .arg("--tokio-probe")
                .arg(&dir)
                .arg(port.to_string()),
            timeout,
        )
        .await?;
        retain(&dir, "probe", &capture)?;
        Ok::<_, Error>(evaluate(
            "tokio-postgres",
            "baseline",
            "0.7.15",
            TOKIO_CASES,
            &capture,
        ))
    }
    .await;
    let shutdown = owned.finish(Duration::from_secs(10), true).await?;
    retain(&dir, "server", &shutdown)?;
    if shutdown.code != Some(0) || shutdown.timed_out || shutdown.truncated {
        report
            .errors
            .push("tokio owned server failed shutdown".into());
    }
    match result {
        Ok(p) => report.profiles.push(p),
        Err(e) => {
            let reason = format!("tokio: {e}");
            report
                .profiles
                .push(failed("tokio-postgres", "baseline", TOKIO_CASES, &reason));
            report.errors.push(reason);
        }
    }
    // Inventory commands never connect to any database. Availability is not execution evidence.
    for (name, args) in [
        ("psql", vec!["--version"]),
        (
            "python",
            vec![
                "-c",
                "import sys,json,importlib.util; print(json.dumps({'executable':sys.executable,'python':sys.version,'psycopg':bool(importlib.util.find_spec('psycopg')),'psycopg2':bool(importlib.util.find_spec('psycopg2'))})); exec(\"try:\\n import psycopg; print('psycopg='+psycopg.__version__+' libpq='+str(psycopg.pq.version()))\\nexcept ImportError: pass\")",
            ],
        ),
    ] {
        let reason = match run(Command::new(name).args(args), Duration::from_secs(5)).await {
            Ok(c) => {
                retain(output, &format!("inventory-{name}"), &c)?;
                if name == "python"
                    && let Some(line) = c.stdout.split(|b| *b == b'\n').next()
                    && let Ok(v) = serde_json::from_slice::<serde_json::Value>(line)
                {
                    if let Some(exe) = v["executable"].as_str() {
                        report.identities.push(identity(Path::new(exe))?);
                    }
                    if v["psycopg"] == false && v["psycopg2"] == false {
                        report.profiles.push(untested("psycopg/libpq","inventory",&[("actual_client","verified mTLS lifecycle against isolated fixture")],"Python present; neither psycopg nor psycopg2 installed (see inventory-python log for exact runtime)"));
                        continue;
                    }
                }
                format!(
                    "inventory exit {:?}, timeout={}; available clients not yet executed against isolated fixture; see inventory log",
                    c.code, c.timed_out
                )
            }
            Err(e) => format!("tool unavailable: {e}"),
        };
        report.profiles.push(untested(
            if name == "python" {
                "psycopg/libpq"
            } else {
                "psql/libpq"
            },
            "inventory",
            &[(
                "actual_client",
                "verified mTLS lifecycle against isolated fixture",
            )],
            &reason,
        ));
    }
    Ok(())
}
fn emit(id: &str, status: &str, actual: &str) {
    use base64::Engine;
    println!(
        "CASE\t{id}\t{status}\t{}",
        base64::engine::general_purpose::STANDARD.encode(actual)
    );
}
async fn tokio_probe(dir: &Path, port: &str) -> Result<(), Error> {
    println!("VERSION\t0.7.15");
    let certs = |name: &str| -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Error> {
        Ok(
            rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(
                dir.join(name),
            )?))
            .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs("ca.pem")? {
        roots.add(cert)?;
    }
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(
        dir.join("app.key.TEST-ONLY"),
    )?))?
    .ok_or("missing private key")?;
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs("app.pem")?, key)?;
    let mut config = tokio_postgres::Config::new();
    config
        .host("localhost")
        .hostaddr("127.0.0.1".parse()?)
        .port(port.parse()?)
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(tokio_postgres::config::SslMode::Require)
        .connect_timeout(Duration::from_secs(5));
    let (client, connection) = config
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
        .await?;
    let task = tokio::spawn(connection);
    emit("connect_init", "PASS", "assertions satisfied");
    let result = client
        .query("SELECT id,body FROM app WHERE id=$1", &[&8000_i64])
        .await;
    match result {
        Ok(rows) if rows.is_empty() => emit("prepared_empty", "PASS", "assertions satisfied"),
        other => emit(
            "prepared_empty",
            "FAIL",
            &format!("expected empty rows; {other:?}"),
        ),
    }
    let result = client
        .simple_query("SELECT id,body FROM app WHERE id=8000")
        .await;
    match result {
        Ok(rows)
            if rows
                .iter()
                .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
                .count()
                == 0
                && rows.iter().any(|m| {
                    matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(0))
                }) =>
        {
            emit("simple_query", "PASS", "assertions satisfied")
        }
        other => emit(
            "simple_query",
            "FAIL",
            &format!("expected empty rows; {other:?}"),
        ),
    }
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_expected_identity_rejects_before_copy() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("artifacts")
            .join(format!("identity-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("input");
        let dest = dir.join("frozen");
        std::fs::write(&input, b"actual binary").unwrap();
        let mut report = Report {
            schema: 1,
            scope: String::new(),
            identities: vec![],
            observed_repository_sources: vec![],
            profiles: vec![],
            errors: vec![],
            verdict: Status::Untested,
            summary: Summary::default(),
        };
        assert!(
            freeze(&input, &dest, Some("stale"), &mut report)
                .unwrap_err()
                .to_string()
                .contains("identity mismatch")
        );
        assert!(!dest.exists());
        let hash = identity(&input).unwrap().sha256;
        freeze(&input, &dest, Some(&hash), &mut report).unwrap();
        assert_eq!(identity(&dest).unwrap().sha256, hash);
    }
    #[cfg(windows)]
    #[test]
    fn java_paths_remove_extended_prefix() {
        assert_eq!(
            external_path(Path::new(r"\\?\K:\fixture")),
            PathBuf::from(r"K:\fixture")
        );
        assert_eq!(
            external_path(Path::new(r"\\?\UNC\host\share")),
            PathBuf::from(r"\\host\share")
        );
    }
}
