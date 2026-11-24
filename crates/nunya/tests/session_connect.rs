//! Live PostgreSQL session connection details and helper fallback.
use super::common::{ServeHarness, ServeResult};
use nunya::{
    cli::session::{AcquireOpts, ConnectionOpts, HelperStore, acquire_or_resume, pg_env},
    config::Config,
    mgmt::client::{ConnectInfo, Credentials, MgmtClient},
    state::model::Mode,
};
use std::{collections::BTreeMap, fs, path::Path, process::Command};

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG pointing at a PostgreSQL test database"]
async fn postgres_sessions_advertise_route_connection() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    for advertised in [None, Some(("gateway.example", 17432))] {
        let mut harness = ServeHarness::new(
            "[readlog]\nsink='file'\npath='read.log'\n",
            Some(&url),
            None,
            None,
            "",
        )?;
        let path = harness.dir().join("nunya.toml");
        if let Some((host, port)) = advertised {
            fs::write(
                &path,
                format!(
                    "advertise_host='{host}'\nadvertise_port={port}\n{}",
                    fs::read_to_string(&path)?
                ),
            )?;
        }
        let cfg = Config::load(&path).map_err(|errors| errors.join("\n"))?;
        let listen = cfg.listen.ok_or("missing gateway listener")?;
        assert_ne!(listen.port(), 6432);
        let database = cfg.upstream.ok_or("missing upstream")?.dbname;
        let server = format!(
            "http://{}",
            cfg.management.ok_or("missing management")?.listen
        );
        let name = "session-connect-agent";
        let mode = Mode::ReadOnly;
        harness.start().await?;
        let (session, _, secret) = harness.acquire_session(name, "postgres", mode).await?;
        let client = MgmtClient::new(
            server.parse()?,
            None,
            Credentials::Bearer {
                name: name.into(),
                secret: secret.secret.clone(),
            },
        )?;

        let (host, port) = advertised.unwrap_or(("127.0.0.1", listen.port()));
        let expected = Some(ConnectInfo::Postgres {
            host: host.into(),
            port,
            database: database.clone(),
        });
        assert_eq!(session.connect, expected);
        assert_eq!(client.session(&session.id).await?.connect, expected);
        assert_eq!(
            client.renew(&session.id, None, None).await?.connect,
            expected
        );

        let store_path = harness.dir().join("helper");
        let store = HelperStore::at(&store_path);
        let opts = AcquireOpts {
            connection: ConnectionOpts {
                config: None,
                server: Some(server),
                follow_leader: false,
                ca_file: None,
                principal: Some(name.into()),
                principal_secret_file: None,
                context: None,
            },
            route: Some("postgres".into()),
            mode: Some(mode),
            ttl: None,
            label: None,
            new_scope: false,
            rotate: false,
            pg_host: None,
            pg_port: None,
            isolated: false,
        };
        let ctx = acquire_or_resume(&client, &store, &opts).await?;
        assert_eq!(ctx.connect, expected);
        let env = BTreeMap::from_iter(pg_env(&ctx));
        assert_eq!(env["PGHOST"].as_str(), host);
        assert_eq!(env["PGPORT"].as_str(), port.to_string());
        assert_eq!(env["PGDATABASE"].as_str(), database);

        let opts = AcquireOpts {
            pg_port: Some(6432),
            ..opts
        };
        let mut ctx = acquire_or_resume(&client, &store, &opts).await?;
        assert_eq!(ctx.connect, expected);
        assert_eq!(BTreeMap::from_iter(pg_env(&ctx))["PGPORT"].as_str(), "6432");

        if advertised.is_some() {
            let secret_path = harness.dir().join("principal.secret");
            fs::write(&secret_path, secret.secret.as_bytes())?;
            let online = cli(&store_path)
                .args(["session", "show", "--principal-secret-file"])
                .arg(secret_path)
                .output()?;
            assert!(online.status.success());
            assert!(online.stderr.is_empty());
            for verb in ["env", "show"] {
                let result = cli(&store_path).args(["session", verb]).output()?;
                assert!(result.status.success());
                assert_eq!(
                    String::from_utf8(result.stderr)?.trim(),
                    "note: session not revalidated with the server (principal secret missing: pass --principal-secret-file or set NUNYA_PRINCIPAL_SECRET)"
                );
                let stdout = String::from_utf8_lossy(&result.stdout);
                if verb == "env" {
                    assert!(stdout.contains("export PGPORT='6432'"));
                    assert!(stdout.contains(&format!("export PGDATABASE='{database}'")));
                } else {
                    assert!(stdout.contains(&format!("id: {}", ctx.session_id)));
                    assert!(stdout.contains("route: postgres"));
                    assert!(stdout.contains("expires_ms: "));
                    assert!(stdout.contains("context: default"));
                    assert!(stdout.contains("\"host\":\"gateway.example\""));
                    assert!(!stdout.contains(ctx.secret.as_str()));
                }
            }
            ctx.expires_ms = 0;
            store.put(&ctx)?;
            let expired = cli(&store_path).args(["session", "env"]).output()?;
            assert!(!expired.status.success());
            assert!(expired.stdout.is_empty());
            assert!(
                String::from_utf8(expired.stderr)?
                    .contains("expired locally and the principal secret is missing")
            );
            for verb in ["renew", "revoke"] {
                let result = cli(&store_path).args(["session", verb]).output()?;
                assert!(!result.status.success());
                assert_eq!(
                    String::from_utf8(result.stderr)?.trim(),
                    "principal secret missing: pass --principal-secret-file PATH or set NUNYA_PRINCIPAL_SECRET"
                );
            }
        }
    }
    Ok(())
}

fn cli(store_path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nunya"));
    command
        .env("NUNYA_HOME", store_path)
        .env_remove("NUNYA_SERVER")
        .env_remove("NUNYA_PRINCIPAL")
        .env_remove("NUNYA_PRINCIPAL_SECRET")
        .env_remove("NUNYA_CA_FILE");
    command
}
