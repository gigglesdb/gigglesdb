//! Real GigglesDB destination qualification; absence of a server is explicit.

use std::{
    error::Error,
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

use skedaddle::{
    CapabilityItem, Criticality, ItemStatus,
    definition::{
        Connection, Engine, GigglesdbDeployment, GigglesdbEndpoint, ObjectKind, ObjectSelector,
    },
    gigglesdb::preflight::{self, REQUIRED_DESTINATION_CAPABILITIES},
    pg::tls,
    security::TlsPolicy,
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

type TestError = Box<dyn Error + Send + Sync>;

struct Server {
    child: Child,
    root: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Err(error) = fs::remove_dir_all(&self.root) {
            eprintln!("FAILED to remove {}: {error}", self.root.display());
        }
    }
}

struct Live {
    // The driver is aborted before the server is stopped, including on panic.
    driver: tokio::task::JoinHandle<()>,
    client: Client,
    config: tls::Config,
    version: String,
    _server: Server,
}

impl Drop for Live {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

async fn live() -> Result<Option<Live>, TestError> {
    let Some(binary) = std::env::var_os("GIGGLES_SERVER_BIN") else {
        eprintln!(
            "SKIPPED: set GIGGLES_SERVER_BIN to a giggles-server executable for live GigglesDB preflight qualification"
        );
        return Ok(None);
    };
    let parent = std::env::var_os("SKEDADDLE_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = parent.join(format!("gd65-n2b-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let child = match Command::new(binary)
        .args(["--dev", "--trust-local", "--data"])
        .arg(root.join("data"))
        .args(["--port", &port.to_string()])
        .env("GIGGLES_OWNER_PASSWORD", uuid::Uuid::new_v4().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            fs::remove_dir_all(root)?;
            return Err(error.into());
        }
    };
    let mut server = Server { child, root };
    let postgres: tokio_postgres::Config =
        format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1")
            .parse()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = server.child.try_wait()? {
            return Err(format!("giggles-server exited during startup: {status}").into());
        }
        if let Ok(Ok((client, connection))) =
            tokio::time::timeout(Duration::from_secs(1), postgres.connect(NoTls)).await
        {
            let driver = tokio::spawn(async move {
                let _ = connection.await;
            });
            let config = tls::Config::new(postgres, "unused-local-test-ca");
            let mut fixture = Live {
                driver,
                client,
                config,
                version: String::new(),
                _server: server,
            };
            let messages = fixture.client.simple_query("SELECT version()").await?;
            fixture.version = messages
                .iter()
                .find_map(|message| match message {
                    SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
                    _ => None,
                })
                .ok_or("version() returned no value")?;
            fixture
                .client
                .batch_execute(
                    "CREATE TABLE public.n2b_scope (id integer, readable text, hidden text)",
                )
                .await?;
            return Ok(Some(fixture));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("giggles-server startup exceeded 30 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn definition(version: &str, deployment: GigglesdbDeployment) -> Connection {
    Connection {
        connection_ref: None,
        engine: Engine::Gigglesdb,
        tls: TlsPolicy::DisableForLocalTest,
        tls_ca_ref: None,
        version: version.into(),
        spanner: None,
        mysql: None,
        gigglesdb: Some(GigglesdbEndpoint {
            deployment,
            database_path: None,
        }),
    }
}

fn selected() -> Vec<ObjectSelector> {
    vec![ObjectSelector {
        schema: "public".into(),
        name: "n2b_scope".into(),
        kind: ObjectKind::Table,
        exclude_columns: vec![],
    }]
}

fn find<'a>(items: &'a [CapabilityItem], suffix: &str) -> &'a CapabilityItem {
    items
        .iter()
        .find(|item| item.id == format!("destination.gigglesdb.{suffix}"))
        .unwrap_or_else(|| panic!("missing {suffix}: {items:?}"))
}

fn assert_blocked(item: &CapabilityItem, evidence: &str) {
    assert_eq!(item.criticality, Criticality::Critical);
    assert_eq!(item.status, ItemStatus::RequiresExternalChange, "{item:?}");
    assert!(
        item.evidence
            .iter()
            .any(|e| e.description.contains(evidence)),
        "missing {evidence:?}: {item:?}"
    );
}

async fn check(fixture: &Live, config: &tls::Config) -> Result<Vec<CapabilityItem>, TestError> {
    Ok(tokio::time::timeout(
        Duration::from_secs(20),
        preflight::check(
            &definition(&fixture.version, GigglesdbDeployment::Server),
            &selected(),
            config,
        ),
    )
    .await??)
}

async fn setup(client: &Client, statements: &[&str]) -> Result<(), TestError> {
    // Role administration cannot run inside a multi-statement implicit transaction.
    for statement in statements {
        client
            .batch_execute(statement)
            .await
            .map_err(|error| format!("Fixture statement {statement:?}: {error:?}"))?;
    }
    Ok(())
}

#[tokio::test]
async fn correct_version_and_complete_principal_pass_all_five_items() -> Result<(), TestError> {
    let Some(fixture) = live().await? else {
        return Ok(());
    };
    // Qualify a migration role rather than relying solely on superuser bypass.
    setup(
        &fixture.client,
        &[
            "CREATE ROLE n2b_full LOGIN",
            "GRANT USAGE ON SCHEMA public TO n2b_full",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.n2b_scope TO n2b_full",
        ],
    )
    .await?;
    let mut config = fixture.config.clone();
    config.postgres.user("n2b_full");
    let items = check(&fixture, &config).await?;
    assert_eq!(items.len(), 5);
    for item in items {
        assert_eq!(item.criticality, Criticality::Critical);
        assert!(!item.sampled);
        assert_eq!(
            item.status,
            ItemStatus::SupportedWithTransformation,
            "{item:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn wrong_version_blocks_and_names_both_strings() -> Result<(), TestError> {
    let Some(fixture) = live().await? else {
        return Ok(());
    };
    let wrong = "PostgreSQL 18.0 (GigglesDB 0.1.0+000000000000)";
    let items = preflight::check(
        &definition(wrong, GigglesdbDeployment::Server),
        &selected(),
        &fixture.config,
    )
    .await?;
    assert_blocked(find(&items, "version_pin"), wrong);
    assert_blocked(find(&items, "version_pin"), &fixture.version);
    Ok(())
}

#[tokio::test]
async fn missing_required_capability_blocks_and_names_token() -> Result<(), TestError> {
    let Some(fixture) = live().await? else {
        return Ok(());
    };
    let mut required = REQUIRED_DESTINATION_CAPABILITIES.to_vec();
    required.push("n2b_made_up_capability");
    let items = preflight::check_with_required_capabilities_for_test(
        &definition(&fixture.version, GigglesdbDeployment::Server),
        &selected(),
        &fixture.config,
        &required,
    )
    .await?;
    assert_blocked(find(&items, "capabilities"), "n2b_made_up_capability");
    Ok(())
}

#[tokio::test]
async fn missing_table_select_blocks_and_names_every_selected_column() -> Result<(), TestError> {
    let Some(fixture) = live().await? else {
        return Ok(());
    };
    setup(
        &fixture.client,
        &[
            "CREATE ROLE n2b_no_select LOGIN",
            "GRANT USAGE ON SCHEMA public TO n2b_no_select",
            "GRANT INSERT, UPDATE, DELETE ON TABLE public.n2b_scope TO n2b_no_select",
        ],
    )
    .await?;
    let mut config = fixture.config.clone();
    config.postgres.user("n2b_no_select");
    let items = check(&fixture, &config).await?;
    for column in ["id", "readable", "hidden"] {
        assert_blocked(
            find(&items, "column_scope"),
            &format!("unreadable column \"public\".\"n2b_scope\".{column}"),
        );
    }
    Ok(())
}

#[tokio::test]
async fn column_level_grant_is_rejected_so_column_scope_equals_table_scope() -> Result<(), TestError>
{
    let Some(fixture) = live().await? else {
        return Ok(());
    };
    setup(&fixture.client, &["CREATE ROLE n2b_column_grant LOGIN"]).await?;
    // GigglesDB column SELECT scope follows table ACLs: partial column GRANT
    // is explicitly unsupported, rather than an alternative grant mechanism.
    let error = fixture
        .client
        .batch_execute("GRANT SELECT (id) ON TABLE public.n2b_scope TO n2b_column_grant")
        .await
        .expect_err("GigglesDB must reject column-level GRANT");
    assert_eq!(error.code().map(|code| code.code()), Some("0A000"));
    Ok(())
}

#[tokio::test]
async fn missing_update_blocks() -> Result<(), TestError> {
    let Some(fixture) = live().await? else {
        return Ok(());
    };
    setup(
        &fixture.client,
        &[
            "CREATE ROLE n2b_no_update LOGIN",
            "GRANT USAGE ON SCHEMA public TO n2b_no_update",
            "GRANT SELECT, INSERT, DELETE ON TABLE public.n2b_scope TO n2b_no_update",
        ],
    )
    .await?;
    let mut config = fixture.config.clone();
    config.postgres.user("n2b_no_update");
    let items = check(&fixture, &config).await?;
    assert_blocked(find(&items, "column_scope"), "missing UPDATE");
    Ok(())
}

#[tokio::test]
async fn excluded_columns_shrink_reported_unreadable_set() -> Result<(), TestError> {
    let Some(fixture) = live().await? else {
        return Ok(());
    };
    setup(
        &fixture.client,
        &[
            "CREATE ROLE n2b_exclude LOGIN",
            "GRANT USAGE ON SCHEMA public TO n2b_exclude",
            "GRANT INSERT, UPDATE, DELETE ON TABLE public.n2b_scope TO n2b_exclude",
        ],
    )
    .await?;
    let mut config = fixture.config.clone();
    config.postgres.user("n2b_exclude");
    let all_items = check(&fixture, &config).await?;
    assert_blocked(
        find(&all_items, "column_scope"),
        "unreadable column \"public\".\"n2b_scope\".hidden",
    );
    let mut objects = selected();
    objects[0].exclude_columns.push("hidden".into());
    let items = preflight::check(
        &definition(&fixture.version, GigglesdbDeployment::Server),
        &objects,
        &config,
    )
    .await?;
    let scope = find(&items, "column_scope");
    for column in ["id", "readable"] {
        assert_blocked(
            scope,
            &format!("unreadable column \"public\".\"n2b_scope\".{column}"),
        );
    }
    assert!(
        scope
            .evidence
            .iter()
            .all(|e| !e.description.contains("hidden")),
        "excluded column still reported: {scope:?}"
    );
    Ok(())
}

#[tokio::test]
async fn embedded_blocks_without_attempting_connection() -> Result<(), TestError> {
    // No running listener and required TLS with a nonexistent CA: either would
    // fail if preflight tried to open a connection before checking deployment.
    let postgres = "host=127.0.0.1 port=1 user=nobody connect_timeout=1".parse()?;
    let config = tls::Config::new(postgres, "nonexistent-n2b-ca.pem");
    let mut definition = definition("unused", GigglesdbDeployment::Embedded);
    definition.tls = TlsPolicy::Required;
    let items = preflight::check(&definition, &selected(), &config).await?;
    assert_eq!(items.len(), 1);
    assert_blocked(
        find(&items, "deployment"),
        "embedded preflight lands with GD-0065 N6",
    );
    Ok(())
}
