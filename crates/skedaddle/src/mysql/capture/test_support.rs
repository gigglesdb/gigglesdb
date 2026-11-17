// Shared live-test installation: real local journal and coordinator.
use skedaddle::{
    coordinator::{self, epoch::Coordinator, journal::Journal, local::Local},
    engine::{Capture, Engine, Runtime},
    mysql::{
        capture::{MysqlCapture, StartupInputs},
        client::MysqlConfig,
        schema::load_descriptors,
        startup::{GtidMode, SelectedTable},
    },
};
use std::{path::Path, sync::Arc, time::Duration};
#[allow(dead_code)] // Engine tests reuse only the input builder.
pub async fn install(
    c: MysqlCapture,
    cfg: &MysqlConfig,
    tables: &[(String, String)],
    root: &Path,
) -> (MysqlCapture, Journal, Runtime, StartupInputs) {
    let root = root.join(format!("startup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("journal")).unwrap();
    let inputs = inputs(cfg, tables).await;
    let mut c = c.with_startup_inputs(inputs.clone());
    let journal = Journal::new(
        Arc::new(coordinator::journal::local_store(root.join("journal")).unwrap()),
        "capture",
        "test",
        Duration::from_secs(3),
    )
    .unwrap();
    let mut coordinator = Coordinator::observer(
        journal.clone(),
        Local::create(root.join("local")).unwrap(),
        "capture-test",
    )
    .unwrap();
    coordinator.acquire().await.unwrap();
    let runtime = Engine::new(coordinator, journal.clone()).runtime();
    runtime
        .marker(
            "source-startup",
            c.startup_record().unwrap().encode().unwrap(),
            String::new(),
        )
        .await
        .unwrap();
    c.prepare_schema(&journal, &runtime).await.unwrap();
    (c, journal, runtime, inputs)
}

pub async fn inputs(cfg: &MysqlConfig, tables: &[(String, String)]) -> StartupInputs {
    let mut client = cfg.connect().await.unwrap();
    let status = client.status().await.unwrap();
    let descriptors = load_descriptors(&mut client.conn, tables).await.unwrap();
    let inputs = StartupInputs {
        selected: descriptors
            .iter()
            .map(|d| SelectedTable {
                schema: d.schema.clone(),
                table: d.table.clone(),
                columns: d.columns.iter().map(|c| c.name.clone()).collect(),
                primary_key: d.primary_key.clone(),
            })
            .collect(),
        mapping_versions: vec!["mysql.v1".into(), "skva.v1".into()],
        gtid_mode: if status.gtid_on {
            GtidMode::ON
        } else {
            GtidMode::OFF
        },
        version: client.version,
    };
    client.conn.disconnect().await.unwrap();
    inputs
}
