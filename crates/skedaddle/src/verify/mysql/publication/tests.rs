//! Pure protocol probes plus a real filesystem journal; no database emulation.
use super::*;
use crate::{
    coordinator::{SourceBinding, SourceConnector, Transition, epoch::Coordinator, local::Local},
    event::{PositionOrder, SourceIdentity},
    mysql::{MysqlPosition, chunk_key::*},
};
use std::sync::Arc;
const UUID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
fn order() -> MysqlOrder {
    MysqlOrder::file(UUID).unwrap()
}
fn position(n: u64) -> Position {
    Position::new(
        order().domain(),
        MysqlPosition::File {
            server_uuid: UUID.into(),
            file_seq: 1,
            offset: n,
        }
        .encode()
        .unwrap(),
    )
}
pub fn key() -> MysqlTableKey {
    MysqlTableKey {
        event_table: "public.accounts".into(),
        schema: "public".into(),
        table: "accounts".into(),
        columns: vec![MysqlKeyColumn {
            name: "id".into(),
            data_type: "bigint".into(),
            column_type: "bigint".into(),
            character_set: None,
            collation: None,
            type_id: "bigint".into(),
            direction: MysqlKeyDirection::Asc,
        }],
    }
}
pub fn publication() -> Publication {
    let section = || crate::coordinator::verification::Section {
        completed: true,
        checked: 0,
        findings: vec![],
        evidence: "contract section collector".into(),
    };
    Publication {
        migration: "migration".into(),
        run: uuid::Uuid::new_v4().to_string(),
        catalog_digest: "a".repeat(64),
        plan_digest: "b".repeat(64),
        sections: Sections {
            schema: section(),
            sequences: section(),
            constraints: section(),
            indexes: section(),
            destination_foreign_keys: section(),
        },
        dead_letters: DeadLetters {
            collection: "contract".into(),
            completed: true,
            count: 0,
            identities: vec![],
            evidence: "contract collection scan".into(),
        },
        timeout: Duration::from_secs(3),
        poll_interval: Duration::from_millis(1),
    }
}
async fn owner() -> (Runtime, Journal, SourceIdentity) {
    let root = crate::test_root::scratch_root()
        .join(format!("mysql-publication-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let j = Journal::new(
        Arc::new(crate::coordinator::journal::local_store(&root).unwrap()),
        "verify",
        "migration",
        Duration::from_secs(3),
    )
    .unwrap();
    let mut c = Coordinator::observer(
        j.clone(),
        Local::create(root.join("local")).unwrap(),
        "owner",
    )
    .unwrap();
    c.acquire().await.unwrap();
    let text =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
            .unwrap();
    let mut d = crate::MigrationDefinition::from_json(
        text.split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    d.source.engine = crate::definition::Engine::Mysql;
    d.source.connection_ref = None;
    d.source.version = "8.4".into();
    d.source.mysql = Some(serde_json::from_value(serde_json::json!({"host":"127.0.0.1","port":3306,"user":"env:SKEDADDLE_TEST_MYSQL_USER","password":"env:SKEDADDLE_TEST_MYSQL_PASSWORD"})).unwrap());
    c.commit(Transition::Define {
        definition: Box::new(d),
    })
    .await
    .unwrap();
    c.commit(Transition::SourceBindingV1 {
        binding: SourceBinding {
            version: 1,
            connector: SourceConnector::Mysql,
            order_domain: order().domain().into(),
            stream: "stream".into(),
        },
    })
    .await
    .unwrap();
    let identity = SourceIdentity {
        engine: "mysql".into(),
        system_id: "cluster".into(),
        lineage: "one".into(),
        db_name: "db".into(),
        stream: "stream".into(),
    };
    let cp =
        crate::engine::test_reader_checkpoint(identity.clone(), position(100), c.state().epoch);
    c.commit(Transition::Progress {
        stream: "applied".into(),
        marker: crate::engine::encode_reader_checkpoint(&cp).unwrap(),
        position: "1".into(),
    })
    .await
    .unwrap();
    (
        crate::engine::Engine::new(c, j.clone()).runtime(),
        j,
        identity,
    )
}
struct Rows;
impl KeyOrder for Rows {
    fn compare_keys(&self, a: &Key, b: &Key) -> verify::Result<Ordering> {
        match (a.as_slice(), b.as_slice()) {
            ([Value::Int8(a)], [Value::Int8(b)]) => Ok(a.cmp(b)),
            _ => Err(VerifyError::Invalid("contract key")),
        }
    }
}
#[async_trait]
impl RowSource for Rows {
    async fn fetch(&mut self, _: &RangePlan, _: &Projection) -> verify::Result<Vec<Row>> {
        Ok(vec![Row {
            key: vec![Value::Int8(1)],
            columns: Default::default(),
        }])
    }
}
struct Input;
#[async_trait]
impl MysqlEvidenceInput for Input {
    async fn arm(&mut self, _: &RangePlan) -> verify::Result<Position> {
        Ok(position(4))
    }
    async fn boundary(&mut self) -> verify::Result<Position> {
        Ok(position(40))
    }
    async fn next(&mut self) -> verify::Result<EvidenceEvent> {
        Ok(EvidenceEvent::Commit {
            position: position(40),
            changes: vec![],
        })
    }
}
#[tokio::test]
async fn marker_publishes_v2_through_real_journal_and_refuses_nonnumeric_keys() {
    let (runtime, j, identity) = owner().await;
    let mut evidence =
        MysqlChangeEvidence::new(Input, order(), Rows, Duration::from_secs(3), 100).unwrap();
    let mut rows = Rows;
    let mut source = CapturedSource {
        source: &mut rows,
        evidence: &mut evidence,
        complete: None,
    };
    let mut waiter = MysqlAppliedWaiter::new(
        JournalMysqlCheckpoint::new(j.clone(), identity, order()).unwrap(),
        order(),
        Duration::from_secs(3),
        Duration::from_millis(1),
    )
    .unwrap();
    let report = run_and_publish(
        &runtime,
        &j,
        &mut source,
        &mut Rows,
        &mut waiter,
        &key(),
        Projection::new(vec![]).unwrap(),
        publication(),
    )
    .await
    .unwrap();
    assert_eq!(
        Position::try_from(report.interval.complete_through.clone()).unwrap(),
        position(40)
    );
    assert_ne!(
        Position::try_from(report.interval.complete_through.clone()).unwrap(),
        position(4)
    );
    assert_eq!(report.version, 2);
    let state = runtime.state().await;
    let saved = j
        .native_verification_report(&state, state.verification.native_report.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(*saved, report);
    let current = crate::runner::verification::native_current(
        &j,
        &state,
        &position(40),
        &[report.tables[0].scope.clone()],
    )
    .await
    .unwrap();
    assert_eq!(current.verification.unwrap().tables[0].rows.verified, 1);
    let mut bad = key();
    bad.columns[0].data_type = "varchar".into();
    bad.columns[0].column_type = "varchar".into();
    let error = run_and_publish(
        &runtime,
        &j,
        &mut source,
        &mut Rows,
        &mut waiter,
        &bad,
        Projection::new(vec![]).unwrap(),
        publication(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<native::Error>(),
        Some(native::Error::NativeReportKeyUnsupported { .. })
    ));
    for sample in [
        super::super::SnapshotBoundary::UpperBound(
            MysqlPosition::decode(position(40).bytes()).unwrap(),
        ),
        super::super::SnapshotBoundary::UncertifiedGtidSample(
            MysqlPosition::decode(position(40).bytes()).unwrap(),
        ),
    ] {
        assert!(matches!(
            sample.durable_report(),
            Err(VerifyError::NonPgVerificationReportUnsupported)
        ));
    }
}

#[test]
fn numeric_busy_keys_preserve_full_width_and_fixed_scale() {
    let column = |kind| native::KeyColumn {
        name: "id".into(),
        mapping_identity: "qualified".into(),
        kind,
    };
    assert_eq!(
        scalar_key(
            &vec![Value::Int8(i64::MIN)],
            &[column(native::KeyType::Signed { bits: 64 })]
        )
        .unwrap(),
        vec![native::Scalar::Signed(i64::MIN)]
    );
    assert_eq!(
        scalar_key(
            &vec![Value::Numeric(
                crate::mapping::Numeric::parse("18446744073709551615").unwrap()
            )],
            &[column(native::KeyType::Unsigned { bits: 64 })]
        )
        .unwrap(),
        vec![native::Scalar::Unsigned(u64::MAX)]
    );
    assert_eq!(
        scalar_key(
            &vec![Value::Numeric(
                crate::mapping::Numeric::parse("-12.30").unwrap()
            )],
            &[column(native::KeyType::Decimal {
                precision: 10,
                scale: 2
            })]
        )
        .unwrap(),
        vec![native::Scalar::Decimal("-1230".into())]
    );
    assert!(
        scalar_key(
            &vec![Value::Int8(-1)],
            &[column(native::KeyType::Unsigned { bits: 64 })]
        )
        .is_err()
    );
    assert!(
        scalar_key(
            &vec![Value::Numeric(
                crate::mapping::Numeric::parse("12.3").unwrap()
            )],
            &[column(native::KeyType::Decimal {
                precision: 10,
                scale: 2
            })]
        )
        .is_err()
    );
}
