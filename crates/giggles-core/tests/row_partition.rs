#![cfg(feature = "legacy-task")]
use giggles_core::{
    replica::{Command, Image},
    *,
};
use serde_json::json;
use std::collections::BTreeSet;
fn key(n: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(n),
    }
}
fn admin(image: Image, command: serde_json::Value) -> Image {
    let (next, reply) = image
        .evaluate(&Command::Admin(serde_json::to_vec(&command).unwrap()))
        .unwrap();
    assert_eq!(reply.code, 0, "{reply:?}");
    next
}
#[test]
fn projection_preserves_versions_but_no_task_catalog_or_accounting_authority() {
    let mut image = Image::new(42).unwrap();
    for n in [1, 49, 50, 99] {
        image = admin(
            image,
            json!({"command":"seed","key":key(n),"row":{"status":"Failed","failed_at":103,"attempt_count":0,"version":999}}),
        );
    }
    image = admin(
        image,
        json!({"command":"seed","key":key(1),"row":{"status":"Queued","failed_at":103,"attempt_count":1,"version":999}}),
    );
    let grant = Grant {
        task: TaskId(1),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(42),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 107),
            distinct_limit: 5,
            transition_limit: 5,
            approval_above: 5,
            valid_from: 1,
            expires: 1000,
        },
    };
    image = admin(image, json!({"command":"grant","grant":grant}));
    image = admin(
        image,
        json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":100,"disclosed_rows":100,"encoded_bytes":100000}}),
    );
    image = admin(
        image,
        json!({"command":"sql","sql":"CREATE TABLE app (id BIGINT PRIMARY KEY, value TEXT)","params":[],"expected_version":null}),
    );
    let (image, reply) = image
        .evaluate(&Command::Task {
            task: TaskId(1),
            principal: Principal(2),
            lower: 10,
            upper: 10,
            action: replica::TaskAction::Execute(
                Request {
                    operation: OperationId(1),
                    selection: Selection::Keys(vec![key(49)]),
                    column: Column::Status,
                    after: Status::Queued,
                },
                None,
            ),
        })
        .unwrap();
    assert_eq!(reply.code, 0);
    let (image, preview) = image
        .evaluate(&Command::Task {
            task: TaskId(1),
            principal: Principal(2),
            lower: 10,
            upper: 10,
            action: replica::TaskAction::Preview(Request {
                operation: OperationId(2),
                selection: Selection::Keys(vec![key(50)]),
                column: Column::Status,
                after: Status::Queued,
            }),
        })
        .unwrap();
    assert_eq!(preview.code, 0);
    let image = admin(
        image,
        json!({"command":"approve","principal":3,"preview":preview.data["preview"],"time":{"Certified":10}}),
    );
    let source = image.snapshot().unwrap();
    assert!(
        !source.tasks.is_empty() && !source.operations.is_empty() && !source.approvals.is_empty()
    );
    let source_bytes = image.bytes().to_vec();
    let left = image.row_partition(key(0), Some(key(50))).unwrap();
    let right = image.row_partition(key(50), None).unwrap();
    let rows = left
        .snapshot()
        .unwrap()
        .rows
        .into_iter()
        .chain(right.snapshot().unwrap().rows)
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(rows, source.rows);
    assert_eq!(rows[&key(1)].version, 1);
    assert_eq!(rows[&key(49)].version, 1);
    for child in [left, right] {
        let s = child.snapshot().unwrap();
        assert!(s.tasks.is_empty() && s.operations.is_empty() && s.approvals.is_empty());
        assert_eq!(child.accounting(TaskId(1)).unwrap(), None);
        let (_, catalog) = child
            .evaluate(&Command::Admin(
                serde_json::to_vec(&json!({"command":"catalog"})).unwrap(),
            ))
            .unwrap();
        assert!(!catalog.data.to_string().contains("app"));
        assert_eq!(Image::from_bytes(child.bytes().to_vec()).unwrap(), child);
    }
    assert_eq!(source_bytes, image.bytes());
    assert!(image.row_partition(key(50), Some(key(50))).is_err());
}
