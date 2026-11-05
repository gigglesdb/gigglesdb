#![allow(dead_code)]
use giggles_core::{
    local::AdminCommand,
    replica::{Image, protected::Intent},
    *,
};
use giggles_distributed::{protected::Plan, *};
use std::collections::BTreeSet;

pub fn key(n: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(n),
    }
}
pub fn row() -> Row {
    Row {
        status: Status::Failed,
        failed_at: 103,
        attempt_count: 0,
        version: 0,
    }
}
pub fn image(domain: u128, limit: u64) -> Image {
    image_with_read_rows(domain, limit, 1000)
}
pub fn image_with_read_rows(domain: u128, limit: u64, read_rows: u64) -> Image {
    let i = Image::new(domain).unwrap();
    let grant = Grant {
        task: TaskId(1),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(42),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 110),
            distinct_limit: limit,
            transition_limit: limit,
            approval_above: limit,
            valid_from: 1,
            expires: 1000,
        },
    };
    let (i, r) = i
        .evaluate(&replica::Command::Admin(
            serde_json::to_vec(&AdminCommand::Grant { grant }).unwrap(),
        ))
        .unwrap();
    assert_eq!(r.code, 0, "{:?}", r.data);
    let (i, r) = i
        .evaluate(&replica::Command::Admin(
            serde_json::to_vec(&AdminCommand::ProvisionTaskResources {
                task: TaskId(1),
                limits: TaskResources {
                    read_rows,
                    disclosed_rows: 1000,
                    encoded_bytes: 1_000_000,
                },
            })
            .unwrap(),
        ))
        .unwrap();
    assert_eq!(r.code, 0, "{:?}", r.data);
    i
}
pub fn plan(routes: Vec<Descriptor>, revision: u64, nonce: u128) -> Plan {
    Plan {
        database: 42,
        nonce,
        revision,
        task: TaskId(1),
        routes,
        intent: Intent {
            principal: Principal(2),
            request: Request {
                operation: OperationId(nonce.try_into().unwrap()),
                selection: Selection::Keys(vec![key(1), key(60)]),
                column: Column::Status,
                after: Status::Queued,
            },
            approval: None,
            lower: 10,
            upper: 12,
            fence: 1,
        },
    }
}
