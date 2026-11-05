mod common;
use common::*;
use giggles_consensus::*;
use giggles_core::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn native(action: TaskAction) -> Command {
    Command::NativeTask {
        task: TaskId(1),
        principal: Principal(2),
        lower: 10,
        upper: 10,
        action,
    }
}
fn apply(image: &mut Image, command: Command) -> Value {
    let (next, reply) = image.evaluate(&command).unwrap();
    *image = Image::from_bytes(next.bytes().to_vec()).unwrap();
    assert_eq!(reply.code, 0, "{reply:?}");
    reply.data
}
fn fixture(rows: u64, transitions: u64, responses: u64, bytes: u64) -> Image {
    let mut image = Image::new(42).unwrap();
    for id in 0..rows {
        apply(
            &mut image,
            admin(json!({"command":"seed","key":key(id),"row":row()})),
        );
    }
    apply(
        &mut image,
        admin(json!({"command":"grant","grant":grant(transitions,10)})),
    );
    apply(
        &mut image,
        admin(
            json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":10000,"disclosed_rows":responses,"encoded_bytes":bytes}}),
        ),
    );
    image
}
fn accounting(image: &Image) -> TaskAccounting {
    image.accounting(TaskId(1)).unwrap().unwrap()
}
fn fixed(response: &Value, code: &str) {
    assert_eq!(
        response,
        &json!({"version":1,"code":code,"operation":null,"count":null,"digest":null,"sql":null})
    );
}

#[test]
fn redacted_preview_execute_status_and_exact_p2_charge() {
    let mut image = fixture(2, 2, 10, 100000);
    let start = image.clone();
    let preview = apply(&mut image, native(TaskAction::Preview(request(1, &[0, 1]))));
    assert_eq!(preview["code"], "preview");
    assert_eq!(preview["count"], 2);
    assert_eq!(preview.as_object().unwrap().len(), 6);
    let (_, trusted) = start
        .evaluate(&task(TaskAction::Preview(request(1, &[0, 1]))))
        .unwrap();
    let effects: Vec<Effect> = serde_json::from_value(trusted.data["effects"].clone()).unwrap();
    let expected = hex::encode(Sha256::digest(serde_json::to_vec(&effects).unwrap()));
    assert_eq!(preview["digest"], expected);
    assert!(!preview.to_string().contains("effects"));
    assert!(!preview.to_string().contains("preview\":"));
    assert_eq!(
        accounting(&image).used.encoded_bytes,
        serde_json::to_vec(&preview).unwrap().len() as u64 + 1024
    );
    let execute = apply(
        &mut image,
        native(TaskAction::Execute(request(1, &[0, 1]), None)),
    );
    assert_eq!(execute["code"], "committed");
    assert_eq!(execute["digest"], preview["digest"]);
    let status = apply(&mut image, native(TaskAction::Status(OperationId(1))));
    assert_eq!(status, execute);
    let retry = apply(
        &mut image,
        native(TaskAction::Execute(request(1, &[0, 1]), None)),
    );
    assert_eq!(retry, execute);
    assert_eq!(image.snapshot().unwrap().tasks[&TaskId(1)].transitions, 2);
    assert_eq!(accounting(&image).used.disclosed_rows, 4);
}

#[test]
fn protected_denials_conflicts_and_absence_each_consume_disclosure() {
    let mut image = fixture(2, 1, 10, 100000);
    let absent = apply(&mut image, native(TaskAction::Status(OperationId(77))));
    assert_eq!(absent["code"], "absent");
    let denied = apply(
        &mut image,
        native(TaskAction::Execute(request(2, &[0, 1]), None)),
    );
    assert_eq!(denied["code"], "54000");
    assert_eq!(denied["operation"], 2);
    assert!(denied["digest"].is_null());
    let status = apply(&mut image, native(TaskAction::Status(OperationId(2))));
    assert_eq!(status, denied);
    let conflict = apply(
        &mut image,
        native(TaskAction::Execute(request(2, &[1]), None)),
    );
    assert_eq!(conflict["code"], "GD002");
    assert_eq!(accounting(&image).used.disclosed_rows, 4);
    assert_eq!(image.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
}

#[test]
fn failed_release_preserves_committed_business_outcome_without_leaking_it() {
    for (responses, bytes) in [(0, 100000), (10, 1)] {
        let mut image = fixture(1, 1, responses, bytes);
        fixed(
            &apply(
                &mut image,
                native(TaskAction::Execute(request(4, &[0]), None)),
            ),
            "42501",
        );
        let snapshot = image.snapshot().unwrap();
        assert_eq!(snapshot.tasks[&TaskId(1)].transitions, 1);
        assert_eq!(snapshot.operations.len(), 1);
        fixed(
            &apply(
                &mut image,
                native(TaskAction::Execute(request(4, &[0]), None)),
            ),
            "42501",
        );
        fixed(
            &apply(&mut image, native(TaskAction::Status(OperationId(4)))),
            "42501",
        );
        assert_eq!(image.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
        assert_eq!(accounting(&image).used.disclosed_rows, 0);
        assert_eq!(accounting(&image).used.encoded_bytes, 0);
    }
}

#[test]
fn exact_encoded_byte_boundary_is_all_or_nothing() {
    let mut generous = fixture(0, 1, 10, 100000);
    let response = apply(&mut generous, native(TaskAction::Status(OperationId(123))));
    let charge = serde_json::to_vec(&response).unwrap().len() as u64 + 1024;
    for (bytes, accepted) in [(charge - 1, false), (charge, true)] {
        let mut image = fixture(0, 1, 10, bytes);
        let output = apply(&mut image, native(TaskAction::Status(OperationId(123))));
        if accepted {
            assert_eq!(output, response);
        } else {
            fixed(&output, "42501");
        }
        assert_eq!(
            accounting(&image).used.encoded_bytes,
            if accepted { charge } else { 0 }
        );
    }
}

#[test]
fn current_authority_and_time_fail_closed_even_for_deduped_results() {
    let mut base = fixture(1, 1, 20, 100000);
    apply(
        &mut base,
        native(TaskAction::Execute(request(5, &[0]), None)),
    );
    let before = accounting(&base).used.disclosed_rows;
    for (principal, lower, upper, code) in [
        (Principal(9), 10, 10, "42501"),
        (Principal(2), 11, 10, "55000"),
        (Principal(2), 9, 9, "55000"),
        (Principal(2), 1001, 1001, "42501"),
    ] {
        let mut image = base.clone();
        fixed(
            &apply(
                &mut image,
                Command::NativeTask {
                    task: TaskId(1),
                    principal,
                    lower,
                    upper,
                    action: TaskAction::Status(OperationId(5)),
                },
            ),
            code,
        );
        assert_eq!(accounting(&image).used.disclosed_rows, before);
    }
    let mut revoked = base;
    apply(&mut revoked, admin(json!({"command":"revoke","task":1})));
    fixed(
        &apply(&mut revoked, native(TaskAction::Status(OperationId(5)))),
        "42501",
    );
    assert_eq!(accounting(&revoked).used.disclosed_rows, before);
}

#[test]
fn replicated_last_allowance_orders_overlapping_disclosures_and_restart() {
    let mut c = Cluster::new(151);
    c.submit(1, admin(json!({"command":"grant","grant":grant(1,5)})));
    c.submit(1,admin(json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":1000,"disclosed_rows":1,"encoded_bytes":100000}})));
    for id in [901u64, 902] {
        c.nodes
            .get_mut(&1)
            .unwrap()
            .propose(
                id.to_be_bytes().to_vec(),
                &native(TaskAction::Status(OperationId(id))),
            )
            .unwrap();
    }
    c.pump();
    let outputs = [901u64, 902].map(|id| c.responses[&id.to_be_bytes().to_vec()]["data"].clone());
    assert_eq!(outputs.iter().filter(|r| r["code"] == "absent").count(), 1);
    assert_eq!(outputs.iter().filter(|r| r["code"] == "42501").count(), 1);
    c.equal();
    c.checkpoint_all();
    for id in VOTERS {
        c.restart(id);
    }
    c.nodes.get_mut(&1).unwrap().campaign().unwrap();
    c.pump();
    fixed(
        &c.submit(1, native(TaskAction::Status(OperationId(901))))["data"],
        "42501",
    );
    assert_eq!(
        accounting(&c.nodes[&1].store.image().unwrap())
            .used
            .disclosed_rows,
        1
    );
    c.equal();
}

#[test]
fn deterministic_replay_has_identical_image_and_safe_response() {
    let image = fixture(1, 1, 10, 100000);
    let command = native(TaskAction::Execute(request(1, &[0]), None));
    let (a, ra) = image.evaluate(&command).unwrap();
    let (b, rb) = image.evaluate(&command).unwrap();
    assert_eq!(a, b);
    assert_eq!(ra.data, rb.data);
    assert!(ra.data.get("effects").is_none());
    assert!(ra.data.get("preview").is_none());
}
