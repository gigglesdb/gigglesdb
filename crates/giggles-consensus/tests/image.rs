mod common;
use common::*;
use giggles_consensus::*;
use giggles_core::*;
use serde_json::json;

#[test]
fn kernel_image_deterministic_all_catalog_and_authority_namespaces() {
    let mut image = Image::new(42).unwrap();
    let mut commands = vec![
        admin(
            json!({"command":"sql","sql":"CREATE TABLE app (id BIGINT PRIMARY KEY, value TEXT)","params":[],"expected_version":null}),
        ),
        admin(
            json!({"command":"sql","sql":"INSERT INTO app VALUES(1,'one')","params":[],"expected_version":null}),
        ),
        admin(json!({"command":"seed","key":key(0),"row":row()})),
        admin(json!({"command":"grant","grant":grant(1,5)})),
        admin(
            json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":100,"disclosed_rows":10,"encoded_bytes":100000}}),
        ),
        task(TaskAction::Execute(request(1, &[0]), None)),
    ];
    for command in commands.drain(..) {
        let (a, ra) = image.evaluate(&command).unwrap();
        let (b, rb) = image.evaluate(&command).unwrap();
        assert_eq!(a, b);
        assert_eq!(
            serde_json::to_value(ra).unwrap(),
            serde_json::to_value(rb).unwrap()
        );
        image = Image::from_bytes(a.bytes().to_vec()).unwrap();
    }
    let (_,r)=image.evaluate(&admin(json!({"command":"sql","sql":"SELECT * FROM app WHERE id=1","params":[],"expected_version":null}))).unwrap();
    assert_eq!(r.code, 0);
    assert!(r.data.to_string().contains("one"));
    let s = image.snapshot().unwrap();
    assert_eq!(s.domain, 42);
    assert_eq!(s.operations.len(), 1);
    assert_eq!(s.tasks[&TaskId(1)].transitions, 1);
    assert!(image.accounting(TaskId(1)).unwrap().unwrap().used.read_rows > 0);
    assert!(
        image
            .evaluate(&admin(json!({"command":"export","path":"must-not-create"})))
            .is_err()
    );
    let mut bad = image.bytes().to_vec();
    bad.push(0);
    assert!(Image::from_bytes(bad).is_err());
}

#[test]
fn disclosure_budget_is_preserved_and_never_refunded_after_lost_delivery() {
    let mut c = Cluster::new(21);
    c.provision(1, 1, 5);
    let before = c.nodes[&1]
        .store
        .image()
        .unwrap()
        .accounting(TaskId(1))
        .unwrap()
        .unwrap();
    c.submit(1, task(TaskAction::Preview(request(1, &[0]))));
    let after = c.nodes[&1]
        .store
        .image()
        .unwrap()
        .accounting(TaskId(1))
        .unwrap()
        .unwrap();
    assert_eq!(after.used.disclosed_rows, before.used.disclosed_rows + 1);
    assert!(after.used.encoded_bytes > before.used.encoded_bytes);
    c.checkpoint_all();
    for id in VOTERS {
        c.restart(id);
    }
    c.nodes.get_mut(&1).unwrap().campaign().unwrap();
    c.pump();
    assert_eq!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .accounting(TaskId(1))
            .unwrap()
            .unwrap(),
        after
    );
    c.submit(1, task(TaskAction::Preview(request(1, &[0]))));
    let twice = c.nodes[&1]
        .store
        .image()
        .unwrap()
        .accounting(TaskId(1))
        .unwrap()
        .unwrap();
    assert_eq!(twice.used.disclosed_rows, after.used.disclosed_rows + 1);
    c.equal();
    c.hash_evidence();
}
