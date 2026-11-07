mod common;
use giggles_core::{
    Admin, Limits, Principal, TaskId, TaskResources, Time,
    operations::{Engine, Error, UniqueIndexRequest},
};
#[test]
fn public_unique_all_business_predicates_refuse_without_effects() {
    for kind in 0..6 {
        let admin =
            Admin::create(common::fresh("public-unique-business"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT)",
                &[],
                None,
            )
            .unwrap();
        if kind == 0 {
            admin.put(common::key(1), common::row()).unwrap();
        } else {
            let executor = admin.grant(common::grant()).unwrap();
            match kind {
                1 => {}
                2 => admin.revoke(TaskId(1)).unwrap(),
                3 => admin
                    .provision_task_resources(TaskId(1), TaskResources::default())
                    .unwrap(),
                4 => {
                    let _decision = executor
                        .execute(common::predicate(1), Time::Certified(10), None)
                        .unwrap();
                }
                5 => {
                    let p = executor
                        .preview(&common::predicate(1), Time::Certified(10))
                        .unwrap();
                    let a = admin.approve(Principal(3), p, Time::Certified(10)).unwrap();
                    admin.revoke_approval(a).unwrap();
                }
                _ => unreachable!(),
            }
        }
        let before = admin.export(common::fresh("unique-digest")).unwrap();
        let engine = Engine::new(admin.clone());
        let request = UniqueIndexRequest {
            table: "items".into(),
            name: "unique_tag".into(),
            columns: vec!["tag".into()],
        };
        let bound = engine.prepare_unique_index_bound(&request).unwrap();
        assert_eq!(
            admin.export(common::fresh("unique-digest")).unwrap(),
            before
        );
        assert_eq!(
            engine.execute_unique_index_bound(&bound),
            Err(Error::PlanRequired)
        );
        assert_eq!(
            engine.execute_unique_index(&request),
            Err(Error::PlanRequired)
        );
        assert_eq!(
            admin.export(common::fresh("unique-digest")).unwrap(),
            before
        );
    }
}
