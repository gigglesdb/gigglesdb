//! Compare against the pre-R5 clone/epoch-adjust/full-bind validation path.
use super::*;

fn reference(p: &Prepared, state: &Relational, epoch: u64) -> R<()> {
    let mut metadata_plan = p.clone();
    metadata_plan.epoch = epoch;
    let bound = metadata_plan.bind_schemas(state)?;
    if bound.epoch != state.epoch {
        return Err(SqlError::StaleSchema);
    }
    match bound.statement.get() {
        Statement::InsertExpressions(values) => returning::validate_insert_constants(
            values,
            state,
            bound.params,
            &bound.parameter_types,
        ),
        Statement::Plan(plan) => {
            plan.validate_immutable_constants(state, bound.params, &bound.parameter_types)
        }
        Statement::Query(q) => q
            .bind(state, bound.params, &bound.parameter_types)?
            .validate_immutable_constants(),
        _ => Ok(()),
    }
}
fn compare(p: &Prepared, state: &Relational) {
    for epoch in [p.epoch, state.epoch, state.epoch.wrapping_add(1)] {
        assert_eq!(
            format!(
                "{:?}",
                p.validate_immutable_constants_at_epoch(state, epoch, None)
            ),
            format!("{:?}", reference(p, state, epoch))
        );
    }
}
fn run(state: &mut Relational, sql: &str) {
    let p = Prepared::parse(sql, 1, state.epoch).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    state
        .execute(&p, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
#[test]
fn full_validation_reference_matrix() {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE t(id int PRIMARY KEY, n int)");
    for sql in [
        "SELECT 1/0 FROM t",
        "SELECT id FROM t ORDER BY 1/0",
        "SELECT id FROM t WHERE 1/0 = 1",
        "SELECT 1+2 FROM t",
        "SELECT id FROM t WHERE id=$1",
        "SELECT id FROM t ORDER BY id",
        "SELECT id FROM t ORDER BY n",
        "SELECT count(*) FROM t",
        "SELECT '2001-02-03'::date",
        "SELECT '2001-02-03 04:05:06+00'::timestamptz",
        "SELECT '1 day'::interval::text",
        "SELECT 1.23456789::float8::text",
    ] {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        compare(&p, &state);
        // The original remains usable, with its epoch and syntax unchanged.
        compare(&p, &state);
    }
}
#[test]
fn full_validation_reference_catalog_changes() {
    let mut state = Relational::default();
    run(&mut state, "CREATE SCHEMA earlier");
    run(&mut state, "CREATE TABLE t(id int PRIMARY KEY, n int)");
    let mut p = Prepared::parse("SELECT id,n FROM t", 1, state.epoch).unwrap();
    p.search_path = vec!["earlier".into(), "public".into()];
    compare(&p, &state);
    for sql in [
        "ALTER TABLE t ADD COLUMN extra int",
        "ALTER TABLE t DROP COLUMN extra",
        "ALTER TABLE t ALTER COLUMN n TYPE text",
        "ALTER TABLE t DROP COLUMN n",
        "ALTER TABLE t ADD COLUMN n text",
        "DROP TABLE t",
        "CREATE TABLE t(id int PRIMARY KEY, n text)",
        "CREATE TABLE earlier.t(id int PRIMARY KEY, n int)",
    ] {
        run(&mut state, sql);
        compare(&p, &state);
    }
    p.search_path = vec!["public".into()];
    compare(&p, &state);
    let p = Prepared::parse("SELECT id,n FROM t", 1, state.epoch).unwrap();
    compare(&p, &state);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn memo_preflight_matches_full_validation_guards() {
    let path = std::env::temp_dir().join(format!("validation-preflight-{}-{}",
        std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = crate::Admin::create_engine(&path, crate::Limits::default(), crate::Engine::V2).unwrap();
    let mut session = admin.sql_session();
    let p = session.prepare("SELECT 1").unwrap();
    for (domain, transaction) in [(p.domain, None), (p.domain.wrapping_add(1), None), (p.domain, Some(42))] {
        let mut p = p.clone();
        p.domain = domain;
        p.transaction = transaction;
        assert_eq!(format!("{:?}", session.constants_validated_preflight(&p)),
            format!("{:?}", session.validate_immutable_constants(&p)));
    }
    session.execute("BEGIN", &[], None).unwrap();
    session.abort();
    assert_eq!(format!("{:?}", session.constants_validated_preflight(&p)),
        format!("{:?}", session.validate_immutable_constants(&p)));
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
