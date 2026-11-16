use super::*;
use crate::{
    pg::chunk::ChunkId,
    verify::{Coverage, TableCoverage},
};

struct IntOrder;
impl KeyOrder for IntOrder {
    fn compare_keys(&self, a: &Key, b: &Key) -> Result<Ordering> {
        match (a.as_slice(), b.as_slice()) {
            ([Value::Int8(a)], [Value::Int8(b)]) => Ok(a.cmp(b)),
            _ => Err(VerifyError::Invalid("integer key required")),
        }
    }
}
fn key(n: i64) -> Key {
    vec![Value::Int8(n)]
}
fn range() -> RangePlan {
    RangePlan {
        id: ChunkId {
            table: "t".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        lo: Some(key(0)),
        hi: Some(key(10)),
    }
}

#[test]
fn range_caps_are_inclusive_and_zero_caps_rejected() {
    let budget = RangeBudget {
        max_rows: 10,
        max_bytes: 100,
    };
    assert!(budget.accepts(10, 100));
    assert!(!budget.accepts(11, 100));
    assert!(!budget.accepts(10, 101));
    assert!(
        RangeBudget {
            max_rows: 0,
            max_bytes: 100
        }
        .validate()
        .is_err()
    );
    assert!(
        RangeBudget {
            max_rows: 10,
            max_bytes: 0
        }
        .validate()
        .is_err()
    );
}

#[test]
fn splits_are_deterministic_contiguous_and_invalidate_parent_plan() {
    let parent = range();
    let split = split_range(&IntOrder, &parent, key(5)).unwrap();
    assert_eq!(split, split_range(&IntOrder, &parent, key(5)).unwrap());
    assert_eq!(split[0].lo, parent.lo);
    assert_eq!(split[1].hi, parent.hi);
    assert_eq!(split[0].hi, split[1].lo);
    assert_ne!(split[0].id.plan, parent.id.plan);
    assert_eq!(split[0].id.plan, split[1].id.plan);
    for invalid in [key(0), key(10), vec![Value::Null], vec![Value::Int4(5)]] {
        assert!(split_range(&IntOrder, &parent, invalid).is_err());
    }
}

#[test]
fn source_lsn_is_parsed_without_cross_lineage_comparison() {
    assert_eq!(parse_lsn("1/FFFFFFFF").unwrap(), 0x1_ffff_ffff);
    assert!(parse_lsn("100000000/0").is_err());
    assert!(parse_lsn("bad").is_err());
}

fn catalog() -> CatalogSections {
    CatalogSections {
        columns: vec![SchemaColumn {
            name: "amount".into(),
            position: 1,
            sql_type: "bigint".into(),
            nullable: false,
            default: Some("1".into()),
            generated: String::new(),
            identity: String::new(),
        }],
        sequences: vec![SequenceState {
            name: "seq".into(),
            last_value: 10,
            is_called: true,
            increment: 1,
            min: 1,
            max: i64::MAX,
            cycle: false,
        }],
        constraints: vec![ConstraintState {
            name: "check".into(),
            kind: "c".into(),
            definition: "CHECK (amount > 0)".into(),
            validated: true,
        }],
        indexes: vec![IndexState {
            name: "pk".into(),
            definition: "UNIQUE (id)".into(),
            valid: true,
        }],
    }
}

#[test]
fn typed_sections_detect_each_schema_attribute_and_missing_extra_objects() {
    let src = catalog();
    assert_eq!(diff_sections(&src, &src), SectionDifferences::default());
    for change in 0..7 {
        let mut dst = src.clone();
        match change {
            0 => dst.columns[0].sql_type = "integer".into(),
            1 => dst.columns[0].nullable = true,
            2 => dst.columns[0].default = None,
            3 => dst.columns[0].generated = "s".into(),
            4 => dst.columns[0].identity = "a".into(),
            5 => dst.columns[0].position = 2,
            _ => dst.columns.clear(),
        }
        assert_eq!(diff_sections(&src, &dst).schema.len(), 1);
        assert_eq!(diff_sections(&dst, &src).schema.len(), 1);
    }
    let mut dst = src.clone();
    dst.constraints[0].validated = false;
    dst.indexes[0].valid = false;
    assert_eq!(diff_sections(&src, &dst).constraints.len(), 1);
    assert_eq!(diff_sections(&dst, &dst).constraints.len(), 1);
    assert_eq!(diff_sections(&src, &dst).indexes.len(), 1);
    assert_eq!(diff_sections(&dst, &dst).indexes.len(), 1);
    dst.constraints.clear();
    dst.indexes.clear();
    assert_eq!(diff_sections(&src, &dst).constraints.len(), 1);
    assert_eq!(diff_sections(&src, &dst).indexes.len(), 1);
}

#[test]
fn sequence_diff_accounts_for_is_called_direction_exhaustion_and_cycle() {
    let src = catalog();
    let mut dst = src.clone();
    dst.sequences[0].is_called = false;
    assert_eq!(diff_sections(&src, &dst).sequences.len(), 1);
    dst.sequences[0].last_value = 11;
    assert!(diff_sections(&src, &dst).sequences.is_empty());
    dst.sequences[0].last_value = 100;
    assert!(diff_sections(&src, &dst).sequences.is_empty());
    let mut src = src;
    src.sequences[0].increment = -1;
    src.sequences[0].min = i64::MIN;
    dst = src.clone();
    dst.sequences[0].last_value = 11;
    assert_eq!(diff_sections(&src, &dst).sequences.len(), 1);
    src.sequences[0].last_value = i64::MIN;
    assert_eq!(diff_sections(&src, &dst).sequences.len(), 1);
    src.sequences[0].cycle = true;
    dst = src.clone();
    assert_eq!(diff_sections(&src, &dst).sequences.len(), 1);
}

fn repair() -> (RepairRequest, RepairContext, TableCoverage) {
    let request = RepairRequest {
        version: REPAIR_VERSION,
        scope: RepairScope {
            range: range(),
            key: key(5),
            columns: vec!["amount".into()],
        },
        source_read_revision: 10,
        source_read_position: pg_position(200),
    };
    let context = RepairContext {
        expected_scope: request.scope.clone(),
        authority_transferred: false,
        destination_change_revision: None,
    };
    let coverage = TableCoverage {
        table: "t".into(),
        rows: Coverage {
            total: 10,
            verified: 10,
            ..Coverage::default()
        },
        exclusion_reason: None,
    };
    (request, context, coverage)
}

#[test]
fn repair_is_versioned_scoped_and_invalidates_coverage() {
    let (mut request, mut context, coverage) = repair();
    let result = decide_repair(&IntOrder, &request, &context, &coverage).unwrap();
    assert_eq!(result.invalidated_coverage.rows.total, 10);
    assert_eq!(result.invalidated_coverage.rows.excluded, 10);
    assert_eq!(result.invalidated_coverage.rows.verified, 0);
    assert!(!result.invalidated_coverage.rows.is_complete());
    assert_eq!(coverage.rows.verified, 10);
    request.version = 99;
    assert_eq!(
        decide_repair(&IntOrder, &request, &context, &coverage),
        Err(RepairRefusal::Version)
    );
    request.version = REPAIR_VERSION;
    request.scope.columns.push("out_of_scope".into());
    assert_eq!(
        decide_repair(&IntOrder, &request, &context, &coverage),
        Err(RepairRefusal::Scope)
    );
    context.expected_scope = request.scope.clone();
    request.scope.key = key(10);
    context.expected_scope = request.scope.clone();
    assert_eq!(
        decide_repair(&IntOrder, &request, &context, &coverage),
        Err(RepairRefusal::Scope)
    );
}

#[test]
fn repair_refuses_newer_or_unknown_destination_changes_after_transfer() {
    let (request, mut context, coverage) = repair();
    context.authority_transferred = true;
    assert_eq!(
        decide_repair(&IntOrder, &request, &context, &coverage),
        Err(RepairRefusal::UnknownDestinationChange)
    );
    context.destination_change_revision = Some(11);
    assert_eq!(
        decide_repair(&IntOrder, &request, &context, &coverage),
        Err(RepairRefusal::NewerDestinationChange)
    );
    for revision in [0, 9, 10] {
        context.destination_change_revision = Some(revision);
        assert!(decide_repair(&IntOrder, &request, &context, &coverage).is_ok());
    }
}

fn pg_position(lsn: u64) -> crate::event::Position {
    crate::verify::sql::PgSourcePosition::new("test-pg").position(lsn)
}

#[test]
fn pg_position_adapter_matches_numeric_lsn_decisions_golden() {
    let pg = PgSourcePosition::new("pg-golden");
    let order = crate::pg::apply::NativePgOrder("pg-golden".into());
    let lsns = [
        0,
        1,
        255,
        256,
        u32::MAX as u64,
        1_u64 << 32,
        (1_u64 << 32) + 1,
        u64::MAX,
    ];
    for a in lsns {
        assert_eq!(pg.lsn(&pg.position(a)).unwrap(), a);
        for b in lsns {
            let actual =
                super::super::compare_positions(&order, &pg.position(a), &pg.position(b)).unwrap();
            assert_eq!(actual, a.cmp(&b));
            assert_eq!(actual == Ordering::Less, a < b);
            assert_eq!(actual != Ordering::Less, a >= b);
        }
    }
    assert!(matches!(
        pg.lsn(&Position::new("other", 1_u64.to_be_bytes())),
        Err(VerifyError::Position(
            crate::event::EventError::PositionDomainMismatch
        ))
    ));
    assert!(matches!(
        pg.lsn(&Position::new("pg-golden", [1])),
        Err(VerifyError::Position(_))
    ));
}

#[test]
fn repair_retains_native_source_boundary_and_shared_revision_refusals() {
    let (mut request, mut context, coverage) = repair();
    request.source_read_position = Position::new("native-repair", b"revision-10");
    let decision = decide_repair(&IntOrder, &request, &context, &coverage).unwrap();
    assert_eq!(
        decision.request.source_read_position,
        request.source_read_position
    );
    context.authority_transferred = true;
    assert_eq!(
        decide_repair(&IntOrder, &request, &context, &coverage),
        Err(RepairRefusal::UnknownDestinationChange)
    );
    context.destination_change_revision = Some(11);
    assert_eq!(
        decide_repair(&IntOrder, &request, &context, &coverage),
        Err(RepairRefusal::NewerDestinationChange)
    );
    context.destination_change_revision = Some(10);
    assert!(decide_repair(&IntOrder, &request, &context, &coverage).is_ok());
}

#[test]
fn dialect_sql_preserves_postgres_and_flattens_gigglesdb_budget() {
    let select = vec!["v.\"id\"::text".into(), "v.\"payload\"::text".into()];
    assert_eq!(
        SqlDialect::Postgres.size_sql(&select, " FROM t v"),
        "SELECT count(*)::text, COALESCE(sum(8::numeric + COALESCE(octet_length(\"v0\"), 0) + 8::numeric + COALESCE(octet_length(\"v1\"), 0)), 0)::text FROM (SELECT v.\"id\"::text AS \"v0\", v.\"payload\"::text AS \"v1\" FROM t v) AS sized"
    );
    assert_eq!(
        SqlDialect::Gigglesdb.size_sql(&select, " FROM t v WHERE v.id >= $1"),
        "SELECT CAST(count(*) AS text), COALESCE(sum(8::numeric + COALESCE(octet_length(v.\"id\"::text), 0) + 8::numeric + COALESCE(octet_length(v.\"payload\"::text), 0)), 0)::text FROM t v WHERE v.id >= $1"
    );
}

#[test]
fn dialect_char_projection_keeps_padding() {
    let ty = PgType::Char {
        encoding: "UTF8".into(),
    };
    assert_eq!(
        SqlDialect::Postgres.text_expression("v.c", &ty),
        "pg_catalog.bpcharout(v.c)::text"
    );
    assert_eq!(SqlDialect::Gigglesdb.text_expression("v.c", &ty), "v.c");
    assert_eq!(
        SqlDialect::Gigglesdb.text_expression("v.i", &PgType::Int4),
        "v.i::text"
    );
}

#[test]
fn gigglesdb_sample_has_ordering_without_window_function() {
    assert_eq!(
        SqlDialect::ordered_sample_sql(
            "v.a::text, v.b::text",
            " FROM t v WHERE v.a >= $1",
            "v.a ASC, v.b DESC"
        ),
        "SELECT v.a::text, v.b::text FROM t v WHERE v.a >= $1 ORDER BY v.a ASC, v.b DESC"
    );
}

#[test]
fn gigglesdb_char_budget_requires_length_and_restores_padding() {
    let ty = PgType::Char {
        encoding: "UTF8".into(),
    };
    assert_eq!(
        SqlDialect::Gigglesdb
            .size_expression("v.c", &ty, Some(9))
            .unwrap(),
        "rpad(v.c::text, 5, ' ')"
    );
    for typmod in [None, Some(-1), Some(4), Some(i32::MIN)] {
        assert!(matches!(SqlDialect::Gigglesdb.size_expression("v.c", &ty, typmod),
            Err(VerifyError::Invalid("GigglesDB verification char byte budget requires a known positive char(n) length modifier"))));
    }
    assert_eq!(
        SqlDialect::Postgres
            .size_expression("v.c", &ty, None)
            .unwrap(),
        "pg_catalog.bpcharout(v.c)::text"
    );
}
