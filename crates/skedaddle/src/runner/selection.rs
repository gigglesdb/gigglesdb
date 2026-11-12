//! Resolve explicit exclusions from observed column names before catalog gates.
use crate::{definition::ObjectSelector, pg::catalog, security::Redactor};

use super::{Failure, FailureKind};

// Catalog observations identify objects, independently of migration projection.
pub(super) fn catalog_objects(objects: &[ObjectSelector]) -> Vec<ObjectSelector> {
    objects
        .iter()
        // Reject reserved objects from catalog selection as well as projection.
        // PG folds unquoted names; the tool always quotes this exact name.
        .filter(|object| object.schema != "_skedaddle")
        .cloned()
        .map(|mut object| {
            object.exclude_columns.clear();
            object
        })
        .collect()
}

pub(super) fn from_observed(
    object: &ObjectSelector,
    columns: Vec<String>,
    redactor: &Redactor,
) -> Result<catalog::TableSelection, Failure> {
    if object.schema == "_skedaddle" {
        return Err(Failure {
            kind: FailureKind::Blocked,
            what: "_skedaddle is reserved for tool metadata and cannot be selected".into(),
            next_step: "Remove reserved-schema objects from the migration selection.".into(),
            table: Some(redactor.redact(&format!("{}.{}", object.schema, object.name))),
            column: None,
        });
    }
    for excluded in &object.exclude_columns {
        if !columns.contains(excluded) {
            let table = redactor.redact(&format!("{}.{}", object.schema, object.name));
            let column = redactor.redact(excluded);
            return Err(Failure {
                kind: FailureKind::Blocked,
                what: format!("{table}: excluded column {column} was not observed in the source catalog"),
                next_step: "Correct the excluded column name or restore the source column, then recollect live catalog evidence.".into(),
                table: Some(table),
                column: Some(column),
            });
        }
    }
    Ok(catalog::TableSelection {
        schema: object.schema.clone(),
        name: object.name.clone(),
        columns: columns
            .into_iter()
            .filter(|column| !object.exclude_columns.contains(column))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::ObjectKind;

    fn object() -> ObjectSelector {
        ObjectSelector {
            schema: "public".into(),
            name: "rows".into(),
            kind: ObjectKind::Table,
            exclude_columns: vec!["private_payload".into()],
        }
    }

    #[test]
    fn projection_preserves_observed_order_and_operational_keys_for_catalog_validation() {
        let selected = from_observed(
            &object(),
            vec!["token".into(), "private_payload".into(), "id".into()],
            &Redactor::default(),
        )
        .unwrap();
        assert_eq!(selected.columns, ["token", "id"]);
        let mut object = object();
        object.exclude_columns = vec!["id".into()];
        // The existing catalog gate, not this name projection, decides key eligibility.
        assert_eq!(
            from_observed(
                &object,
                vec!["id".into(), "body".into()],
                &Redactor::default()
            )
            .unwrap()
            .columns,
            ["body"]
        );
    }

    #[test]
    fn unknown_exclusion_blocks_with_column_and_actionable_next_step() {
        let blocked =
            from_observed(&object(), vec!["id".into()], &Redactor::default()).unwrap_err();
        assert!(matches!(blocked.kind, FailureKind::Blocked));
        assert_eq!(blocked.table.as_deref(), Some("public.rows"));
        assert_eq!(blocked.column.as_deref(), Some("private_payload"));
        assert!(blocked.what.contains("private_payload"));
        assert!(
            blocked
                .next_step
                .contains("Correct the excluded column name")
        );
    }

    #[test]
    fn reserved_schema_is_never_admitted_by_either_entry_point() {
        let mut internal = object();
        internal.schema = "_skedaddle".into();
        assert!(catalog_objects(&[internal.clone()]).is_empty());
        let failure = from_observed(
            &internal,
            vec!["id".into(), "private_payload".into()],
            &Redactor::default(),
        )
        .unwrap_err();
        assert!(matches!(failure.kind, FailureKind::Blocked));
        assert!(failure.what.contains("reserved"));
        for schema in ["public", "_Skedaddle", "_skedaddle_extra"] {
            let mut ordinary = internal.clone();
            ordinary.schema = schema.into();
            let selected = catalog_objects(&[internal.clone(), ordinary.clone()]);
            assert_eq!(selected.len(), 1);
            assert_eq!(selected[0].schema, schema);
            assert!(
                from_observed(
                    &ordinary,
                    vec!["id".into(), "private_payload".into()],
                    &Redactor::default(),
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn catalog_identity_ignores_projection_without_mutating_definition() {
        let objects = vec![object()];
        let normalized = catalog_objects(&objects);
        assert!(normalized[0].exclude_columns.is_empty());
        assert_eq!(normalized[0].schema, objects[0].schema);
        assert_eq!(normalized[0].name, objects[0].name);
        assert_eq!(normalized[0].kind, objects[0].kind);
        assert_eq!(objects[0].exclude_columns, ["private_payload"]);
        assert_eq!(catalog_objects(&normalized), normalized);
    }
}
