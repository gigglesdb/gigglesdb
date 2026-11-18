//! Lower structured Spanner requests without retaining their values.
use crate::{
    error::{NyCode, Refusal},
    ir::*,
    policy::{Policy, Treatment},
    schema::{Column, ColumnRef, Relation, Snapshot},
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{KeySet, Mutation, ReadRequest, key_range, mutation};

/// Location of a parameter in its original structured request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValuePosition {
    /// A primary-key part of a read.
    Key {
        /// Key index.
        key: usize,
        /// Part index.
        part: usize,
    },
    /// A cell in a mutation's values.
    MutationValue {
        /// Row index.
        row: usize,
        /// Column index.
        column: usize,
    },
    /// A primary-key part of a delete.
    DeleteKey {
        /// Key index.
        key: usize,
        /// Part index.
        part: usize,
    },
}
/// Resolved statement and one-based parameter location mapping.
#[derive(Debug, Clone, PartialEq)]
pub struct Lowered {
    /// Policy-checkable statement.
    pub stmt: ResolvedStatement,
    /// Parameter n is at positions[n - 1].
    pub positions: Vec<ValuePosition>,
}
fn refuse() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "structured request unavailable")
}
fn table<'a>(snapshot: &'a Snapshot, name: &str) -> Result<&'a Relation, Refusal> {
    let (schema, name) = name.split_once('.').unwrap_or(("", name));
    let mut found = snapshot
        .relations
        .iter()
        .filter(|r| r.schema.eq_ignore_ascii_case(schema) && r.name.eq_ignore_ascii_case(name));
    let result = found.next().ok_or_else(refuse)?;
    if found.next().is_some() {
        return Err(refuse());
    }
    Ok(result)
}
fn column<'a>(table: &'a Relation, name: &str) -> Result<&'a Column, Refusal> {
    let mut found = table
        .columns
        .iter()
        .filter(|c| c.name.eq_ignore_ascii_case(name));
    let result = found.next().ok_or_else(refuse)?;
    if found.next().is_some() {
        return Err(refuse());
    }
    Ok(result)
}
fn reference(table: &Relation, column: &Column) -> ColumnRef {
    ColumnRef {
        schema: table.schema.clone(),
        table: table.name.clone(),
        column: column.name.clone(),
    }
}
fn label(policy: &Policy, column: &ColumnRef) -> Label {
    match policy.treatment(column) {
        Some(Treatment::Blind { domain }) => Label::Blind(domain.clone()),
        Some(Treatment::Masked { .. }) => Label::Masked,
        Some(Treatment::Show) | None => Label::Plain,
    }
}
fn empty(kind: StatementKind) -> Lowered {
    Lowered {
        stmt: ResolvedStatement {
            canonical_sql: None,
            kind,
            outputs: vec![],
            uses: vec![],
            writes: vec![],
            param_count: 0,
            plain_params: vec![],
            source_span: Span { start: 0, end: 0 },
        },
        positions: vec![],
    }
}
impl Lowered {
    fn param(&mut self, position: ValuePosition) -> Result<ValueSource, Refusal> {
        self.stmt.param_count = self.stmt.param_count.checked_add(1).ok_or_else(refuse)?;
        self.positions.push(position);
        Ok(ValueSource::Param(self.stmt.param_count))
    }
}
fn keys(
    out: &mut Lowered,
    set: &KeySet,
    table: &Relation,
    policy: &Policy,
    delete: bool,
) -> Result<(), Refusal> {
    let pk = table
        .primary_key
        .iter()
        .map(|name| column(table, name))
        .collect::<Result<Vec<_>, _>>()?;
    for (key, values) in set.keys.iter().enumerate() {
        if values.values.len() > pk.len() {
            return Err(refuse());
        }
        for (part, col) in pk.iter().enumerate().take(values.values.len()) {
            let column = reference(table, col);
            let label = label(policy, &column);
            if label != Label::Plain {
                let position = if delete {
                    ValuePosition::DeleteKey { key, part }
                } else {
                    ValuePosition::Key { key, part }
                };
                let other = out.param(position)?;
                out.stmt.uses.push(ColumnUse {
                    column,
                    label,
                    kind: UseKind::Equality { other },
                    context: Context::Where,
                });
            }
        }
    }
    for range in &set.ranges {
        let start = match &range.start_key_type {
            Some(
                key_range::StartKeyType::StartClosed(v) | key_range::StartKeyType::StartOpen(v),
            ) => v,
            None => return Err(refuse()),
        };
        let end = match &range.end_key_type {
            Some(key_range::EndKeyType::EndClosed(v) | key_range::EndKeyType::EndOpen(v)) => v,
            None => return Err(refuse()),
        };
        for bound in [start, end] {
            if bound.values.len() > pk.len() {
                return Err(refuse());
            }
            for col in pk.iter().take(bound.values.len()) {
                let column = reference(table, col);
                let label = label(policy, &column);
                if label != Label::Plain {
                    out.stmt.uses.push(ColumnUse {
                        column,
                        label,
                        kind: UseKind::Comparison,
                        context: Context::Where,
                    });
                }
            }
        }
    }
    Ok(())
}
/// Lower a Read or StreamingRead for the shared policy checker.
pub fn lower_read(
    req: &ReadRequest,
    snapshot: &Snapshot,
    policy: &Policy,
) -> Result<Lowered, Refusal> {
    if !req.partition_token.is_empty() || req.data_boost_enabled {
        return Err(Refusal::new(
            NyCode::StatementNotAllowed,
            "partitioned read unsupported",
        ));
    }
    if !req.index.is_empty() {
        return Err(refuse());
    }
    let table = table(snapshot, &req.table)?;
    let mut out = empty(StatementKind::Select);
    for name in &req.columns {
        let col = column(table, name)?;
        let column = reference(table, col);
        let label = label(policy, &column);
        if label != Label::Plain {
            out.stmt.uses.push(ColumnUse {
                column: column.clone(),
                label: label.clone(),
                kind: UseKind::Output,
                context: Context::SelectList,
            });
        }
        out.stmt.outputs.push(OutputColumn {
            name: col.name.clone(),
            label,
            source: Some(column),
            ty: Some(col.ty.clone()),
        });
    }
    keys(
        &mut out,
        req.key_set.as_ref().ok_or_else(refuse)?,
        table,
        policy,
        false,
    )?;
    Ok(out)
}
/// Lower a single Commit mutation for the shared policy checker.
pub fn lower_mutation(
    m: &Mutation,
    snapshot: &Snapshot,
    policy: &Policy,
) -> Result<Lowered, Refusal> {
    let (write, kind) = match &m.operation {
        Some(
            mutation::Operation::Insert(w)
            | mutation::Operation::InsertOrUpdate(w)
            | mutation::Operation::Replace(w),
        ) => (w, StatementKind::Insert),
        Some(mutation::Operation::Update(w)) => (w, StatementKind::Update),
        Some(mutation::Operation::Delete(d)) => {
            let table = table(snapshot, &d.table)?;
            let mut out = empty(StatementKind::Delete);
            keys(
                &mut out,
                d.key_set.as_ref().ok_or_else(refuse)?,
                table,
                policy,
                true,
            )?;
            return Ok(out);
        }
        None => return Err(refuse()),
    };
    let table = table(snapshot, &write.table)?;
    let columns = write
        .columns
        .iter()
        .map(|name| column(table, name))
        .collect::<Result<Vec<_>, _>>()?;
    let mut out = empty(kind);
    for (row, values) in write.values.iter().enumerate() {
        if values.values.len() != columns.len() {
            return Err(refuse());
        }
        for (column_index, col) in columns.iter().enumerate() {
            let column = reference(table, col);
            let label = label(policy, &column);
            let value = out.param(ValuePosition::MutationValue {
                row,
                column: column_index,
            })?;
            if !matches!(label, Label::Blind(_)) {
                out.stmt.plain_params.push(out.stmt.param_count);
            }
            out.stmt.writes.push(WriteTarget {
                column,
                label,
                value,
            });
        }
    }
    Ok(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ir::check::check,
        policy::{DomainName, ReadLogSink},
        schema::{Column, RelationKind},
        value::ColumnType,
    };
    use prost_types::{ListValue, Value};
    fn fixture() -> (Snapshot, Policy) {
        let mut r = Relation {
            schema: "".into(),
            name: "Users".into(),
            kind: RelationKind::Table,
            columns: ["Email", "Name"]
                .into_iter()
                .map(|n| Column {
                    name: n.into(),
                    ty: ColumnType::Text,
                    nullable: false,
                    generated: None,
                    default: None,
                })
                .collect(),
            view_definition: None,
            primary_key: vec!["Email".into()],
        };
        let mut outbox = r.clone();
        outbox.name = "Outbox".into();
        let col = reference(&r, &r.columns[0]);
        r.primary_key = vec!["Email".into()];
        let mut p = Policy {
            columns: Default::default(),
            domains: Default::default(),
            snapshot_max_age_ms: 100,
            readlog: ReadLogSink::Stdout,
        };
        p.columns.insert(
            col,
            Treatment::Blind {
                domain: DomainName("email".into()),
            },
        );
        p.columns.insert(
            reference(&outbox, &outbox.columns[0]),
            Treatment::Blind {
                domain: DomainName("email".into()),
            },
        );
        (
            Snapshot::new(vec![r, outbox], vec![], vec!["".into()], 0),
            p,
        )
    }
    fn values(n: usize) -> ListValue {
        ListValue {
            values: vec![Value::default(); n],
        }
    }
    fn request() -> ReadRequest {
        ReadRequest {
            table: "uSeRs".into(),
            columns: vec!["email".into(), "NAME".into()],
            key_set: Some(KeySet {
                keys: vec![values(1)],
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    #[test]
    fn blind_key_read_and_delete() {
        let (s, p) = fixture();
        let r = request();
        let lowered = lower_read(&r, &s, &p).unwrap();
        assert_eq!(
            lowered.positions,
            vec![ValuePosition::Key { key: 0, part: 0 }]
        );
        assert!(matches!(
            lowered.stmt.uses[1].kind,
            UseKind::Equality {
                other: ValueSource::Param(1)
            }
        ));
        assert_eq!(
            lowered.stmt.outputs[0].source.as_ref().unwrap().table,
            "Users"
        );
        assert_eq!(check(&lowered.stmt, &p, &s).unwrap().inputs.len(), 1);
        let m = Mutation {
            operation: Some(mutation::Operation::Delete(mutation::Delete {
                table: r.table,
                key_set: r.key_set,
            })),
        };
        let lowered = lower_mutation(&m, &s, &p).unwrap();
        assert_eq!(
            lowered.positions,
            vec![ValuePosition::DeleteKey { key: 0, part: 0 }]
        );
        assert_eq!(check(&lowered.stmt, &p, &s).unwrap().inputs.len(), 1);
    }
    #[test]
    fn ranges_refuse_and_read_features_are_checked() {
        let (s, p) = fixture();
        let mut r = request();
        r.key_set = Some(KeySet {
            all: true,
            ranges: vec![googleapis_tonic_google_spanner_v1::google::spanner::v1::KeyRange {
                start_key_type: Some(key_range::StartKeyType::StartClosed(values(1))),
                end_key_type: Some(key_range::EndKeyType::EndOpen(values(0))),
            }],
            ..Default::default()
        });
        let lowered = lower_read(&r, &s, &p).unwrap();
        assert!(
            lowered
                .stmt
                .uses
                .iter()
                .any(|u| u.kind == UseKind::Comparison)
        );
        assert_eq!(
            check(&lowered.stmt, &p, &s).unwrap_err().code,
            NyCode::BlindOperation
        );
        r.index = "idx".into();
        assert_eq!(
            lower_read(&r, &s, &p).unwrap_err().code,
            NyCode::NotAnalysable
        );
        r.index.clear();
        r.partition_token = vec![1];
        assert_eq!(
            lower_read(&r, &s, &p).unwrap_err().code,
            NyCode::StatementNotAllowed
        );
        r.partition_token.clear();
        r.data_boost_enabled = true;
        assert_eq!(
            lower_read(&r, &s, &p).unwrap_err().code,
            NyCode::StatementNotAllowed
        );
    }
    #[test]
    fn writes_map_each_cell_and_refuse_bad_shapes() {
        let (s, p) = fixture();
        let write = mutation::Write {
            table: "outbox".into(),
            columns: vec!["email".into(), "name".into()],
            values: vec![values(2), values(2)],
        };
        for operation in [
            mutation::Operation::Insert(write.clone()),
            mutation::Operation::Update(write.clone()),
            mutation::Operation::InsertOrUpdate(write.clone()),
            mutation::Operation::Replace(write.clone()),
        ] {
            let lowered = lower_mutation(
                &Mutation {
                    operation: Some(operation),
                },
                &s,
                &p,
            )
            .unwrap();
            assert_eq!(
                lowered.positions,
                vec![
                    ValuePosition::MutationValue { row: 0, column: 0 },
                    ValuePosition::MutationValue { row: 0, column: 1 },
                    ValuePosition::MutationValue { row: 1, column: 0 },
                    ValuePosition::MutationValue { row: 1, column: 1 }
                ]
            );
            assert_eq!(lowered.stmt.plain_params, vec![2, 4]);
            assert_eq!(check(&lowered.stmt, &p, &s).unwrap().inputs.len(), 2);
        }
        let mut bad = write;
        bad.values = vec![values(1)];
        assert_eq!(
            lower_mutation(
                &Mutation {
                    operation: Some(mutation::Operation::Insert(bad))
                },
                &s,
                &p
            )
            .unwrap_err()
            .code,
            NyCode::NotAnalysable
        );
        assert!(lower_mutation(&Mutation::default(), &s, &p).is_err());
        let mut r = request();
        r.columns = vec!["missing".into()];
        assert!(lower_read(&r, &s, &p).is_err());
        r = request();
        r.table = "missing".into();
        assert!(lower_read(&r, &s, &p).is_err());
        r = request();
        r.key_set.as_mut().unwrap().keys = vec![values(3)];
        assert!(lower_read(&r, &s, &p).is_err());
        let plain = Policy {
            columns: Default::default(),
            ..p
        };
        let lowered = lower_read(&request(), &s, &plain).unwrap();
        assert_eq!(lowered.stmt.param_count, 0);
        assert!(check(&lowered.stmt, &plain, &s).is_ok());
    }
}
