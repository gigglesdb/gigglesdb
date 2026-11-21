//! Connection-local table for column-name lists in native wire replies.

use crate::api::proto::{self, execute_reply::NativeResult};
use std::{collections::HashMap, fmt};

pub(crate) struct ShapeTable {
    by_id: Vec<Vec<String>>,
    ids: HashMap<Vec<String>, u32>,
    cap: usize,
}

impl ShapeTable {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            by_id: Vec::new(),
            ids: HashMap::new(),
            cap,
        }
    }

    pub(super) fn checkpoint(&self) -> usize {
        self.by_id.len()
    }

    pub(super) fn rollback(&mut self, checkpoint: usize) {
        for columns in self.by_id.drain(checkpoint..) {
            self.ids.remove(&columns);
        }
    }

    /// Takes every nonempty reply column-name list in traversal order.
    pub(crate) fn compress(&mut self, reply: &mut proto::ExecuteReply) -> Vec<proto::WireShapeRef> {
        let mut refs = Vec::new();
        for (position, field) in column_fields(reply).into_iter().enumerate() {
            let Some(field) = field.filter(|columns| !columns.is_empty()) else {
                continue;
            };
            let mut columns = std::mem::take(field);
            let shape_id = if let Some(&id) = self.ids.get(&columns) {
                columns = Vec::new();
                Some(id)
            } else if self.by_id.len() < self.cap
                && let Ok(id) = u32::try_from(self.by_id.len())
            {
                self.ids.insert(columns.clone(), id);
                self.by_id.push(columns.clone());
                Some(id)
            } else {
                None
            };
            refs.push(proto::WireShapeRef {
                position: position as u32,
                shape_id,
                columns,
            });
        }
        refs
    }
}

// giggles.proto's repeated string columns fields, in source order:
// SqlResult.columns (reply, position 0), NativeInsertMany.columns (request),
// NativeOutcome.columns (reply, position 1), NativeUniqueConstraint.columns,
// NativeForeignKey.columns, NativeCreateIndex.columns (request), and
// WireShapeRef.columns (wire envelope).
// Transaction emits carry repeated NativeColumn messages with typed metadata.
// Absent and empty fields keep their positions, and emit no shape reference.
fn column_fields(reply: &mut proto::ExecuteReply) -> [Option<&mut Vec<String>>; 2] {
    let sql = reply.sql.as_mut().map(|sql| &mut sql.columns);
    let operation = match &mut reply.native_result {
        Some(NativeResult::OperationOutcome(outcome)) => Some(&mut outcome.columns),
        _ => None,
    };
    [sql, operation]
}

#[derive(Debug)]
pub(crate) enum ShapeError {
    InvalidPosition(u32),
    UnknownId(u32),
    UnexpectedDefinition(u32),
}

impl fmt::Display for ShapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPosition(position) => write!(f, "invalid wire shape position {position}"),
            Self::UnknownId(id) => write!(f, "unknown wire shape ID {id}"),
            Self::UnexpectedDefinition(id) => {
                write!(f, "out-of-sequence wire shape definition {id}")
            }
        }
    }
}

impl std::error::Error for ShapeError {}

pub(crate) fn expand(
    client_table: &mut Vec<Vec<String>>,
    reply: &mut proto::ExecuteReply,
    refs: &[proto::WireShapeRef],
) -> Result<(), ShapeError> {
    let mut fields = column_fields(reply);
    let checkpoint = client_table.len();
    let result = refs.iter().try_for_each(|shape| {
        let field = fields
            .get_mut(shape.position as usize)
            .and_then(Option::take)
            .filter(|columns| columns.is_empty())
            .ok_or(ShapeError::InvalidPosition(shape.position))?;

        match shape.shape_id {
            None => field.clone_from(&shape.columns),
            Some(id) if shape.columns.is_empty() => {
                let columns = client_table
                    .get(id as usize)
                    .ok_or(ShapeError::UnknownId(id))?;
                field.clone_from(columns);
            }
            Some(id) if id as usize == client_table.len() => {
                field.clone_from(&shape.columns);
                client_table.push(shape.columns.clone());
            }
            Some(id) => return Err(ShapeError::UnexpectedDefinition(id)),
        }
        Ok(())
    });
    if result.is_err() {
        client_table.truncate(checkpoint);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{
        ExecuteReply, NativeColumn, NativeEmittedRow, NativeEmittedRows, NativeEmittedValue,
        NativeOutcome, NativeTransactionOutcome, SqlResult, WireShapeRef,
        execute_reply::NativeResult, native_emitted_value::Value,
    };

    fn sql(columns: &[&str]) -> ExecuteReply {
        ExecuteReply {
            sql: Some(SqlResult {
                columns: columns.iter().map(|name| (*name).into()).collect(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn first_reply_carries_columns_then_id_only() {
        let mut table = ShapeTable::new(256);
        let original = sql(&["id", "name"]);
        let mut first = original.clone();
        assert_eq!(
            table.compress(&mut first),
            [WireShapeRef {
                position: 0,
                shape_id: Some(0),
                columns: vec!["id".into(), "name".into()],
            }]
        );
        assert!(first.sql.unwrap().columns.is_empty());

        let mut second = original;
        assert_eq!(
            table.compress(&mut second),
            [WireShapeRef {
                position: 0,
                shape_id: Some(0),
                columns: vec![],
            }]
        );
        assert!(second.sql.unwrap().columns.is_empty());
    }

    #[test]
    fn changed_columns_get_new_id() {
        let mut table = ShapeTable::new(256);
        table.compress(&mut sql(&["id", "name"]));
        assert_eq!(
            table.compress(&mut sql(&["id", "name", "email"])),
            [WireShapeRef {
                position: 0,
                shape_id: Some(1),
                columns: vec!["id".into(), "name".into(), "email".into()],
            }]
        );
    }

    #[test]
    fn ids_never_reused() {
        let mut table = ShapeTable::new(256);
        let mut client = Vec::new();
        for i in 0..300 {
            let name = format!("column_{i}");
            let original = sql(&[&name]);
            let mut reply = original.clone();
            let refs = table.compress(&mut reply);
            assert_eq!(
                refs,
                [WireShapeRef {
                    position: 0,
                    shape_id: (i < 256).then_some(i),
                    columns: vec![name],
                }]
            );
            expand(&mut client, &mut reply, &refs).unwrap();
            assert_eq!(reply, original);
        }
        assert_eq!(client.len(), 256);
        for (name, id, columns) in [
            ("column_0", Some(0), vec![]),
            ("column_299", None, vec!["column_299".into()]),
        ] {
            assert_eq!(
                table.compress(&mut sql(&[name])),
                [WireShapeRef {
                    position: 0,
                    shape_id: id,
                    columns,
                }]
            );
        }
    }

    #[test]
    fn expand_restores_exact_reply() {
        let operation = ExecuteReply {
            native_result: Some(NativeResult::OperationOutcome(NativeOutcome {
                columns: vec!["id".into()],
                matched: 2,
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut both = sql(&["id"]);
        both.native_result = operation.native_result.clone();
        let column = NativeColumn {
            id: 7,
            name: "email".into(),
            data_type: proto::NativeDataType::Text as i32,
            not_null: true,
        };
        let transaction = ExecuteReply {
            native_result: Some(NativeResult::TransactionOutcome(NativeTransactionOutcome {
                emitted: vec![
                    NativeEmittedValue {
                        value: Some(Value::Row(NativeEmittedRow {
                            columns: vec![column.clone()],
                            ..Default::default()
                        })),
                    },
                    NativeEmittedValue {
                        value: Some(Value::Rows(NativeEmittedRows {
                            columns: vec![column],
                            complete: true,
                            ..Default::default()
                        })),
                    },
                ],
                ..Default::default()
            })),
            ..Default::default()
        };

        let mut server = ShapeTable::new(256);
        let mut client = Vec::new();
        for (original, positions) in [
            (sql(&["id", "name"]), vec![0]),
            (sql(&[]), vec![]),
            (operation, vec![1]),
            (both.clone(), vec![0, 1]),
            (both, vec![0, 1]),
            (transaction, vec![]),
        ] {
            let mut reply = original.clone();
            let refs = server.compress(&mut reply);
            assert_eq!(
                refs.iter().map(|shape| shape.position).collect::<Vec<_>>(),
                positions
            );
            expand(&mut client, &mut reply, &refs).unwrap();
            assert_eq!(reply, original);
        }
    }

    #[test]
    fn expand_unknown_id_is_error() {
        let mut client = Vec::new();
        let mut reply = sql(&[]);
        let refs = [WireShapeRef {
            position: 0,
            shape_id: Some(0),
            columns: vec![],
        }];
        assert!(expand(&mut client, &mut reply, &refs).is_err());
    }

    #[test]
    fn expand_rejects_out_of_sequence_definitions() {
        let mut client = vec![vec!["id".into()]];
        let mut reply = sql(&[]);
        for id in [0, 2] {
            let refs = [WireShapeRef {
                position: 0,
                shape_id: Some(id),
                columns: vec!["name".into()],
            }];
            assert!(expand(&mut client, &mut reply, &refs).is_err());
            assert_eq!(client, [vec!["id".to_owned()]]);
        }
    }
}
