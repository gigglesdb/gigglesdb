//! Connection-local table for column-name and type-OID lists in native wire replies.

use crate::api::proto::{self, execute_reply::NativeResult};
use std::{collections::HashMap, fmt};

pub(crate) struct ShapeTable {
    by_id: Vec<(Vec<String>, Vec<u32>)>,
    ids: HashMap<(Vec<String>, Vec<u32>), u32>,
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
            let Some(field) = field.filter(|(columns, _)| !columns.is_empty()) else {
                continue;
            };
            let (names, types) = field;
            let mut columns = std::mem::take(names);
            let mut type_oids = types.map(std::mem::take).unwrap_or_default();
            let key = (columns.clone(), type_oids.clone());
            let shape_id = if let Some(&id) = self.ids.get(&key) {
                columns = Vec::new();
                type_oids = Vec::new();
                Some(id)
            } else if self.by_id.len() < self.cap
                && let Ok(id) = u32::try_from(self.by_id.len())
            {
                self.ids.insert(key.clone(), id);
                self.by_id.push(key);
                Some(id)
            } else {
                None
            };
            refs.push(proto::WireShapeRef {
                position: position as u32,
                shape_id,
                columns,
                type_oids,
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
type ColumnFields<'a> = [Option<(&'a mut Vec<String>, Option<&'a mut Vec<u32>>)>; 2];

fn column_fields(reply: &mut proto::ExecuteReply) -> ColumnFields<'_> {
    let sql = reply
        .sql
        .as_mut()
        .map(|sql| (&mut sql.columns, Some(&mut sql.column_types)));
    let operation = match &mut reply.native_result {
        Some(NativeResult::OperationOutcome(outcome)) => Some((&mut outcome.columns, None)),
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
    client_table: &mut Vec<(Vec<String>, Vec<u32>)>,
    reply: &mut proto::ExecuteReply,
    refs: &[proto::WireShapeRef],
) -> Result<(), ShapeError> {
    let mut fields = column_fields(reply);
    let checkpoint = client_table.len();
    let result = refs.iter().try_for_each(|shape| {
        if !shape.type_oids.is_empty()
            && (shape.type_oids.len() != shape.columns.len() || shape.position != 0)
        {
            return Err(ShapeError::InvalidPosition(shape.position));
        }
        let (columns, types) = fields
            .get_mut(shape.position as usize)
            .and_then(Option::take)
            .filter(|(columns, types)| {
                columns.is_empty() && types.as_ref().is_none_or(|t| t.is_empty())
            })
            .ok_or(ShapeError::InvalidPosition(shape.position))?;
        let restored = match shape.shape_id {
            None => (shape.columns.clone(), shape.type_oids.clone()),
            Some(id) if shape.columns.is_empty() => {
                let value = client_table
                    .get(id as usize)
                    .ok_or(ShapeError::UnknownId(id))?;
                if shape.position == 1 && !value.1.is_empty() {
                    return Err(ShapeError::InvalidPosition(shape.position));
                }
                value.clone()
            }
            Some(id) if id as usize == client_table.len() => {
                let value = (shape.columns.clone(), shape.type_oids.clone());
                client_table.push(value.clone());
                value
            }
            Some(id) => return Err(ShapeError::UnexpectedDefinition(id)),
        };
        *columns = restored.0;
        if let Some(types) = types {
            *types = restored.1;
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

    fn typed(oid: u32) -> ExecuteReply {
        let mut reply = sql(&["id"]);
        reply.sql.as_mut().unwrap().column_types = vec![oid];
        reply
    }

    #[test]
    fn types_distinguish_shapes() {
        let mut table = ShapeTable::new(256);
        assert_eq!(table.compress(&mut typed(23))[0].shape_id, Some(0));
        assert_eq!(table.compress(&mut typed(25))[0].shape_id, Some(1));
        assert_eq!(table.compress(&mut typed(23))[0].shape_id, Some(0));
    }

    #[test]
    fn definition_carries_types_reference_carries_none() {
        let mut table = ShapeTable::new(256);
        let mut reply = typed(23);
        let refs = table.compress(&mut reply);
        assert_eq!(refs[0].columns, ["id"]);
        assert_eq!(refs[0].type_oids, [23]);
        assert!(reply.sql.unwrap().column_types.is_empty());
        let refs = table.compress(&mut typed(23));
        assert!(refs[0].columns.is_empty());
        assert!(refs[0].type_oids.is_empty());
    }

    #[test]
    fn cap_inline_carries_types() {
        let mut table = ShapeTable::new(1);
        table.compress(&mut typed(23));
        let mut reply = typed(25);
        let refs = table.compress(&mut reply);
        assert_eq!(refs[0].shape_id, None);
        assert_eq!(refs[0].type_oids, [25]);
        expand(&mut Vec::new(), &mut reply, &refs).unwrap();
        assert_eq!(reply, typed(25));
    }

    #[test]
    fn expand_restores_column_types() {
        let mut table = ShapeTable::new(256);
        let mut client = Vec::new();
        for _ in 0..2 {
            let mut reply = typed(23);
            let refs = table.compress(&mut reply);
            expand(&mut client, &mut reply, &refs).unwrap();
            assert_eq!(reply, typed(23));
        }
    }

    #[test]
    fn expand_rejects_type_length_mismatch() {
        let refs = [WireShapeRef {
            columns: vec!["id".into()],
            type_oids: vec![23, 25],
            ..Default::default()
        }];
        assert!(expand(&mut Vec::new(), &mut sql(&[]), &refs).is_err());
    }

    #[test]
    fn expand_rejects_types_at_native_position() {
        let mut reply = ExecuteReply {
            native_result: Some(NativeResult::OperationOutcome(NativeOutcome::default())),
            ..Default::default()
        };
        let refs = [WireShapeRef {
            position: 1,
            columns: vec!["id".into()],
            type_oids: vec![23],
            ..Default::default()
        }];
        assert!(expand(&mut Vec::new(), &mut reply, &refs).is_err());
    }

    #[test]
    fn expand_rejects_types_on_reference() {
        let refs = [WireShapeRef {
            shape_id: Some(0),
            type_oids: vec![23],
            ..Default::default()
        }];
        let mut client = vec![(vec!["id".into()], vec![23])];
        assert!(expand(&mut client, &mut sql(&[]), &refs).is_err());
    }

    #[test]
    fn expand_rejects_nonempty_destinations_and_rolls_back() {
        for mut reply in [typed(23), {
            let mut r = sql(&[]);
            r.sql.as_mut().unwrap().column_types = vec![23];
            r
        }] {
            let refs = [WireShapeRef {
                columns: vec!["id".into()],
                type_oids: vec![23],
                shape_id: Some(0),
                ..Default::default()
            }];
            let mut client = Vec::new();
            assert!(expand(&mut client, &mut reply, &refs).is_err());
            assert!(client.is_empty());
        }
        let mut client = Vec::new();
        let refs = [
            WireShapeRef {
                columns: vec!["id".into()],
                type_oids: vec![23],
                shape_id: Some(0),
                ..Default::default()
            },
            WireShapeRef {
                position: 1,
                type_oids: vec![23],
                ..Default::default()
            },
        ];
        assert!(expand(&mut client, &mut sql(&[]), &refs).is_err());
        assert!(client.is_empty());
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
                type_oids: vec![],
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
                type_oids: vec![],
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
                type_oids: vec![],
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
                    type_oids: vec![],
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
                    type_oids: vec![],
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
            type_oids: vec![],
        }];
        assert!(expand(&mut client, &mut reply, &refs).is_err());
    }

    #[test]
    fn expand_rejects_out_of_sequence_definitions() {
        let mut client = vec![(vec!["id".into()], vec![])];
        let mut reply = sql(&[]);
        for id in [0, 2] {
            let refs = [WireShapeRef {
                position: 0,
                shape_id: Some(id),
                columns: vec!["name".into()],
                type_oids: vec![],
            }];
            assert!(expand(&mut client, &mut reply, &refs).is_err());
            assert_eq!(client, [(vec!["id".to_owned()], vec![])]);
        }
    }
}
