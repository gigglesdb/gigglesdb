//! Pure, finite prepared task grammar. No authority or storage access here.
use crate::Request;
use giggles_core::{
    ApprovalId, Column, Key, OperationId, RowId, Selection, Status, TenantId,
    relational::{ColumnDef, DataType, Description, Value},
};
type R<T> = Result<T, &'static str>;

#[derive(Clone, Copy)]
pub enum TaskStatement {
    Status,
    Key { execute: bool },
    Range { execute: bool },
}
impl TaskStatement {
    pub fn parse(sql: &str) -> R<Self> {
        if sql.len() > 4096 {
            return Err("54000");
        }
        // Tokenize punctuation, retaining every other byte. No stripping comments,
        // case folding, literal interpolation or accepting a partial parse.
        // Explicit punctuation splitting avoids treating adjacent words as one.
        let mut words = Vec::new();
        let mut start = 0;
        for (i, c) in sql.char_indices() {
            if c.is_ascii_whitespace() || matches!(c, '(' | ')' | ',') {
                if start < i {
                    words.push(&sql[start..i]);
                }
                if !c.is_ascii_whitespace() {
                    words.push(&sql[i..i + 1]);
                }
                start = i + c.len_utf8();
            }
        }
        if start < sql.len() {
            words.push(&sql[start..]);
        }
        let (plan, prefix) = match words.as_slice() {
            ["GIGGLES", "STATUS", ..] => (Self::Status, 2),
            ["GIGGLES", "PREVIEW", "KEY", ..] => (Self::Key { execute: false }, 3),
            ["GIGGLES", "EXECUTE", "KEY", ..] => (Self::Key { execute: true }, 3),
            ["GIGGLES", "PREVIEW", "RANGE", ..] => (Self::Range { execute: false }, 3),
            ["GIGGLES", "EXECUTE", "RANGE", ..] => (Self::Range { execute: true }, 3),
            _ => return Err("42601"),
        };
        let n = plan.description().parameters.len();
        let mut expected = vec!["(".to_owned()];
        for i in 1..=n {
            if i > 1 {
                expected.push(",".into());
            }
            expected.push(format!("${i}"));
        }
        expected.push(")".into());
        if words[prefix..] != expected {
            return Err("42601");
        }
        Ok(plan)
    }
    pub fn description(self) -> Description {
        use DataType::*;
        let parameters = match self {
            Self::Status => vec![Bytes],
            Self::Key { execute } => {
                let mut p = vec![Bytes, Bytes, Bytes, Text];
                if execute {
                    p.push(Bytes);
                }
                p
            }
            Self::Range { execute } => {
                let mut p = vec![Bytes, Bytes, Text, BigInt, BigInt, Text];
                if execute {
                    p.push(Bytes);
                }
                p
            }
        };
        Description {
            parameters,
            columns: vec![ColumnDef {
                id: 0,
                name: "decision".into(),
                data_type: Text,
                not_null: true,
            }],
        }
    }
    pub fn bind(self, p: &[Value]) -> R<Request> {
        if p.len() != self.description().parameters.len() {
            return Err("08P01");
        }
        fn id(v: &Value) -> R<u64> {
            match v {
                Value::Bytes(b) => Ok(u64::from_be_bytes(
                    b.as_slice().try_into().map_err(|_| "22023")?,
                )),
                Value::Null => Err("23502"),
                _ => Err("42804"),
            }
        }
        fn status(v: &Value) -> R<Status> {
            match v {
                Value::Text(s) => match s.as_str() {
                    "failed" => Ok(Status::Failed),
                    "queued" => Ok(Status::Queued),
                    "succeeded" => Ok(Status::Succeeded),
                    _ => Err("22023"),
                },
                Value::Null => Err("23502"),
                _ => Err("42804"),
            }
        }
        fn bound(v: &Value) -> R<u64> {
            match v {
                Value::Int(n) => u64::try_from(*n).map_err(|_| "22003"),
                Value::Null => Err("23502"),
                _ => Err("42804"),
            }
        }
        let operation = OperationId(id(&p[0])?);
        if matches!(self, Self::Status) {
            return Ok(Request::Status { operation });
        }
        let tenant = TenantId(id(&p[1])?);
        let (selection, after, execute) = match self {
            Self::Key { execute } => (
                Selection::Keys(vec![Key {
                    tenant,
                    row: RowId(id(&p[2])?),
                }]),
                status(&p[3])?,
                execute,
            ),
            Self::Range { execute } => {
                let from = bound(&p[3])?;
                let until = bound(&p[4])?;
                if from >= until {
                    return Err("22023");
                }
                (
                    Selection::Predicate {
                        tenant,
                        status: status(&p[2])?,
                        from,
                        until,
                    },
                    status(&p[5])?,
                    execute,
                )
            }
            Self::Status => unreachable!(),
        };
        let request = giggles_core::Request {
            operation,
            selection,
            column: Column::Status,
            after,
        };
        if execute {
            let approval = match p.last().unwrap() {
                Value::Null => None,
                v => Some(ApprovalId(id(v)?)),
            };
            Ok(Request::Execute { request, approval })
        } else {
            Ok(Request::Preview { request })
        }
    }
}
