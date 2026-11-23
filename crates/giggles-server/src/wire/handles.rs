//! Connection-local, catalog-free native operation and SQL handles.

use crate::api::{
    codec::{self, Invocation},
    error::ApiError,
    proto,
};
use giggles_language::{Bindings, PreparedText};
use std::sync::Arc;

enum Slot {
    Native(PreparedText),
    Sql(Arc<str>),
}

pub(super) struct HandleTable {
    slots: Vec<Option<Slot>>,
    free: Vec<u32>,
    cap: usize,
}

impl HandleTable {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            cap,
        }
    }

    pub(super) fn prepare(
        &mut self,
        text: &str,
        language_version: Option<&str>,
    ) -> Result<u32, ApiError> {
        self.check_capacity()?;
        let input = proto::ExecuteRequest {
            request: Some(proto::execute_request::Request::Text(
                proto::NativeTextRequest {
                    text: text.into(),
                    language_version: language_version.map(str::to_owned),
                    bindings: vec![],
                },
            )),
            ..Default::default()
        };
        codec::validate_execute(&input)?;
        let version = codec::decode_language_version(language_version)?;
        if codec::text_task_statement(text, version).is_some() {
            return Err(ApiError::InvalidArgument);
        }
        let prepared =
            giggles_language::prepare_text(text, version).map_err(|_| ApiError::InvalidArgument)?;

        self.insert(Slot::Native(prepared))
    }

    pub(super) fn prepare_sql(
        &mut self,
        text: &str,
        language_version: Option<&str>,
    ) -> Result<u32, ApiError> {
        if language_version.is_some() || text.trim().is_empty() {
            return Err(ApiError::InvalidArgument);
        }
        self.check_capacity()?;
        self.insert(Slot::Sql(Arc::from(text)))
    }

    fn check_capacity(&self) -> Result<(), ApiError> {
        if self.free.is_empty() && self.slots.len() >= self.cap {
            Err(ApiError::ResourceExhausted)
        } else {
            Ok(())
        }
    }

    fn insert(&mut self, slot: Slot) -> Result<u32, ApiError> {
        // Number new slots from zero and reuse the most recently closed slot.
        if let Some(handle) = self.free.pop() {
            self.slots[handle as usize] = Some(slot);
            Ok(handle)
        } else {
            let handle =
                u32::try_from(self.slots.len()).map_err(|_| ApiError::ResourceExhausted)?;
            self.slots.push(Some(slot));
            Ok(handle)
        }
    }

    pub(super) fn invocation(
        &self,
        handle: u32,
        bindings: Bindings,
        params: Vec<proto::Value>,
    ) -> Result<Invocation, ApiError> {
        let slot = self
            .slots
            .get(handle as usize)
            .and_then(Option::as_ref)
            .ok_or(ApiError::InvalidArgument)?;
        match slot {
            Slot::Native(prepared) => {
                if !params.is_empty() {
                    return Err(ApiError::InvalidArgument);
                }
                prepared
                    .lower(&bindings)
                    .map(Invocation::from)
                    .map_err(|_| ApiError::InvalidArgument)
            }
            Slot::Sql(text) => {
                if !bindings.is_empty() {
                    return Err(ApiError::InvalidArgument);
                }
                codec::decode_execute(proto::ExecuteRequest {
                    request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
                        sql: text.to_string(),
                        params,
                    })),
                    ..Default::default()
                })
                .map(|decoded| decoded.request)
            }
        }
    }

    pub(super) fn close(&mut self, handle: u32) -> bool {
        if self
            .slots
            .get_mut(handle as usize)
            .and_then(Option::take)
            .is_none()
        {
            return false;
        }
        self.free.push(handle);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_prepare_shares_cap_and_free_list() {
        let mut handles = HandleTable::new(3);
        assert_eq!(handles.prepare("t where id=1", None).unwrap(), 0);
        assert_eq!(handles.prepare_sql("SELECT 1", None).unwrap(), 1);
        assert_eq!(handles.prepare("t where id=2", None).unwrap(), 2);
        assert!(matches!(
            handles.prepare_sql("SELECT 2", None),
            Err(ApiError::ResourceExhausted)
        ));
        assert!(matches!(
            handles.prepare("t where id=3", None),
            Err(ApiError::ResourceExhausted)
        ));
        assert!(handles.close(0));
        assert!(handles.close(1));
        assert_eq!(handles.prepare("t where id=3", None).unwrap(), 1);
        assert_eq!(handles.prepare_sql("SELECT 2", None).unwrap(), 0);
        assert!(matches!(
            handles.prepare_sql("SELECT 3", None),
            Err(ApiError::ResourceExhausted)
        ));
    }

    #[test]
    fn sql_prepare_refuses_language_version() {
        let mut handles = HandleTable::new(1);
        for version in ["", "0.18", "unknown"] {
            assert!(matches!(
                handles.prepare_sql("SELECT 1", Some(version)),
                Err(ApiError::InvalidArgument)
            ));
        }
        assert_eq!(handles.prepare_sql("SELECT 1", None).unwrap(), 0);
    }

    #[test]
    fn sql_prepare_refuses_blank_text() {
        let mut handles = HandleTable::new(1);
        for text in ["", " \t\r\n", "\u{2003}"] {
            assert!(matches!(
                handles.prepare_sql(text, None),
                Err(ApiError::InvalidArgument)
            ));
        }
        assert_eq!(handles.prepare_sql("SELECT 1", None).unwrap(), 0);
    }

    #[test]
    fn run_kind_mismatch_is_invalid_argument() {
        let mut handles = HandleTable::new(2);
        let sql = handles.prepare_sql("SELECT 1", None).unwrap();
        let native = handles.prepare("t where id=1", None).unwrap();
        let bindings = crate::api::native_codec::bindings(vec![proto::NativeBinding {
            name: Some("id".into()),
            value: Some(proto::Value {
                value: Some(proto::value::Value::IntValue(7)),
            }),
        }])
        .unwrap();
        assert!(matches!(
            handles.invocation(sql, bindings, vec![]),
            Err(ApiError::InvalidArgument)
        ));
        assert!(matches!(
            handles.invocation(
                native,
                Default::default(),
                vec![proto::Value {
                    value: Some(proto::value::Value::IntValue(7)),
                }]
            ),
            Err(ApiError::InvalidArgument)
        ));
    }

    #[test]
    fn sql_invocation_equals_execute_sql() {
        for (text, params) in [
            ("SELECT 1", vec![]),
            (
                "SELECT $1",
                vec![proto::Value {
                    value: Some(proto::value::Value::IntValue(7)),
                }],
            ),
            ("SELEC 1", vec![]),
        ] {
            let expected = codec::decode_execute(proto::ExecuteRequest {
                request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
                    sql: text.into(),
                    params: params.clone(),
                })),
                ..Default::default()
            })
            .unwrap();
            let mut handles = HandleTable::new(1);
            let handle = handles.prepare_sql(text, None).unwrap();
            let actual = handles
                .invocation(handle, Default::default(), params)
                .unwrap();
            assert_eq!(
                format!("{actual:?}"),
                format!("{:?}", expected.request),
                "{text}"
            );
            // QueuedRun::resolve applies this same operation-ID validation.
            let id = codec::decode_operation_id(Some("sql-run-1")).unwrap();
            assert!(matches!(
                codec::validate_operation_id(&actual, id.as_ref()),
                Err(ApiError::InvalidArgument)
            ));
            // Malformed parameter values must use EXECUTE's validation as well.
            assert!(matches!(
                handles.invocation(
                    handle,
                    Default::default(),
                    vec![proto::Value { value: None }]
                ),
                Err(ApiError::InvalidArgument)
            ));
        }
    }

    #[test]
    fn prepare_refuses_task_ddl() {
        let text = "revoke task \"inc-436\"";
        assert!(
            giggles_language::parse_task_with_version(
                text,
                giggles_language::LanguageVersion::Embedded022
            )
            .is_ok()
        );
        assert!(matches!(
            HandleTable::new(1).prepare(text, Some("0.22")),
            Err(ApiError::InvalidArgument)
        ));
    }

    #[test]
    fn cap_enforced_without_eviction() {
        let mut handles = HandleTable::new(256);
        for slot in 0..256 {
            assert_eq!(handles.prepare("t where id=1", None).unwrap(), slot);
        }
        assert!(matches!(
            handles.prepare("t where id=2", None),
            Err(ApiError::ResourceExhausted)
        ));
        assert!(handles.invocation(5, Default::default(), vec![]).is_ok());
        assert!(handles.close(5));
        assert!(!handles.close(5));
        assert!(matches!(
            handles.invocation(5, Default::default(), vec![]),
            Err(ApiError::InvalidArgument)
        ));
        assert_eq!(handles.prepare("t where id=2", None).unwrap(), 5);
        assert!(handles.invocation(255, Default::default(), vec![]).is_ok());
    }

    #[test]
    fn invocation_equals_lowered_text() {
        for text in [
            "t where id=$id select id",
            "insert t set id=$id",
            "update t where id=$id set body=\"new\"",
            "delete t where id=$id",
            "transaction { insert t set id=$id; delete t where id=$id }",
        ] {
            let version = Some("0.18");
            let bindings = vec![proto::NativeBinding {
                name: Some("id".into()),
                value: Some(proto::Value {
                    value: Some(proto::value::Value::IntValue(7)),
                }),
            }];
            let expected = codec::decode_execute(proto::ExecuteRequest {
                request: Some(proto::execute_request::Request::Text(
                    proto::NativeTextRequest {
                        text: text.into(),
                        language_version: version.map(str::to_owned),
                        bindings: bindings.clone(),
                    },
                )),
                ..Default::default()
            })
            .unwrap();
            let mut handles = HandleTable::new(1);
            let handle = handles.prepare(text, version).unwrap();
            let bindings = crate::api::native_codec::bindings(bindings).unwrap();
            let actual = handles.invocation(handle, bindings, vec![]).unwrap();
            assert_eq!(
                format!("{actual:?}"),
                format!("{:?}", expected.request),
                "{text}"
            );
        }
    }
}
