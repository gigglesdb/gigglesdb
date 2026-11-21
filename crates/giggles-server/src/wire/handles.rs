//! Connection-local, catalog-free native operation handles.

use crate::api::{
    codec::{self, Invocation},
    error::ApiError,
    proto,
};
use giggles_language::{Bindings, PreparedText};

pub(super) struct HandleTable {
    slots: Vec<Option<PreparedText>>,
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
        if self.free.is_empty() && self.slots.len() >= self.cap {
            return Err(ApiError::ResourceExhausted);
        }
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

        // Number new slots from zero and reuse the most recently closed slot.
        if let Some(handle) = self.free.pop() {
            self.slots[handle as usize] = Some(prepared);
            Ok(handle)
        } else {
            let handle =
                u32::try_from(self.slots.len()).map_err(|_| ApiError::ResourceExhausted)?;
            self.slots.push(Some(prepared));
            Ok(handle)
        }
    }

    pub(super) fn invocation(
        &self,
        handle: u32,
        bindings: Bindings,
    ) -> Result<Invocation, ApiError> {
        self.slots
            .get(handle as usize)
            .and_then(Option::as_ref)
            .ok_or(ApiError::InvalidArgument)?
            .lower(&bindings)
            .map(Invocation::from)
            .map_err(|_| ApiError::InvalidArgument)
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
        assert!(handles.invocation(5, Default::default()).is_ok());
        assert!(handles.close(5));
        assert!(!handles.close(5));
        assert!(matches!(
            handles.invocation(5, Default::default()),
            Err(ApiError::InvalidArgument)
        ));
        assert_eq!(handles.prepare("t where id=2", None).unwrap(), 5);
        assert!(handles.invocation(255, Default::default()).is_ok());
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
            let actual = handles.invocation(handle, bindings).unwrap();
            assert_eq!(
                format!("{actual:?}"),
                format!("{:?}", expected.request),
                "{text}"
            );
        }
    }
}
