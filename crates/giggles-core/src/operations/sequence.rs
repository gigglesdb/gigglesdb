//! Native sequence intents use the SQL DDL executor and durable callbacks.
use super::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceCall {
    Nextval(String),
    Currval(String),
    Setval { name: String, value: i64, is_called: Option<bool> },
}
impl Engine {
    pub fn execute_sequence_catalog(&self, request: &CatalogRequest) -> Result<u64> {
        use crate::relational::defaults::SequenceDdl;
        let (name, ddl) = match request {
            CatalogRequest::CreateSequence { name } => (name, SequenceDdl::create_native(name.clone())),
            CatalogRequest::DropSequence { name, cascade } => (name, SequenceDdl::drop_native(name.clone(), *cascade)),
            _ => return Err(Error::Unsupported),
        };
        self.admin.sql_session().native_scope(false, || {
            if !identifier(name) { return Err(Error::Bind("identifier".into())); }
            validate_size(request)
        }, |session| session.sequence_catalog_engine(ddl).map_err(core))
    }
    /// Each engine call has a fresh session, including its currval state.
    pub fn execute_sequence(&self, call: &SequenceCall) -> Result<i64> {
        self.admin.sql_session().execute_sequence_native(call)
    }
}
impl SqlSession {
    pub fn execute_sequence_native(&mut self, call: &SequenceCall) -> Result<i64> {
        let (operation, name, value, called) = match call {
            SequenceCall::Nextval(name) => ("nextval", name, None, true),
            SequenceCall::Currval(name) => ("currval", name, None, true),
            SequenceCall::Setval { name, value, is_called } => ("setval", name, Some(*value), is_called.unwrap_or(true)),
        };
        self.native_scope(false, || {
            if !identifier(name) { return Err(Error::Bind("identifier".into())); }
            Ok(())
        }, |session| session.sequence_call_engine(operation, name, value, called).map_err(core))
    }
}
