//! Authenticated HTTP and gRPC over shared, bounded persistent sessions.
mod body;
pub mod codec;
mod dispatch;
pub mod error;
mod grpc;
mod http;
mod identity;
pub mod json;
mod limits;
mod native;
mod open;
pub(crate) mod native_codec;
mod native_json;
pub mod net;
mod session;

pub mod proto {
    tonic::include_proto!("giggles.v1");
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("giggles_descriptor");
}

pub use dispatch::{ApiRuntime, CallOwnership, Completion, Encoding, ListenerKind, ResponseLease};
pub use identity::ConnectionIdentity;
pub use limits::ApiLimits;
