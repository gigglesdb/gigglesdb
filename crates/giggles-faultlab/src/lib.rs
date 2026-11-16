//! Bounded local TCP fault proxy. Synthetic domains are not cloud qualification.
//!
//! ```no_run
//! use giggles_faultlab::proxy::{Proxy, ProxyLimits, Direction, Fault};
//! # #[tokio::main]
//! # async fn main() -> giggles_faultlab::Result<()> {
//! let proxy = Proxy::start("127.0.0.1:9000".parse()?, ProxyLimits::default()).await?;
//! proxy.set(Direction::Downstream, Fault::Partition)?;
//! proxy.set(Direction::Downstream, Fault::Pass)?;
//! let trace = proxy.stop().await;
//! assert!(!trace.overflow);
//! # Ok(())
//! # }
//! ```
pub mod process;
pub mod proxy;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
