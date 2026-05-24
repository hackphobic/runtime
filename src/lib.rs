//! bee-runtime (modernized)
//!
//! Core ideas:
//! - Structured concurrency via JoinSet
//! - Cancellation via CancellationToken
//! - Deterministic startup/shutdown using a dependency DAG
//! - Typed events via Topic<E> (tokio broadcast)
//! - Services with typed errors (erased at runtime boundary)

pub mod event;
pub mod service;
pub mod runtime;

mod util;

pub use event::{Topic, TopicClosed, TopicConfig};
pub use runtime::{Runtime, RuntimeBuilder, RuntimeError, RuntimeHandle, ShutdownReason};
pub use service::{Service, ServiceContext, ServiceName, ServiceSpec, ServiceStartError};