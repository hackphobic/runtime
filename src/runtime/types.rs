use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::util::DEFAULT_SHUTDOWN_TIMEOUT;

#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeInner>,
}

impl RuntimeHandle {
    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::Relaxed)
    }

    /// Request shutdown for the whole runtime.
    pub fn request_shutdown(&self, reason: ShutdownReason) {
        self.inner.set_shutdown_reason(reason);
        self.inner.cancel.cancel();
        self.inner.notify.notify_waiters();
    }

    pub fn shutdown_reason(&self) -> ShutdownReason {
        self.inner.reason.load()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancel.clone()
    }

    pub async fn wait_shutdown_requested(&self) {
        if self.is_shutting_down() {
            return;
        }
        self.inner.notify.notified().await;
    }
}

/// Shutdown reason is intentionally small/stable.
/// If you want richer typed reasons, you can extend this to store Arc<dyn Any>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReason {
    Requested,
    ServiceFailed,
    Signal,
}

impl ShutdownReason {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            ShutdownReason::Requested => 1,
            ShutdownReason::ServiceFailed => 2,
            ShutdownReason::Signal => 3,
        }
    }
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            2 => ShutdownReason::ServiceFailed,
            3 => ShutdownReason::Signal,
            _ => ShutdownReason::Requested,
        }
    }
}

/// Errors from `Runtime::run()`.
#[derive(thiserror::Error, Debug)]
pub enum RuntimeError {
    #[error("dependency graph error: {0}")]
    Dag(#[from] crate::runtime::dag::DagError),

    #[error("a service failed: {0}")]
    ServiceFailed(#[from] crate::service::DynServiceError),

    #[error("shutdown timed out after {0:?}")]
    ShutdownTimeout(std::time::Duration),
}

pub struct Runtime {
    pub(crate) inner: Arc<RuntimeInner>,
    pub(crate) specs: Vec<crate::service::ServiceSpec>,
}

impl Runtime {
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle {
            inner: self.inner.clone(),
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancel.clone()
    }

    /// Run until shutdown is requested or a service fails.
    pub async fn run(self) -> Result<(), RuntimeError> {
        crate::runtime::supervisor::run_supervised(self).await
    }

    /// Like `run()` but installs a Ctrl-C listener.
    pub async fn run_with_ctrl_c(self) -> Result<(), RuntimeError> {
        let handle = self.handle();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                handle.request_shutdown(ShutdownReason::Signal);
            }
        });
        self.run().await
    }
}

pub(crate) struct RuntimeInner {
    pub(crate) cancel: CancellationToken,
    pub(crate) notify: Notify,
    pub(crate) shutting_down: AtomicBool,
    pub(crate) reason: ShutdownReasonAtomic,
    pub(crate) shutdown_timeout: std::time::Duration,
}

impl RuntimeInner {
    pub(crate) fn new(shutdown_timeout: Option<std::time::Duration>) -> Self {
        Self {
            cancel: CancellationToken::new(),
            notify: Notify::new(),
            shutting_down: AtomicBool::new(false),
            reason: ShutdownReasonAtomic::new(ShutdownReason::Requested),
            shutdown_timeout: shutdown_timeout.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT),
        }
    }

    pub(crate) fn set_shutdown_reason(&self, reason: ShutdownReason) {
        self.shutting_down.store(true, Ordering::Relaxed);
        self.reason.store(reason);
    }
}

/// Small atomic wrapper for ShutdownReason.
pub(crate) struct ShutdownReasonAtomic(std::sync::atomic::AtomicU8);

impl ShutdownReasonAtomic {
    pub(crate) fn new(r: ShutdownReason) -> Self {
        Self(std::sync::atomic::AtomicU8::new(r.as_u8()))
    }
    pub(crate) fn load(&self) -> ShutdownReason {
        ShutdownReason::from_u8(self.0.load(Ordering::Relaxed))
    }
    pub(crate) fn store(&self, r: ShutdownReason) {
        self.0.store(r.as_u8(), Ordering::Relaxed);
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("specs_len", &self.specs.len())
            .finish()
    }
}