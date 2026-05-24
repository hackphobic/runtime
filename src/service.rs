use std::{error::Error, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

/// Type alias for service names (kept simple & explicit).
pub type ServiceName = &'static str;

/// The context each service receives.
#[derive(Clone)]
pub struct ServiceContext {
    /// Root cancellation token for this service (child of runtime token).
    pub cancel: CancellationToken,

    /// Runtime handle (for querying shutdown, requesting shutdown, etc.).
    pub runtime: crate::runtime::RuntimeHandle,
}

impl ServiceContext {
    /// Convenience: await cancellation.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }

    /// Create a child cancellation token, useful for subtasks.
    pub fn child_cancel(&self) -> CancellationToken {
        self.cancel.child_token()
    }
}

/// A long-running unit of work.
///
/// Services should generally:
/// - loop on input
/// - `tokio::select!` with `ctx.cancel.cancelled()`
/// - return `Ok(())` on graceful exit
pub trait Service: Send + Sync + 'static {
    /// A stable, human-readable name used in the dependency graph.
    const NAME: ServiceName;

    /// Per-service typed error.
    type Error: Error + Send + Sync + 'static;

    /// Main execution function.
    ///
    /// The runtime calls this once; returning ends the service.
    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

/// Specification used by the runtime builder.
pub struct ServiceSpec {
    pub name: ServiceName,
    pub deps: Vec<ServiceName>,
    pub factory: Box<dyn Fn() -> Arc<dyn DynService> + Send + Sync>,
}

impl fmt::Debug for ServiceSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceSpec")
            .field("name", &self.name)
            .field("deps", &self.deps)
            .finish_non_exhaustive()
    }
}

/// Error returned when a service fails to start (e.g. factory panics, etc.).
#[derive(thiserror::Error, Debug)]
pub enum ServiceStartError {
    #[error("service factory panicked for {0}")]
    FactoryPanicked(ServiceName),

    #[error("duplicate service name: {0}")]
    Duplicate(ServiceName),
}

/// Internal trait for erasing heterogeneous services.
pub trait DynService: Send + Sync + 'static {
    fn name(&self) -> ServiceName;

    fn run_boxed(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> futures::future::BoxFuture<'static, Result<(), DynServiceError>>;
}

/// Runtime boundary error type: keep source + service name.
#[derive(Debug)]
pub struct DynServiceError {
    pub service: ServiceName,
    pub source: Box<dyn Error + Send + Sync + 'static>,
}

impl fmt::Display for DynServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "service {} failed: {}", self.service, self.source)
    }
}

impl Error for DynServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*self.source)
    }
}

/// Adapter from typed `Service` into erased `DynService`.
pub struct ServiceAdapter<S: Service>(Arc<S>);

impl<S: Service> ServiceAdapter<S> {
    pub fn new(service: Arc<S>) -> Arc<dyn DynService> {
        Arc::new(Self(service))
    }
}

impl<S: Service> DynService for ServiceAdapter<S> {
    fn name(&self) -> ServiceName {
        S::NAME
    }

    fn run_boxed(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> futures::future::BoxFuture<'static, Result<(), DynServiceError>> {
        let inner = self.0.clone();
        Box::pin(async move {
            inner.run(ctx).await.map_err(|e| DynServiceError {
                service: S::NAME,
                source: Box::new(e),
            })
        })
    }
}