use std::{sync::Arc, time::Duration};

use crate::service::{Service, ServiceAdapter, ServiceName, ServiceSpec, ServiceStartError};

use super::types::{Runtime, RuntimeInner};

pub struct RuntimeBuilder {
    specs: Vec<ServiceSpec>,
    shutdown_timeout: Option<Duration>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self {
            specs: Vec::new(),
            shutdown_timeout: None,
        }
    }

    /// Configure shutdown timeout (how long we wait for tasks to stop after cancellation).
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.shutdown_timeout = Some(d);
        self
    }

    /// Add a service with explicit dependencies.
    ///
    /// This is intentionally explicit DI: you build your service with the dependencies it needs
    /// and pass it in.
    pub fn service<S>(mut self, service: Arc<S>, deps: &[ServiceName]) -> Result<Self, ServiceStartError>
    where
        S: Service,
    {
        let name = S::NAME;

        if self.specs.iter().any(|s| s.name == name) {
            return Err(ServiceStartError::Duplicate(name));
        }

        let deps = deps.to_vec();
        self.specs.push(ServiceSpec {
            name,
            deps,
            factory: Box::new(move || ServiceAdapter::new(service.clone())),
        });

        Ok(self)
    }

    /// Convenience for services with no dependencies.
    pub fn service0<S>(self, service: Arc<S>) -> Result<Self, ServiceStartError>
    where
        S: Service,
    {
        self.service(service, &[])
    }

    pub fn build(self) -> Runtime {
        Runtime {
            inner: Arc::new(RuntimeInner::new(self.shutdown_timeout)),
            specs: self.specs,
        }
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}