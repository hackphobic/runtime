use std::{sync::Arc, time::Instant};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::service::{DynServiceError, ServiceContext};

use super::{
    dag::topo_sort,
    types::{Runtime, RuntimeError, ShutdownReason},
};

pub(crate) async fn run_supervised(rt: Runtime) -> Result<(), RuntimeError> {
    let order = topo_sort(&rt.specs)?;

    let handle = rt.handle();
    let root_cancel = rt.cancellation_token();

    let mut joinset: JoinSet<Result<(), DynServiceError>> = JoinSet::new();

    // Start services in topo order.
    for &idx in &order {
        let spec = &rt.specs[idx];
        let service = (spec.factory)();

        let service_cancel = child_service_token(&root_cancel, spec.name);

        let ctx = ServiceContext {
            cancel: service_cancel,
            runtime: handle.clone(),
        };

        info!(service = spec.name, deps = ?spec.deps, "starting service");

        joinset.spawn(async move { service.run_boxed(ctx).await });
    }

    // Wait for either:
    // - shutdown requested
    // - any service fails
    //
    // If a service returns Err => cancel everything and bubble it up.
    // If all services finish Ok (unlikely) => treat as graceful exit.
    loop {
        tokio::select! {
            _ = handle.wait_shutdown_requested() => {
                info!(reason = ?handle.shutdown_reason(), "shutdown requested");
                break;
            }
            res = joinset.join_next() => {
                match res {
                    None => {
                        info!("all services exited");
                        break;
                    }
                    Some(join_res) => {
                        match join_res {
                            Ok(Ok(())) => {
                                // A service exited gracefully; we keep running unless shutdown already requested.
                                if handle.is_shutting_down() {
                                    // continue draining
                                } else {
                                    warn!("a service exited without error; runtime continues");
                                }
                            }
                            Ok(Err(e)) => {
                                warn!(service = e.service, error = %e, "service failed; shutting down runtime");
                                handle.request_shutdown(ShutdownReason::ServiceFailed);
                                // cancel already triggered, now drain and return error
                                drain_with_timeout(&mut joinset, rt.inner.shutdown_timeout).await?;
                                return Err(RuntimeError::ServiceFailed(e));
                            }
                            Err(join_err) => {
                                // Task panicked or was cancelled unexpectedly
                                warn!(error = %join_err, "service task join error; shutting down runtime");
                                handle.request_shutdown(ShutdownReason::ServiceFailed);
                                drain_with_timeout(&mut joinset, rt.inner.shutdown_timeout).await?;
                                // wrap join error
                                return Err(RuntimeError::ServiceFailed(DynServiceError{
                                    service: "<unknown>",
                                    source: Box::new(join_err),
                                }));
                            }
                        }
                    }
                }
            }
        }
    }

    // Graceful shutdown: cancel root, then drain.
    handle.request_shutdown(handle.shutdown_reason());
    drain_with_timeout(&mut joinset, rt.inner.shutdown_timeout).await?;
    Ok(())
}

fn child_service_token(root: &CancellationToken, _name: &'static str) -> CancellationToken {
    // Named tokens would be nice; tokio_util doesn't have naming, but we keep the structure.
    root.child_token()
}

async fn drain_with_timeout(
    joinset: &mut JoinSet<Result<(), DynServiceError>>,
    timeout: std::time::Duration,
) -> Result<(), RuntimeError> {
    let start = Instant::now();

    while start.elapsed() < timeout {
        match tokio::time::timeout(std::time::Duration::from_millis(200), joinset.join_next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => return Ok(()),
            Err(_) => {
                // keep waiting
            }
        }
    }

    Err(RuntimeError::ShutdownTimeout(timeout))
}