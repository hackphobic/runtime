# Example
```rust
use std::{sync::Arc, time::Duration};

use runtime::{RuntimeBuilder, Service, ServiceContext, Topic, TopicConfig};
use tokio::sync::broadcast;
use tracing::{info, warn};

#[derive(Debug, Clone)]
struct Tick(u64);

#[derive(thiserror::Error, Debug)]
enum TickerError {
    #[error("publish failed")]
    PublishFailed,
}

struct Ticker {
    ticks: Topic<Tick>,
}

impl Service for Ticker {
    const NAME: &'static str = "ticker";
    type Error = TickerError;

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let mut n = 0u64;
            loop {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => {
                        info!("ticker: cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {
                        n += 1;
                        self.ticks.publish(Tick(n)).map_err(|_| TickerError::PublishFailed)?;
                    }
                }
            }
            Ok(())
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum PrinterError {
    #[error("topic recv error: {0}")]
    Recv(#[from] broadcast::error::RecvError),
}

struct Printer {
    rx: broadcast::Receiver<Arc<Tick>>,
}

impl Service for Printer {
    const NAME: &'static str = "printer";
    type Error = PrinterError;

    fn run(
        self: Arc<Self>,
        ctx: ServiceContext,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            let mut rx = self.rx.resubscribe();
            loop {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => {
                        info!("printer: cancelled");
                        break;
                    }
                    msg = rx.recv() => {
                        let tick = msg?;
                        info!(tick = tick.0, "printer: got tick");
                        if tick.0 >= 10 {
                            warn!("printer: requesting shutdown");
                            ctx.runtime.request_shutdown(runtime::ShutdownReason::Requested);
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), runtime::RuntimeError> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let ticks = Topic::<Tick>::new(TopicConfig { capacity: 64 });

    let ticker = Arc::new(Ticker { ticks: ticks.clone() });
    let printer = Arc::new(Printer { rx: ticks.subscribe() });

    let rt = RuntimeBuilder::new()
        .service0(ticker)?
        .service(printer, &["ticker"])? // printer depends on ticker
        .shutdown_timeout(Duration::from_secs(3))
        .build();

    rt.run().await
}
```