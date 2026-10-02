mod accounts;
mod api;
mod app_state;
mod config;
mod delivery;
mod dlq;
mod domain;
mod events;
mod journal;
mod metrics;
mod partition;
mod ws;

use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("payment_rail=debug,tower_http=info").init();
    // NOTE: binds localhost only until 03-04 lands TLS+auth — do not expose publicly as-is.
    let cfg = config::Config::from_env().expect("invalid config");
    std::fs::create_dir_all(&cfg.journal_dir).ok();
    cfg.guard_partitions(std::path::Path::new(&cfg.journal_dir)).expect("partition guard");
    cfg.apply_env();
    tracing::info!("partitions: {} (PARTITIONS env)", cfg.partitions);
    let draining = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let w = app_state::spawn_writers(
        std::path::PathBuf::from(&cfg.journal_dir),
        cfg.partitions,
        draining.clone(),
        shutdown_tx.clone(),
    )
    .expect("journal open/replay");
    let supervisors = w.supervisors;
    let state = app_state::AppState {
        writers: w.slots,
        dlq: w.dlq,
        bcast: w.bcast,
        pending: w.pending.clone(),
        registry: w.registry,
        failed_tx: w.failed_bcast,
        store: w.store,
        events: w.events,
        draining: draining.clone(),
        ready: w.ready,
        shutdown_tx: shutdown_tx.clone(),
    };
    crate::ws::spawn_ack_sweeper(state.clone());
    let app = api::router(state);
    let addr: SocketAddr = cfg.listen_addr.parse().expect("LISTEN_ADDR");
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    let mut hook_rx = shutdown_tx.subscribe();
    // SIGTERM: stop intake, drain batches, final syncs, exit (deadline enforced).
    // The test hook (/test/shutdown) drives this same path via the watch channel.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = hook_rx.wait_for(|v| *v) => {},
            }
            tracing::info!("shutdown signal: draining");
            draining.store(true, Ordering::Relaxed);
            let _ = shutdown_tx.send(true);
        })
        .await
        .expect("serve");
    let deadline = Duration::from_secs(cfg.shutdown_deadline_secs);
    let ok = tokio::time::timeout(deadline, async {
        for h in supervisors {
            let _ = h.await;
        }
    })
    .await
    .is_ok();
    if ok {
        tracing::info!("clean shutdown: all partitions drained and synced");
        Ok(())
    } else {
        eprintln!("shutdown deadline exceeded: delivery may be incomplete");
        std::process::exit(1);
    }
}
