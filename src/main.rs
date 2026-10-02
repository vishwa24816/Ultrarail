mod accounts;
mod api;
mod app_state;
mod audit;
mod auth;
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

extern crate metrics as metrics_core;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicit crypto provider: the tree contains both ring and aws-lc,
    // so rustls cannot auto-pick one.
    let _ = rustls::crypto::ring::default_provider().install_default();
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
        audit: w.audit,
    };
    crate::ws::spawn_ack_sweeper(state.clone());
    // Prometheus exporter on its own port (localhost by default).
    let prom_addr: SocketAddr = cfg.prom_addr.parse().expect("PROM_ADDR");
    tokio::spawn(async move {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        if metrics_core::set_global_recorder(recorder).is_err() {
            return;
        }
        let app = axum::Router::new().route(
            "/metrics",
            axum::routing::get(move || {
                let h = handle.clone();
                async move { h.render() }
            }),
        );
        let listener = match tokio::net::TcpListener::bind(prom_addr).await {
            Ok(l) => l,
            Err(_) => return,
        };
        let _ = axum::serve(listener, app).await;
    });
    let app = api::router(state.clone());
    let addr: SocketAddr = cfg.listen_addr.parse().expect("LISTEN_ADDR");
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    let mut hook_rx = shutdown_tx.subscribe();
    // SIGTERM: stop intake, drain batches, final syncs, exit (deadline enforced).
    // The test hook (/test/shutdown) drives this same path via the watch channel.
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = hook_rx.wait_for(|v| *v) => {},
        }
        tracing::info!("shutdown signal: draining");
        draining.store(true, Ordering::Relaxed);
        let _ = shutdown_tx.send(true);
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
    });
    let use_tls = std::env::var("PAYMENT_TLS_CERT").ok().or_else(|| std::env::var("TLS_CERT").ok());
    let use_key = std::env::var("PAYMENT_TLS_KEY").ok().or_else(|| std::env::var("TLS_KEY").ok());
    match (use_tls, use_key) {
        (Some(cert), Some(key)) => {
            tracing::info!("TLS on {addr}");
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                .await
                .expect("TLS cert/key");
            axum_server::bind_rustls(addr, tls).handle(handle).serve(app.into_make_service()).await.expect("serve");
        }
        _ => {
            if std::env::var("PAYMENT_TLS_OFF").as_deref() == Ok("true")
                || std::env::var("TLS_OFF").as_deref() == Ok("true")
            {
                tracing::warn!("TLS_OFF=true: plaintext sandbox listener on {addr}");
            } else {
                tracing::warn!("no TLS cert configured: plaintext listener on {addr} (sandbox only, do not expose)");
            }
            tracing::info!("listening on {addr}");
            axum_server::bind(addr).handle(handle).serve(app.into_make_service()).await.expect("serve");
        }
    }
    let deadline = Duration::from_secs(cfg.shutdown_deadline_secs);
    let ok = tokio::time::timeout(deadline, async {
        for h in supervisors {
            let _ = h.await;
        }
    })
    .await
    .is_ok();
    // Final barriers for best-effort stores (audit + DLQ coalesce syncs).
    if let Ok(mut a) = state.audit.lock() {
        a.sync();
    }
    if let Ok(mut d) = state.dlq.lock() {
        d.sync_all();
    }
    if ok {
        tracing::info!("clean shutdown: all partitions drained and synced");
        Ok(())
    } else {
        eprintln!("shutdown deadline exceeded: delivery may be incomplete");
        std::process::exit(1);
    }
}
