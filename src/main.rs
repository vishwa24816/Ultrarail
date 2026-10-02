mod api;
mod app_state;
mod domain;
mod journal;
mod metrics;

use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("payment_rail=debug,tower_http=info").init();
    // NOTE: binds localhost only; authN lands in Phase 5 — do not expose publicly as-is.
    let dir = std::env::var("JOURNAL_DIR").unwrap_or_else(|_| "./data".into());
    std::fs::create_dir_all(&dir).ok();
    let tx = app_state::spawn_writer(std::path::PathBuf::from(dir).join("journal.wal"))
        .expect("journal open/replay");
    let app = api::router(app_state::AppState { tx });
    let addr: SocketAddr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into()).parse().expect("LISTEN_ADDR");
    tracing::info!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
    Ok(())
}
