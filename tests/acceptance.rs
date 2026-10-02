//! Phase 1 success criteria, end to end over HTTP.

use std::path::PathBuf;

// Reuse the binary crate? Integration tests can't see it — drive over HTTP.
async fn spawn_app() -> (String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let _path: PathBuf = dir.path().join("journal.wal");
    // Build via the binary: launch `cargo run` equivalent in-process is not
    // possible from integration tests, so boot the pieces through a helper
    // binary target. Simplest reliable path: start the compiled debug binary.
    let bin = env!("CARGO_BIN_EXE_payment-rail");
    let port = portpicker_free();
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env("JOURNAL_DIR", dir.path())
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .kill_on_drop(true);
    let mut child = cmd.spawn().expect("spawn server");
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if client.get(format!("{base}/health")).send().await.map(|r| r.status().is_success()).unwrap_or(false) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // Leak the child for the test duration; kill_on_drop cleans up.
    std::mem::forget(child);
    (base, dir)
}

fn portpicker_free() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn valid_body() -> serde_json::Value {
    serde_json::json!({
        "idempotency_scope": "payments",
        "debit_account": "user:1",
        "credit_account": "merchant:9",
        "amount": 250,
        "currency": "USD"
    })
}

#[tokio::test]
async fn valid_payment_accepted_durable() {
    let (base, _dir) = spawn_app().await;
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "acc-1")
        .json(&valid_body())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["status"], "ACCEPTED_DURABLE");
    assert!(body["tx_id"].is_string());
}

#[tokio::test]
async fn unbalanced_rejected_with_no_settlement() {
    let (base, _dir) = spawn_app().await;
    let client = reqwest::Client::new();
    // amount <= 0 is invalid
    let mut bad = valid_body();
    bad["amount"] = serde_json::json!(-5);
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "rej-1")
        .json(&bad)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 422);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["status"], "REJECTED");
}

#[tokio::test]
async fn invalid_request_rejected() {
    let (base, _dir) = spawn_app().await;
    let client = reqwest::Client::new();
    // missing Idempotency-Key header
    let res = client.post(format!("{base}/payments")).json(&valid_body()).send().await.unwrap();
    assert_eq!(res.status(), 400);
    // unknown currency variant
    let mut bad = valid_body();
    bad["currency"] = serde_json::json!("XYZ");
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "rej-2")
        .json(&bad)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 422);
}
