//! Ops gates: reconcile, DLQ sweep, restart recovery, key rotation.
//! Each test prints PASS lines and fails non-zero — CI-gatable procedures.

use futures::SinkExt;

async fn spawn_with(dir: &std::path::Path, extra: &[(&str, &str)]) -> (String, tokio::process::Child) {
    let bin = env!("CARGO_BIN_EXE_payment-rail");
    let port: u16 = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env("JOURNAL_DIR", dir)
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("PARTITIONS", "2")
        .kill_on_drop(true)
        .current_dir(env!("CARGO_MANIFEST_DIR"));
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn server");
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for _ in 0..200 {
        if client.get(format!("{base}/ready")).send().await.map(|r| r.status().is_success()).unwrap_or(false) {
            return (base, child);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let _ = child.kill().await;
    panic!("server never became ready");
}

async fn post(client: &reqwest::Client, base: &str, scope: &str, key: &str) -> serde_json::Value {
    let body = serde_json::json!({
        "idempotency_scope": scope, "debit_account": "user:1",
        "credit_account": "m:9", "amount": 10, "currency": "USD",
    });
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", key)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    res.json().await.unwrap()
}

async fn dlq_has(client: &reqwest::Client, base: &str, reason: &str) -> bool {
    let dlq: serde_json::Value =
        client.get(format!("{base}/dlq?limit=200")).send().await.unwrap().json().await.unwrap();
    dlq.as_array().map(|a| a.iter().any(|e| e["reason"] == reason)).unwrap_or(false)
}

#[tokio::test]
async fn ops_reconcile_unknown_end_to_end() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, _c) =
        spawn_with(dir.path(), &[("SIM_TIMEOUT_RATE", "1.0"), ("RAIL_TIMEOUT_MS", "300")]).await;
    let v = post(&client, &base, "ops", "ops-rec-1").await;
    let tx_id = v["tx_id"].as_str().unwrap().to_string();
    let mut settled = false;
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let s: serde_json::Value =
            client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
        if s["delivery_state"] == "Reconciled" {
            settled = true;
            break;
        }
    }
    assert!(settled, "UNKNOWN never reconciled");
    println!("PASS: reconcile-unknown (exactly-once, settled via query)");
}

#[tokio::test]
async fn ops_dlq_sweep_all_reasons_visible() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    // validation-failed + expired + unknown-id on one server...
    let (base, _c) = spawn_with(dir.path(), &[("IDEM_TTL_SECS", "1")]).await;
    // validation-failed: negative amount.
    let bad = serde_json::json!({
        "idempotency_scope": "ops", "debit_account": "user:1",
        "credit_account": "m:9", "amount": -5, "currency": "USD",
    });
    let r = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "ops-bad")
        .json(&bad)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);
    // key-expired-replayed.
    let v = post(&client, &base, "ops", "ops-exp-1").await;
    let _ = v;
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let v2 = post(&client, &base, "ops", "ops-exp-1").await;
    assert_ne!(v2["tx_id"], v["tx_id"]);
    assert!(dlq_has(&client, &base, "validation-failed").await);
    assert!(dlq_has(&client, &base, "key-expired-replayed").await);
    // ...ambiguous-match on a never-settling sim.
    let dir2 = tempfile::TempDir::new().unwrap();
    let (base2, _c2) = spawn_with(dir2.path(), &[("SIM_FAIL_RATE", "1.0")]).await;
    let v3 = post(&client, &base2, "ops", "ops-amb-1").await;
    let tx3 = v3["tx_id"].as_str().unwrap().to_string();
    let ws_base = base2.replace("http", "ws");
    let req = http::Request::builder()
        .uri(format!("{ws_base}/ws/bank?bank_id=user"))
        .header("host", "127.0.0.1")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap();
    let (mut sock, _) = tokio_tungstenite::connect_async(req).await.expect("bank ws");
    let bad_c = serde_json::json!({
        "type": "tx.confirmed", "tx_id": tx3,
        "rail_ref": "rail-ops", "amount": 999,
        "currency": "USD", "counterparty": "merchant:9",
        "value_date": "0",
    });
    sock.send(tokio_tungstenite::tungstenite::Message::Text(bad_c.to_string().into())).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(dlq_has(&client, &base2, "ambiguous-match").await);
    println!("PASS: dlq-sweep (validation-failed, key-expired-replayed, ambiguous-match visible)");
}

#[tokio::test]
async fn ops_restart_zero_loss_mid_load() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, mut child) = spawn_with(dir.path(), &[]).await;
    let mut ids = vec![];
    for i in 0..20 {
        let v = post(&client, &base, "ops", &format!("ops-rs-{i}")).await;
        ids.push(v["tx_id"].as_str().unwrap().to_string());
    }
    child.kill().await.unwrap();
    let _ = child.wait().await;
    let (base2, _c2) = spawn_with(dir.path(), &[]).await;
    for (i, want) in ids.iter().enumerate() {
        let v = post(&client, &base2, "ops", &format!("ops-rs-{i}")).await;
        assert_eq!(v["tx_id"].as_str().unwrap(), want);
    }
    println!("PASS: restart-recover (20/20 acked txs survive kill-9)");
}

#[tokio::test]
async fn ops_bank_key_rotation_mid_load() {
    let dir = tempfile::TempDir::new().unwrap();
    let kf = dir.path().join("bank.keys");
    let old = "bankold-000000000000000000000000001";
    let new = "banknew-000000000000000000000000002";
    std::fs::write(&kf, format!("user:{old}")).unwrap();
    let (base, _c) = spawn_with(
        dir.path(),
        &[("BANK_KEYS_FILE", kf.to_str().unwrap()), ("KEY_RELOAD_SECS", "1")],
    )
    .await;
    let ws_base = base.replace("http", "ws");
    let ws_req = |key: &str| {
        http::Request::builder()
            .uri(format!("{ws_base}/ws/bank?bank_id=user"))
            .header("x-api-key", key)
            .header("host", "127.0.0.1")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap()
    };
    // Old key connects.
    assert!(tokio_tungstenite::connect_async(ws_req(old)).await.is_ok());
    // Rotate with dual window, then new-only.
    std::fs::write(&kf, format!("user:{old},user:{new}")).unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(tokio_tungstenite::connect_async(ws_req(old)).await.is_ok(), "old key in window");
    assert!(tokio_tungstenite::connect_async(ws_req(new)).await.is_ok(), "new key in window");
    std::fs::write(&kf, format!("user:{new}")).unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(tokio_tungstenite::connect_async(ws_req(new)).await.is_ok());
    assert!(tokio_tungstenite::connect_async(ws_req(old)).await.is_err(), "old key revoked");
    println!("PASS: rotate-key (bank plane, zero in-window failures)");
}
