//! Delivery guarantees: UNKNOWN reconciles without duplicate submits.

async fn spawn_with(dir: &std::path::Path, extra: &[(&str, &str)]) -> (String, tokio::process::Child) {
    let bin = env!("CARGO_BIN_EXE_payment-rail");
    let port: u16 = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env("JOURNAL_DIR", dir)
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("PARTITIONS", "2")
        .kill_on_drop(true);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn server");
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for _ in 0..200 {
        if client.get(format!("{base}/health")).send().await.map(|r| r.status().is_success()).unwrap_or(false) {
            return (base, child);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let _ = child.kill().await;
    panic!("server never became healthy");
}

#[tokio::test]
async fn timeout_goes_unknown_then_reconciles_without_resubmit() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, _child) =
        spawn_with(dir.path(), &[("SIM_TIMEOUT_RATE", "1.0"), ("RAIL_TIMEOUT_MS", "500")]).await;
    let body = serde_json::json!({
        "idempotency_scope": "storm",
        "debit_account": "user:1",
        "credit_account": "m:9",
        "amount": 100,
        "currency": "USD",
    });
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "storm-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let v: serde_json::Value = res.json().await.unwrap();
    let tx_id = v["tx_id"].as_str().unwrap().to_string();
    // Poll until reconciled; then confirm exactly ONE unknown event (no resubmit storm).
    let mut settled = false;
    let mut unknowns = 0;
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let s: serde_json::Value =
            client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
        let evs = s["events"].as_array().cloned().unwrap_or_default();
        unknowns = evs.iter().filter(|e| e["kind"] == "unknown").count();
        if s["delivery_state"] == "Reconciled" {
            settled = true;
            // One more sweep to be sure no resubmit happened after reconcile.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let s2: serde_json::Value =
                client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
            let unknowns2 =
                s2["events"].as_array().map(|a| a.iter().filter(|e| e["kind"] == "unknown").count()).unwrap_or(0);
            assert_eq!(unknowns2, unknowns, "resubmit after reconcile detected");
            break;
        }
    }
    assert!(settled, "tx never reconciled");
    assert_eq!(unknowns, 1, "exactly one submit attempt expected, got {unknowns} unknowns");
}
