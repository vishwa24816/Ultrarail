//! Delivery guarantees: UNKNOWN reconciles without duplicate submits.

async fn spawn_with(dir: &std::path::Path, extra: &[(&str, &str)]) -> (String, String, tokio::process::Child) {
    let bin = env!("CARGO_BIN_EXE_payment-rail");
    let port: u16 = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let prom: u16 = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env("JOURNAL_DIR", dir)
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("PROM_ADDR", format!("127.0.0.1:{prom}"))
        .env("PARTITIONS", "2")
        .kill_on_drop(true);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn server");
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for _ in 0..200 {
        if client.get(format!("{base}/ready")).send().await.map(|r| r.status().is_success()).unwrap_or(false) {
            let prom_base = format!("http://127.0.0.1:{prom}");
            return (base, prom_base, child);
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
    let (base, _prom, _child) =
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

#[tokio::test]
async fn audit_trail_covers_lifecycle_and_labels_stay_bounded() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, prom_base, _child) = spawn_with(dir.path(), &[]).await;
    // 100 unique keys: label cardinality must stay bounded.
    for i in 0..100 {
        let body = serde_json::json!({
            "idempotency_scope": "audit",
            "debit_account": format!("user:{i}"),
            "credit_account": "m:9",
            "amount": 10,
            "currency": "USD",
        });
        let res = client
            .post(format!("{base}/payments"))
            .header("Idempotency-Key", format!("audit-{i}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201);
    }
    // One tx through settle, then check its audit trail is ordered.
    let body = serde_json::json!({
        "idempotency_scope": "audit", "debit_account": "user:1",
        "credit_account": "m:9", "amount": 10, "currency": "USD",
    });
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "audit-one")
        .json(&body)
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = res.json().await.unwrap();
    let tx_id = v["tx_id"].as_str().unwrap().to_string();
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let s: serde_json::Value =
            client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
        if s["delivery_state"] == "Settled" {
            break;
        }
    }
    let a: serde_json::Value =
        client.get(format!("{base}/audit?tx_id={tx_id}")).send().await.unwrap().json().await.unwrap();
    let trail = a.as_array().cloned().unwrap_or_default();
    assert!(trail.len() >= 2, "expected accept + settle entries, got {trail:?}");
    let mut last = 0u64;
    for e in &trail {
        let t = e["at_ms"].as_u64().unwrap_or(0);
        assert!(t >= last, "audit trail out of order: {trail:?}");
        last = t;
    }
    // Metrics exported, labels bounded (no tx_id/key/account in labels).
    let m = client.get(format!("{prom_base}/metrics")).send().await.unwrap().text().await.unwrap();
    assert!(m.contains("payments_accepted_total"), "exporter must serve counters");
    assert!(!m.contains("audit-"), "tx ids must never appear as label values");
}
