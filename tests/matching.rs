//! Matching guarantees: single settlement under duplicate storms,
//! visible exceptions for mismatches, guard survives restarts.

use futures::{SinkExt, StreamExt};

async fn spawn(dir: &std::path::Path) -> (String, tokio::process::Child) {
    spawn_with(dir, &[]).await
}

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
        if client.get(format!("{base}/ready")).send().await.map(|r| r.status().is_success()).unwrap_or(false) {
            return (base, child);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let _ = child.kill().await;
    panic!("server never became ready");
}

async fn submit(client: &reqwest::Client, base: &str, key: &str, debit: &str, amount: i64) -> String {
    let body = serde_json::json!({
        "idempotency_scope": "matching",
        "debit_account": debit,
        "credit_account": "merchant:9",
        "amount": amount,
        "currency": "USD",
    });
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", key)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let v: serde_json::Value = res.json().await.unwrap();
    v["tx_id"].as_str().unwrap().to_string()
}

async fn wait_matched(client: &reqwest::Client, base: &str, tx_id: &str) {
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let s: serde_json::Value =
            client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
        if s["matched"] == true {
            return;
        }
    }
    panic!("tx {tx_id} never matched");
}

async fn settle_events(client: &reqwest::Client, base: &str, tx_id: &str) -> usize {
    let s: serde_json::Value =
        client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
    // Only the WRITER's settle event counts: delivery logs its own Settled event
    // per ack ("routed to matcher"), while the guard + true settlement live in
    // the writer ("matcher confident"). Double-settle means 2+ of the latter.
    s["events"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|e| e["kind"] == "settled" && e["detail"] == "matcher confident")
                .count()
        })
        .unwrap_or(0)
}

async fn bank_conn(base: &str) -> (
    futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,
    futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
) {
    let url = base.replace("http", "ws") + "/ws/bank?bank_id=user";
    let req = http::Request::builder()
        .uri(&url)
        .header("host", "127.0.0.1")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap();
    let (s, _) = tokio_tungstenite::connect_async(req).await.expect("bank ws");
    use futures::StreamExt as _;
    let (w, r) = s.split();
    (w, r)
}

#[tokio::test]
async fn duplicate_storm_settles_exactly_once() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, _child) = spawn(dir.path()).await;
    let tx_id = submit(&client, &base, "storm-1", "user:1", 100).await;
    wait_matched(&client, &base, &tx_id).await;
    // Exact day for the confirm: read back the tx timestamp.
    let s: serde_json::Value =
        client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
    let day = s["tx"]["timestamp_ms"].as_u64().unwrap() / 86_400_000;
    let (mut w, _r) = bank_conn(&base).await;
    // Storm: 20 duplicate confirmations, some concurrent via a second socket.
    let (mut w2, _r2) = bank_conn(&base).await;
    for i in 0..10 {
        let mk = |amount| {
            serde_json::json!({
                "type": "tx.confirmed", "tx_id": tx_id,
                "rail_ref": format!("rail-storm-{i}"), "amount": amount,
                "currency": "USD", "counterparty": "merchant:9",
                "value_date": day.to_string(),
            })
            .to_string()
        };
        w.send(tokio_tungstenite::tungstenite::Message::Text(mk(100).into())).await.unwrap();
        w2.send(tokio_tungstenite::tungstenite::Message::Text(mk(100).into())).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(settle_events(&client, &base, &tx_id).await, 1, "double settlement under storm");
    let a: serde_json::Value = client
        .get(format!("{base}/audit?tx_id={tx_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        a.as_array().map(|x| x.iter().any(|e| e["reason"] == "duplicate-confirmation")).unwrap_or(false),
        "duplicates must be audited: {a}"
    );
}

#[tokio::test]
async fn mismatch_routes_to_exception_queue() {
    // Sim never settles (always transient) so the bank confirm is the only
    // match attempt — deterministic near-miss, no race.
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, _child) = spawn_with(dir.path(), &[("SIM_FAIL_RATE", "1.0")]).await;
    let tx_id = submit(&client, &base, "mm-2", "user:3", 200).await;
    let s: serde_json::Value =
        client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
    let day = s["tx"]["timestamp_ms"].as_u64().unwrap() / 86_400_000;
    let (mut w, _r) = bank_conn(&base).await;
    // Wrong amount AND wrong day: ambiguous, must not settle.
    for (amount, vdate) in [(199, day.to_string()), (200, "0".to_string())] {
        let bad = serde_json::json!({
            "type": "tx.confirmed", "tx_id": tx_id,
            "rail_ref": "rail-mm", "amount": amount,
            "currency": "USD", "counterparty": "merchant:9",
            "value_date": vdate,
        });
        w.send(tokio_tungstenite::tungstenite::Message::Text(bad.to_string().into())).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // Not settled...
    assert_eq!(settle_events(&client, &base, &tx_id).await, 0, "near-miss must not settle");
    // ...but visible with reasons.
    let dlq: serde_json::Value =
        client.get(format!("{base}/dlq?limit=100")).send().await.unwrap().json().await.unwrap();
    let amb: Vec<_> = dlq
        .as_array()
        .map(|a| a.iter().filter(|e| e["reason"] == "ambiguous-match").collect())
        .unwrap_or_default();
    assert_eq!(amb.len(), 2, "both near-misses visible: {dlq}");
    // Unknown-id confirm lands exactly once.
    let ghost = serde_json::json!({
        "type": "tx.confirmed", "tx_id": "no-such-tx",
        "rail_ref": "rail-x", "amount": 1,
        "currency": "USD", "counterparty": "m:9",
        "value_date": "0",
    });
    w.send(tokio_tungstenite::tungstenite::Message::Text(ghost.to_string().into())).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let dlq: serde_json::Value =
        client.get(format!("{base}/dlq?limit=100")).send().await.unwrap().json().await.unwrap();
    let unmatched: Vec<_> = dlq
        .as_array()
        .map(|a| a.iter().filter(|e| e["reason"] == "unmatched-confirmation").collect())
        .unwrap_or_default();
    assert_eq!(unmatched.len(), 1, "exactly one DLQ entry for unknown confirm: {dlq}");
}

#[tokio::test]
async fn restart_keeps_single_settlement() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, mut child) = spawn(dir.path()).await;
    let tx_id = submit(&client, &base, "rs-1", "user:4", 100).await;
    wait_matched(&client, &base, &tx_id).await;
    child.kill().await.unwrap();
    let _ = child.wait().await;
    let (base2, _c2) = spawn(dir.path()).await;
    // Duplicate after restart must still stop at the rebuilt guard.
    let s: serde_json::Value =
        client.get(format!("{base2}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
    let day = s["tx"]["timestamp_ms"].as_u64().unwrap() / 86_400_000;
    let (mut w, _r) = bank_conn(&base2).await;
    for i in 0..5 {
        let m = serde_json::json!({
            "type": "tx.confirmed", "tx_id": tx_id,
            "rail_ref": format!("rail-rs-{i}"), "amount": 100,
            "currency": "USD", "counterparty": "merchant:9",
            "value_date": day.to_string(),
        });
        w.send(tokio_tungstenite::tungstenite::Message::Text(m.to_string().into())).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(settle_events(&client, &base2, &tx_id).await, 1, "double settle after restart");
}
