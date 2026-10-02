//! Phase 2 success criteria: kill-9 loses nothing acked; torn tails are discarded.

use std::path::PathBuf;

async fn spawn_on(dir: &std::path::Path) -> (String, tokio::process::Child) {
    spawn_on_with(dir, &[]).await
}

async fn spawn_on_with(dir: &std::path::Path, extra_env: &[(&str, &str)]) -> (String, tokio::process::Child) {
    let bin = env!("CARGO_BIN_EXE_payment-rail");
    let port: u16 = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env("JOURNAL_DIR", dir)
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("PARTITIONS", "4")
        .kill_on_drop(true);
    for (k, v) in extra_env {
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

async fn post(client: &reqwest::Client, base: &str, key: &str, i: u64) -> String {
    let body = serde_json::json!({
        "idempotency_scope": "recovery",
        "debit_account": format!("user:{}", i % 10),
        "credit_account": format!("merchant:{}", i % 3),
        "amount": 100 + i as i64,
        "currency": "USD",
    });
    let res = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", key)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "key={key}");
    let v: serde_json::Value = res.json().await.unwrap();
    v["tx_id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn kill9_loses_nothing_acked() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let mut ids = Vec::new();
    let (base, mut child) = spawn_on(dir.path()).await;
    for i in 0..50 {
        ids.push(post(&client, &base, &format!("k9-{i}"), i).await);
    }
    child.kill().await.expect("SIGKILL equivalent");
    let _ = child.wait().await;
    // Respawn on the SAME journal dir: every acked tx must still resolve to the same id.
    let (base2, _child2) = spawn_on(dir.path()).await;
    for (i, want) in ids.iter().enumerate() {
        let got = post(&client, &base2, &format!("k9-{i}"), i as u64).await;
        assert_eq!(&got, want, "tx id changed across kill-9 for k9-{i}");
    }
}

#[tokio::test]
async fn torn_tail_discarded() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, mut child) = spawn_on(dir.path()).await;
    let first = post(&client, &base, "torn-1", 7).await;
    child.kill().await.expect("kill");
    let _ = child.wait().await;
    // Scribble a torn tail onto partition files by hand (crash mid-append).
    for i in 0..4 {
        let p: PathBuf = dir.path().join(format!("journal-{i}.wal"));
        if p.exists() {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(b"\x09\x00\x00\x00half a record").unwrap();
            f.flush().unwrap();
        }
    }
    let (base2, _child2) = spawn_on(dir.path()).await;
    let got = post(&client, &base2, "torn-1", 7).await;
    assert_eq!(got, first, "good record lost after torn tail");
}

#[tokio::test]
async fn ttl_expiry_returns_fresh_and_dlq_logs() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, _child) = spawn_on_with(dir.path(), &[("IDEM_TTL_SECS", "1")]).await;
    let body = serde_json::json!({
        "idempotency_scope": "ttl",
        "debit_account": "user:1",
        "credit_account": "m:9",
        "amount": 100,
        "currency": "USD",
    });
    let once = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "ttl-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(once.status(), 201);
    let v1: serde_json::Value = once.json().await.unwrap();
    assert_eq!(v1["idempotency"], "fresh");
    // Within TTL: same key replays the ORIGINAL tx.
    let twice = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "ttl-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let v2: serde_json::Value = twice.json().await.unwrap();
    assert_eq!(v2["idempotency"], "replayed");
    assert_eq!(v2["tx_id"], v1["tx_id"]);
    // Past TTL: same key is a NEW payment, flagged fresh, old id in DLQ.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let thrice = client
        .post(format!("{base}/payments"))
        .header("Idempotency-Key", "ttl-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let v3: serde_json::Value = thrice.json().await.unwrap();
    assert_eq!(v3["idempotency"], "fresh");
    assert_ne!(v3["tx_id"], v1["tx_id"]);
    let dlq: serde_json::Value =
        client.get(format!("{base}/dlq?limit=50")).send().await.unwrap().json().await.unwrap();
    let hits: Vec<&serde_json::Value> = dlq
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["reason"] == "key-expired-replayed" && e["key"] == "ttl-1")
        .collect();
    assert_eq!(hits.len(), 1, "expected one DLQ entry, got {dlq}");
    assert_eq!(hits[0]["tx_id"], v1["tx_id"]);
}
