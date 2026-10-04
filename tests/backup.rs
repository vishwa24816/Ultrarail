//! Ops leftovers: backup/restore round-trip and zero-downtime key rotation.

fn manifest_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

async fn spawn_with(dir: &std::path::Path, extra: &[(&str, &str)]) -> (String, tokio::process::Child) {
    let bin = env!("CARGO_BIN_EXE_payment-rail");
    let port: u16 = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env("JOURNAL_DIR", dir)
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("PARTITIONS", "2")
        .kill_on_drop(true)
        .current_dir(manifest_dir());
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

async fn post_with_key(client: &reqwest::Client, base: &str, key: &str, idem: &str) -> u16 {
    let body = serde_json::json!({
        "idempotency_scope": "ops",
        "debit_account": "user:1", "credit_account": "m:9",
        "amount": 10, "currency": "USD",
    });
    client
        .post(format!("{base}/payments"))
        .header("x-api-key", key)
        .header("Idempotency-Key", idem)
        .json(&body)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test]
async fn key_rotation_without_downtime() {
    let dir = tempfile::TempDir::new().unwrap();
    let keyfile = dir.path().join("client.keys");
    let old = "rotater-old-00000000000000000000001";
    let new = "rotater-new-00000000000000000000002";
    std::fs::write(&keyfile, format!("rotater:{old}")).unwrap();
    let (base, _child) = spawn_with(
        dir.path(),
        &[
            ("CLIENT_KEYS_FILE", keyfile.to_str().unwrap()),
            ("KEY_RELOAD_SECS", "1"),
            ("PAYMENT_TEST_HOOKS", "true"),
        ],
    )
    .await;
    let client = reqwest::Client::new();
    assert_eq!(post_with_key(&client, &base, old, "rk-1").await, 201);
    // Rotate: dual-key window, then new-only.
    std::fs::write(&keyfile, format!("rotater:{old},rotater:{new}")).unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(post_with_key(&client, &base, old, "rk-2").await, 201, "old key must work in window");
    assert_eq!(post_with_key(&client, &base, new, "rk-3").await, 201, "new key must work in window");
    std::fs::write(&keyfile, format!("rotater:{new}")).unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(post_with_key(&client, &base, new, "rk-4").await, 201);
    assert_eq!(post_with_key(&client, &base, old, "rk-5").await, 401, "old key revoked after window");
    println!("PASS: rotation with zero in-window 401s");
}

#[tokio::test]
async fn backup_restore_round_trip() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = reqwest::Client::new();
    let (base, mut child) =
        spawn_with(dir.path(), &[("PAYMENT_TEST_HOOKS", "true")]).await;
    let mut ids = vec![];
    for i in 0..10 {
        let body = serde_json::json!({
            "idempotency_scope": "backup", "debit_account": format!("user:{i}"),
            "credit_account": "m:9", "amount": 10, "currency": "USD",
        });
        let res = client
            .post(format!("{base}/payments"))
            .header("Idempotency-Key", format!("bk-{i}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201);
        let v: serde_json::Value = res.json().await.unwrap();
        ids.push(v["tx_id"].as_str().unwrap().to_string());
    }
    let backup = dir.path().join("backup");
    let st = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-File",
            "scripts/backup.ps1",
            "-JournalDir",
            dir.path().to_str().unwrap(),
            "-OutDir",
            backup.to_str().unwrap(),
            "-BaseUrl",
            &base,
        ])
        .current_dir(manifest_dir())
        .status()
        .expect("run backup.ps1");
    assert!(st.success(), "backup script failed");
    child.kill().await.unwrap();
    let _ = child.wait().await;
    // Wipe + restore into a fresh dir.
    let restored = dir.path().join("restored");
    let st = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-File",
            "scripts/restore.ps1",
            "-BackupDir",
            backup.to_str().unwrap(),
            "-JournalDir",
            restored.to_str().unwrap(),
        ])
        .current_dir(manifest_dir())
        .status()
        .expect("run restore.ps1");
    assert!(st.success(), "restore script failed");
    // Swap restored dir into place under a new temp home (server reads JOURNAL_DIR).
    let home = tempfile::TempDir::new().unwrap();
    let home_j = home.path().join("data");
    std::fs::create_dir_all(&home_j).unwrap();
    for f in std::fs::read_dir(&restored).unwrap() {
        let f = f.unwrap();
        std::fs::copy(f.path(), home_j.join(f.file_name())).unwrap();
    }
    let (base2, _c2) = spawn_with(&home_j, &[]).await;
    for (i, want) in ids.iter().enumerate() {
        let s: serde_json::Value = client
            .get(format!("{base2}/payments/{want}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(s["tx"]["tx_id"], *want, "tx {i} missing after restore");
    }
    println!("PASS: backup/restore round-trip, 10/10 txs queryable");
}
