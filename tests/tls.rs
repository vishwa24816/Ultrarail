//! TLS + API keys: HTTPS/WSS handshake with fixture cert, uniform 401s,
//! bank-key owner binding.

use std::sync::Arc;

const CLIENT_KEY: &str = "tester-000000000000000000000000000001";
const USER_KEY: &str = "user-000000000000000000000000000002";
const MERCH_KEY: &str = "merchant-00000000000000000000000003";

async fn spawn_tls(dir: &std::path::Path) -> (String, tokio::process::Child) {
    let bin = env!("CARGO_BIN_EXE_payment-rail");
    let port: u16 = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut child = tokio::process::Command::new(bin)
        .env("JOURNAL_DIR", dir)
        .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
        .env("PARTITIONS", "2")
        .env("TLS_CERT", "tests/certs/cert.pem")
        .env("TLS_KEY", "tests/certs/key.pem")
        .env("BANK_KEYS", format!("user:{USER_KEY},merchant:{MERCH_KEY}"))
        .env("CLIENT_KEYS", format!("tester:{CLIENT_KEY}"))
        .kill_on_drop(true)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .spawn()
        .expect("spawn server");
    let base = format!("https://127.0.0.1:{port}");
    let client = reqwest::Client::builder().danger_accept_invalid_certs(true).build().unwrap();
    for _ in 0..200 {
        if client.get(format!("{base}/health")).send().await.map(|r| r.status().is_success()).unwrap_or(false) {
            return (base, child);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let _ = child.kill().await;
    panic!("TLS server never became healthy");
}

fn ws_request(url: &str, key: &str) -> http::Request<()> {
    // Hand-built upgrade request: tungstenite validates these headers itself.
    http::Request::builder()
        .uri(url)
        .header("x-api-key", key)
        .header("host", "127.0.0.1")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap()
}
fn tls_connector() -> tokio_tungstenite::Connector {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let pem = std::fs::read("tests/certs/cert.pem").unwrap();
    let mut rd = std::io::BufReader::new(&pem[..]);
    let certs: Vec<_> = rustls_pemfile::certs(&mut rd).filter_map(|r| r.ok()).collect();
    assert!(!certs.is_empty());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certs[0].clone()).unwrap();
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_tungstenite::Connector::Rustls(Arc::new(cfg))
}

#[tokio::test]
async fn tls_auth_matrix() {
    let dir = tempfile::TempDir::new().unwrap();
    let (base, _child) = spawn_tls(dir.path()).await;
    let plain = reqwest::Client::new();
    let tls = reqwest::Client::builder().danger_accept_invalid_certs(true).build().unwrap();
    let body = serde_json::json!({
        "idempotency_scope": "tls", "debit_account": "user:1",
        "credit_account": "m:9", "amount": 50, "currency": "USD",
    });

    // Plaintext is dead on a TLS port.
    assert!(plain.get(format!("http://{}/health", base.trim_start_matches("https://"))).send().await.is_err());

    // No key -> uniform 401, same body for missing vs wrong.
    let no_key = tls.post(format!("{base}/payments")).json(&body).send().await.unwrap();
    assert_eq!(no_key.status(), 401);
    let no_body = no_key.text().await.unwrap();
    let bad_key = tls
        .post(format!("{base}/payments"))
        .header("x-api-key", "wrong-key-000000000000000000000000")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(bad_key.status(), 401);
    assert_eq!(bad_key.text().await.unwrap(), no_body, "401s must be identical (no oracle)");

    // Bank key on client route -> 401; client key works.
    let cross = tls
        .post(format!("{base}/payments"))
        .header("x-api-key", USER_KEY)
        .header("Idempotency-Key", "tls-cross")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(cross.status(), 401);
    let ok = tls
        .post(format!("{base}/payments"))
        .header("x-api-key", CLIENT_KEY)
        .header("Idempotency-Key", "tls-ok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 201);
}

#[tokio::test]
async fn wss_bank_flow_with_owner_binding() {
    use futures::{SinkExt, StreamExt};
    let dir = tempfile::TempDir::new().unwrap();
    let (base, _child) = spawn_tls(dir.path()).await;
    let wss = base.replace("https", "wss");
    let tls = reqwest::Client::builder().danger_accept_invalid_certs(true).build().unwrap();

    // Cross-bank snooping: user key as merchant -> 401 at upgrade.
    let req = ws_request(&format!("{wss}/ws/bank?bank_id=merchant"), USER_KEY);
    let denied = tokio_tungstenite::connect_async_tls_with_config(req, None, false, Some(tls_connector())).await;
    assert!(denied.is_err(), "cross-bank listen must fail");

    // Owner listens, submits over HTTPS, gets the event, acks.
    let req = ws_request(&format!("{wss}/ws/bank?bank_id=user"), USER_KEY);
    let (mut sock, _) =
        tokio_tungstenite::connect_async_tls_with_config(req, None, false, Some(tls_connector()))
            .await
            .expect("wss bank connect");
    let body = serde_json::json!({
        "idempotency_scope": "wss", "debit_account": "user:7",
        "credit_account": "m:7", "amount": 77, "currency": "USD",
    });
    let res = tls
        .post(format!("{base}/payments"))
        .header("x-api-key", CLIENT_KEY)
        .header("Idempotency-Key", "wss-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let ev: serde_json::Value = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let m = sock.next().await.unwrap().unwrap();
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(m.to_text().unwrap()) {
                if v.get("tx_id").is_some() {
                    return v;
                }
            }
        }
    })
    .await
    .expect("bank event over wss");
    assert_eq!(ev["amount"], 77);
    sock.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::json!({"tx_id": ev["tx_id"], "ack": true}).to_string().into(),
    ))
    .await
    .unwrap();
}
