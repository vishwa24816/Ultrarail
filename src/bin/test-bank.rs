//! Dummy test bank: `test-bank -- [N] [URL] [--no-ack]`
//! Plays BOTH sides at production level: submits N payments over /ws/client,
//! listens as sender-bank + receiver-bank on /ws/bank, validates every receipt,
//! acks every event, and asserts balance conservation end to end.

use futures::{SinkExt, StreamExt};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(100);
    let base = args.get(2).cloned().unwrap_or_else(|| "http://127.0.0.1:3000".into());
    let no_ack = args.iter().any(|a| a == "--no-ack");
    let ws_base = base.replace("http", "ws");

    // Bank listeners first (both sides), so no event is missed.
    // Owner-bound keys: each side authenticates as itself.
    let bank_user_key = std::env::var("BANK_KEY_USER").ok();
    let bank_merch_key = std::env::var("BANK_KEY_MERCHANT").ok();
    let (mut bank_user_w, mut bank_user_r) = bank_socket(&ws_base, "user", bank_user_key).await;
    let (mut bank_merch_w, mut bank_merch_r) = bank_socket(&ws_base, "merchant", bank_merch_key).await;
    let (mut cli_w, mut cli_r) = client_socket(&ws_base).await;

    if args.iter().any(|a| a == "--terminal") {
        terminal_scenarios(&base, &mut bank_user_w, &mut cli_w, &mut cli_r).await;
        return;
    }

    let mut submitted: Vec<(String, i64)> = Vec::with_capacity(n);
    let mut receipts = 0;
    for i in 0..n {
        let key = format!("testbank-{i}");
        let amount = 100 + (i as i64 % 50);
        let req = serde_json::json!({
            "action": "submit",
            "idempotency_scope": "testbank",
            "idempotency_key": key,
            "debit_account": format!("user:{}", i % 10),
            "credit_account": format!("merchant:{}", i % 3),
            "amount": amount,
            "currency": "USD",
        });
        cli_w.send(tokio_tungstenite::tungstenite::Message::Text(req.to_string().into())).await.unwrap();
        let msg = cli_r.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(v["status"], "ACCEPTED_DURABLE", "receipt {i}: {v}");
        assert_eq!(v["idempotency"], "fresh", "receipt {i}: {v}");
        submitted.push((v["tx_id"].as_str().unwrap().to_string(), amount));
        receipts += 1;
    }

    // Drain both bank streams, validate + ack every event.
    let mut seen_user = 0;
    let mut seen_merch = 0;
    let mut sum_debit = 0i64;
    let mut sum_credit = 0i64;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while (seen_user < n || seen_merch < n) && tokio::time::Instant::now() < deadline {
        let next = async {
            tokio::select! {
                m = bank_user_r.next() => (true, m),
                m = bank_merch_r.next() => (false, m),
            }
        };
        let (is_user, m) = next.await;
        let msg = match m {
            Some(Ok(x)) => x,
            _ => break,
        };
        let ev: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        let amount = ev["amount"].as_i64().unwrap();
        assert_eq!(ev["amount"], ev["price"], "price must equal amount for qty 1: {ev}");
        sum_debit += amount;
        sum_credit += amount;
        if !no_ack {
            let ack = serde_json::json!({"tx_id": ev["tx_id"], "ack": true}).to_string();
            let w = if is_user { &mut bank_user_w } else { &mut bank_merch_w };
            w.send(tokio_tungstenite::tungstenite::Message::Text(ack.into())).await.unwrap();
        }
        if is_user {
            seen_user += 1;
        } else {
            seen_merch += 1;
        }
    }

    println!("--- test-bank ---");
    println!("submitted: {n}  receipts: {receipts}  bank-user events: {seen_user}  bank-merchant events: {seen_merch}");
    println!("sum debit: {sum_debit}  sum credit: {sum_credit}");
    let expect: i64 = submitted.iter().map(|(_, a)| a).sum();
    // Each tx is seen by BOTH banks, so each side sums to the submitted total.
    assert_eq!(receipts, n, "every submit must be accepted");
    assert_eq!(seen_user, n, "sender bank must see every tx");
    assert_eq!(seen_merch, n, "receiver bank must see every tx");
    assert_eq!(sum_debit, 2 * expect, "debit conservation across both banks");
    assert_eq!(sum_credit, 2 * expect, "credit conservation across both banks");
    assert_eq!(sum_debit, sum_credit, "double-entry balance");
    println!("PASS: {n}/{n} dual-side validated, books balance");
}

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn api_key() -> Option<String> {
    std::env::var("CLIENT_KEY").or_else(|_| std::env::var("BANK_KEY")).ok()
}

async fn bank_socket(
    ws_base: &str,
    bank: &str,
    key: Option<String>,
) -> (
    futures::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>,
    futures::stream::SplitStream<WsStream>,
) {
    let url = format!("{ws_base}/ws/bank?bank_id={bank}");
    connect_with_key(&url, key).await
}

async fn client_socket(
    ws_base: &str,
) -> (
    futures::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>,
    futures::stream::SplitStream<WsStream>,
) {
    let url = format!("{ws_base}/ws/client");
    connect_with_key(&url, api_key()).await
}

async fn connect_with_key(
    url: &str,
    key: Option<String>,
) -> (
    futures::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>,
    futures::stream::SplitStream<WsStream>,
) {
    // Sandbox TLS uses a self-signed fixture: accept invalid certs ONLY here
    // (test-bank is a rehearsal tool, never production traffic).
    let connector = url.starts_with("wss").then(|| {
        let cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoVerify))
            .with_no_client_auth();
        tokio_tungstenite::Connector::Rustls(std::sync::Arc::new(cfg))
    });
    let mut req = http::Request::builder()
        .uri(url)
        .header("host", "127.0.0.1")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==");
    if let Some(k) = key {
        req = req.header("x-api-key", k);
    }
    let (s, _) = tokio_tungstenite::connect_async_tls_with_config(
        req.body(()).unwrap(),
        None,
        false,
        connector,
    )
    .await
    .unwrap_or_else(|_| panic!("connect {url} (server down? keys missing/rejected?)"));
    s.split()
}

#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end: &rustls::pki_types::CertificateDer,
        _inter: &[rustls::pki_types::CertificateDer],
        _server: &rustls::pki_types::ServerName,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _msg: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _msg: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes().to_vec()
    }
}

type CliW = futures::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>;
type CliR = futures::stream::SplitStream<WsStream>;

/// Terminal-failure rehearsal: frozen account + low balance fail WITHOUT retry.
async fn terminal_scenarios(base: &str, bank_w: &mut CliW, cli_w: &mut CliW, cli_r: &mut CliR) {
    use tokio_tungstenite::tungstenite::Message;
    let client = reqwest::Client::new();

    // Freeze user:99 totally via bank WS.
    bank_w
        .send(Message::Text(
            r#"{"type":"account.flag","account":"user:99","flag":"totally_frozen","on":true}"#.into(),
        ))
        .await
        .unwrap();
    // Low balance for user:98.
    bank_w
        .send(Message::Text(r#"{"type":"balance.update","account":"user:98","balance":5}"#.into()))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for (i, debit, amount, want) in [
        (0, "user:99", 100, "FROZEN_TOTAL"),
        (1, "user:98", 100, "INSUFFICIENT_FUNDS"),
    ] {
        let req = serde_json::json!({
            "action": "submit",
            "idempotency_scope": "terminal",
            "idempotency_key": format!("term-{i}"),
            "debit_account": debit,
            "credit_account": "merchant:1",
            "amount": amount,
            "currency": "USD",
        });
        cli_w.send(Message::Text(req.to_string().into())).await.unwrap();
        let msg = cli_r.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(v["status"], "ACCEPTED_DURABLE", "accept first, fail in delivery: {v}");
        let tx_id = v["tx_id"].as_str().unwrap().to_string();
        // Poll status until terminal FAILED.
        let mut state = String::new();
        let mut events = vec![];
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let s: serde_json::Value =
                client.get(format!("{base}/payments/{tx_id}")).send().await.unwrap().json().await.unwrap();
            state = s["delivery_state"].as_str().unwrap_or("").to_string();
            events = s["events"].as_array().cloned().unwrap_or_default();
            if state == "Failed" {
                break;
            }
        }
        assert_eq!(state, "Failed", "tx {tx_id} must end FAILED, got {state}");
        let failed = events.iter().find(|e| e["kind"] == "failed").expect("failed event");
        let detail = failed["detail"].as_str().unwrap_or("");
        assert!(
            detail.contains(&format!("transaction failed for {want}")),
            "message must name the flag, got: {detail}"
        );
        assert!(
            !events.iter().any(|e| e["kind"] == "attempt"),
            "terminal failures must not attempt the rail: {events:?}"
        );
        println!("terminal case {want}: FAILED as required, zero rail attempts");
    }
    println!("PASS: terminal failures fail fast without retry");
}
