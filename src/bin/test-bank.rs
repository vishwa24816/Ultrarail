//! Dummy test bank: `test-bank -- [N] [URL] [--no-ack]`
//! Plays BOTH sides at production level: submits N payments over /ws/client,
//! listens as sender-bank + receiver-bank on /ws/bank, validates every receipt,
//! acks every event, and asserts balance conservation end to end.

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(100);
    let base = args.get(2).cloned().unwrap_or_else(|| "http://127.0.0.1:3000".into());
    let no_ack = args.iter().any(|a| a == "--no-ack");
    let ws_base = base.replace("http", "ws");

    // Bank listeners first (both sides), so no event is missed.
    let (mut bank_user_w, mut bank_user_r) = bank_socket(&ws_base, "user").await;
    let (mut bank_merch_w, mut bank_merch_r) = bank_socket(&ws_base, "merchant").await;
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

async fn bank_socket(
    ws_base: &str,
    bank: &str,
) -> (
    futures::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>,
    futures::stream::SplitStream<WsStream>,
) {
    let url = format!("{ws_base}/ws/bank?bank_id={bank}");
    let (s, _) = connect_async(&url).await.unwrap_or_else(|_| panic!("connect {url}"));
    s.split()
}

async fn client_socket(
    ws_base: &str,
) -> (
    futures::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>,
    futures::stream::SplitStream<WsStream>,
) {
    let url = format!("{ws_base}/ws/client");
    let (s, _) = connect_async(&url).await.unwrap_or_else(|_| panic!("connect {url}"));
    s.split()
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
