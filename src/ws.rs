//! Websocket planes: `/ws/client` (submit + receipts) and `/ws/bank` (settlement events + acks).
//! Protocol frozen for Phase 3 rail-adapter reuse. Localhost only; Phase 5 adds authN.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ws, Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use tokio::sync::{broadcast, oneshot};

use crate::app_state::{AppState, WriteCmd};
use crate::accounts::Terminal;
use crate::dlq::{now_ms, DlqEntry};
use crate::domain::{validate_account, CreatePayment, Money};
use crate::metrics;
use crate::partition;

pub const ACK_TIMEOUT_SECS: u64 = 30;

/// Streamed to banks per accepted tx.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BankEvent {
    pub tx_id: String,
    pub status: &'static str,
    pub amount: i64,
    pub price: i64,
    pub quantity: u64,
    pub currency: String,
    pub debit_account: String,
    pub credit_account: String,
    pub lsn: u64,
    pub partition: usize,
}

pub struct PendingAck {
    pub bank: String,
    pub deadline: Instant,
    pub tx_id: String,
}

pub type PendingMap = Arc<Mutex<HashMap<String, PendingAck>>>;

pub fn pending_map() -> PendingMap {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Shared submit path for HTTP and WS clients.
pub async fn submit(
    state: &AppState,
    scope: String,
    key: String,
    debit: String,
    credit: String,
    money: Money,
) -> Result<(String, u64, bool, usize), String> {
    let t0 = Instant::now();
    let n = state.partitions();
    let bucket = partition::bucket();
    let p = partition::route(&scope, &key, &debit, &credit, bucket, n);
    let tx_id = partition::tx_id(bucket, &debit, &credit);
    let (tx_reply, rx) = oneshot::channel();
    let cmd = WriteCmd { scope, key, money, debit_account: debit, credit_account: credit, bucket, tx_id, reply: tx_reply };
    if state.draining.load(std::sync::atomic::Ordering::Relaxed) {
        return Err("draining for shutdown".to_string());
    }
    state.writers[p].send(cmd).await?;
    let (id, lsn, replayed) = rx.await.map_err(|_| "writer gone".to_string())??;
    metrics::observe_total(t0.elapsed().as_secs_f64() * 1000.0);
    Ok((id, lsn, replayed, p))
}

pub fn publish(
    bcast: &broadcast::Sender<BankEvent>,
    pending: &PendingMap,
    ev: &BankEvent,
    bank_a: &str,
    bank_b: &str,
) {
    let _ = bcast.send(ev.clone());
    if let Ok(mut m) = pending.lock() {
        for bank in [bank_a, bank_b] {
            m.entry(format!("{}:{}", bank, ev.tx_id)).or_insert(PendingAck {
                bank: bank.to_string(),
                deadline: Instant::now() + Duration::from_secs(ack_timeout()),
                tx_id: ev.tx_id.clone(),
            });
        }
    }
}

fn ack_timeout() -> u64 {
    std::env::var("ACK_TIMEOUT_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(ACK_TIMEOUT_SECS)
}

fn parse_terminal(reason: &str) -> Option<Terminal> {
    match reason {
        "INSUFFICIENT_FUNDS" => Some(Terminal::InsufficientFunds),
        "RISKY_CLIENT" => Some(Terminal::RiskyClient),
        "FROZEN_DEBIT" => Some(Terminal::FrozenDebit),
        "FROZEN_CREDIT" => Some(Terminal::FrozenCredit),
        "FROZEN_TOTAL" => Some(Terminal::FrozenTotal),
        _ => None,
    }
}

/// Sweeper: unacked bank deliveries past deadline go to the DLQ. Spawn once at boot.
pub fn spawn_ack_sweeper(state: AppState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let expired: Vec<PendingAck> = match state.pending.lock() {
                Ok(mut m) => {
                    let now = Instant::now();
                    let xs: Vec<PendingAck> =
                        m.iter().filter(|(_, p)| p.deadline <= now).map(|(_, p)| PendingAck {
                            bank: p.bank.clone(),
                            deadline: p.deadline,
                            tx_id: p.tx_id.clone(),
                        }).collect();
                    for x in &xs {
                        m.remove(&format!("{}:{}", x.bank, x.tx_id));
                    }
                    xs
                }
                Err(_) => vec![],
            };
            let had = !expired.is_empty();
            for x in expired {
                if let Ok(mut d) = state.dlq.lock() {
                    d.push(DlqEntry {
                        reason: "bank-no-ack".into(),
                        scope: String::new(),
                        key: String::new(),
                        tx_id: Some(x.tx_id),
                        detail: format!("bank {} never acked", x.bank),
                        at_ms: now_ms(),
                    });
                }
            }
            // One fsync for the whole sweep batch — DLQ survives a crash right after.
            if had {
                if let Ok(mut d) = state.dlq.lock() {
                    d.flush();
                }
            }
        }
    });
}

pub async fn ws_client(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(|socket| async move {
        let (mut send, mut recv) = socket.split();
        while let Some(Ok(msg)) = recv.next().await {
            let text = match msg {
                ws::Message::Text(t) => t,
                ws::Message::Close(_) => break,
                _ => continue,
            };
            let v: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(e) => {
                    let _ = send.send(ws::Message::Text(format!(r#"{{"error":"bad json: {e}"}}"#).into())).await;
                    continue;
                }
            };
            if v["action"] != "submit" {
                let _ = send.send(ws::Message::Text(r#"{"error":"unknown action"}"#.into())).await;
                continue;
            }
            let body: Result<CreatePayment, _> = serde_json::from_value(serde_json::json!({
                "idempotency_scope": v["idempotency_scope"],
                "debit_account": v["debit_account"],
                "credit_account": v["credit_account"],
                "amount": v["amount"],
                "currency": v["currency"],
                "price": v["price"],
                "quantity": v["quantity"],
            }));
            let (key, body) = match (v["idempotency_key"].as_str(), body) {
                (Some(k), Ok(b)) => (k.to_string(), b),
                _ => {
                    let _ = send.send(ws::Message::Text(r#"{"status":"REJECTED","reason":"bad submit envelope"}"#.into())).await;
                    continue;
                }
            };
            if validator::Validate::validate(&body).is_err() {
                let _ = send.send(ws::Message::Text(r#"{"status":"REJECTED","reason":"validation"}"#.into())).await;
                continue;
            }
            let money = Money {
                amount: body.amount,
                price: body.price.unwrap_or(body.amount),
                quantity: body.quantity.unwrap_or(1),
                currency: body.currency,
            };
            let out = submit(&state, body.idempotency_scope, key, body.debit_account, body.credit_account, money).await;
            let reply = match out {
                Ok((id, lsn, replayed, _)) => serde_json::json!({
                    "tx_id": id, "status": "ACCEPTED_DURABLE", "lsn": lsn,
                    "idempotency": if replayed { "replayed" } else { "fresh" },
                }),
                Err(e) => serde_json::json!({"status": "REJECTED", "reason": e}),
            };
            if send.send(ws::Message::Text(reply.to_string().into())).await.is_err() {
                break;
            }
        }
    })
}

pub async fn ws_bank(
    State(state): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
    owner: Option<axum::extract::Extension<crate::auth::BankOwner>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let bank = q.get("bank_id").cloned().unwrap_or_default();
    // Localhost only; Phase 5 adds authN. Allowlist keeps routing keys sane.
    if bank.is_empty() || !validate_account(&bank) {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    // A bank key listens only as its owner (no cross-bank snooping).
    if let Some(axum::extract::Extension(o)) = owner {
        if o.0 != bank {
            return axum::http::StatusCode::UNAUTHORIZED.into_response();
        }
    }
    ws.on_upgrade(|socket| async move {
        let (mut send, mut recv) = socket.split();
        let mut rx = state.bcast.subscribe();
        // Bounded inbox: a lagging bank gets dropped, never blocks writers.
        loop {
            tokio::select! {
                got = rx.recv() => {
                    let ev: BankEvent = match got {
                        Ok(e) => e,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            if let Ok(mut d) = state.dlq.lock() {
                                d.push(DlqEntry {
                                    reason: "bank-lag-drop".into(), scope: String::new(),
                                    key: String::new(), tx_id: None,
                                    detail: format!("bank {bank} lagged off the stream"),
                                    at_ms: now_ms(),
                                });
                            }
                            break;
                        }
                        Err(_) => break,
                    };
                    if ev.debit_account.starts_with(&bank) || ev.credit_account.starts_with(&bank) {
                        let s = serde_json::to_string(&ev).unwrap_or_default();
                        if send.send(ws::Message::Text(s.into())).await.is_err() {
                            break;
                        }
                    }
                }
                msg = recv.next() => {
                    match msg {
                        Some(Ok(ws::Message::Text(t))) => {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                                if v["ack"] == true {
                                    if let Some(id) = v["tx_id"].as_str() {
                                        if let Ok(mut m) = state.pending.lock() {
                                            m.remove(&format!("{bank}:{id}"));
                                        }
                                    }
                                } else if v["type"] == "balance.update" {
                                    if let (Some(acct), Some(bal)) =
                                        (v["account"].as_str(), v["balance"].as_i64())
                                    {
                                        state.registry.set_balance(acct, bal);
                                    }
                                } else if v["type"] == "account.flag" {
                                    if let (Some(acct), Some(flag), Some(on)) = (
                                        v["account"].as_str(),
                                        v["flag"].as_str(),
                                        v["on"].as_bool(),
                                    ) {
                                        state.registry.set_flag(acct, flag, on);
                                    }
                                } else if v["type"] == "tx.failed" {
                                    if let (Some(id), Some(reason)) =
                                        (v["tx_id"].as_str(), v["reason"].as_str())
                                    {
                                        if let Some(term) = parse_terminal(reason) {
                                            let _ = state.failed_tx.send((id.to_string(), term));
                                        }
                                        if let Ok(mut m) = state.pending.lock() {
                                            m.remove(&format!("{bank}:{id}"));
                                        }
                                    }
                                }
                            }
                        }
                        _ => break,
                    }
                }
            }
        }
    })
    .into_response()
}
