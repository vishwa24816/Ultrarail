//! API-key auth: `BANK_KEYS=owner:secret,...`, `CLIENT_KEYS=name:secret,...`.
//! Unset => open sandbox mode + loud WARN. Set => enforced, uniform 401s,
//! constant-time compare, bank WS bound to key owner.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;

#[derive(Clone, Default)]
pub struct KeySets {
    pub bank: Arc<HashMap<String, String>>,   // owner -> secret
    pub client: Arc<HashMap<String, String>>, // name -> secret
    pub enforced: bool,
}

fn parse_list(env: &str) -> HashMap<String, String> {
    // Canonical PAYMENT_ names first, bare legacy names as fallback.
    let bare = env.strip_prefix("PAYMENT_").unwrap_or(env);
    std::env::var(env)
        .or_else(|_| std::env::var(bare))
        .unwrap_or_default()
        .split(',')
        .filter_map(|p| {
            let (k, v) = p.split_once(':')?;
            let (k, v) = (k.trim(), v.trim());
            if k.len() >= 1 && v.len() >= 32 {
                Some((k.to_string(), v.to_string()))
            } else {
                None
            }
        })
        .collect()
}

impl KeySets {
    pub fn from_env() -> Self {
        let bank = parse_list("PAYMENT_BANK_KEYS");
        let client = parse_list("PAYMENT_CLIENT_KEYS");
        let enforced = !bank.is_empty() || !client.is_empty();
        if !enforced {
            tracing::warn!("no API keys configured: running OPEN (sandbox only, do not expose)");
        }
        Self { bank: Arc::new(bank), client: Arc::new(client), enforced }
    }
}

/// Constant-time equality. Same work for match and mismatch paths.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        // Still walk to avoid a length-timing shortcut, then reject.
        let n = a.len().max(b.len());
        let mut d = 0u8;
        for i in 0..n {
            d |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
        }
        let _ = d;
        return false;
    }
    let mut d = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        d |= x ^ y;
    }
    d == 0
}

fn uniform_401() -> Response {
    (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "unauthorized"}))).into_response()
}

fn lookup(keys: &HashMap<String, String>, presented: &str) -> Option<String> {
    // Compare against every entry (no early exit on name) to avoid user enumeration.
    let mut hit = None;
    for (name, secret) in keys {
        if ct_eq(secret.as_bytes(), presented.as_bytes()) {
            hit = Some(name.clone());
        }
    }
    hit
}

fn key_of(req: &Request) -> Option<String> {
    req.headers().get("x-api-key")?.to_str().ok().map(|s| s.to_string())
}

/// Client plane: /payments, /ws/client, /dlq, /audit, /payments/:id.
pub async fn client_auth(State(keys): State<KeySets>, req: Request, next: Next) -> Response {
    if !keys.enforced {
        return next.run(req).await;
    }
    match key_of(&req).and_then(|k| lookup(&keys.client, &k)) {
        Some(_) => next.run(req).await,
        None => uniform_401(),
    }
}

/// Bank plane: /ws/bank. Inserts owner into extensions; handler enforces bank_id == owner.
pub async fn bank_auth(State(keys): State<KeySets>, mut req: Request, next: Next) -> Response {
    if !keys.enforced {
        return next.run(req).await;
    }
    match key_of(&req).and_then(|k| lookup(&keys.bank, &k)) {
        Some(owner) => {
            req.extensions_mut().insert(BankOwner(owner));
            next.run(req).await
        }
        None => uniform_401(),
    }
}

#[derive(Clone)]
pub struct BankOwner(pub String);
