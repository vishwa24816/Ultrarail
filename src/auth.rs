//! API-key auth: `BANK_KEYS=owner:secret,...`, `CLIENT_KEYS=name:secret,...`
//! or `BANK_KEYS_FILE` / `CLIENT_KEYS_FILE` (same format, one per line or
//! comma-separated) with mtime reload for zero-downtime rotation.
//! Unset => open sandbox mode + loud WARN. Set => enforced, uniform 401s,
//! constant-time compare, bank WS bound to key owner.

use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;

#[derive(Clone, Default)]
pub struct KeySets {
    // (owner/name, secret) pairs — one name may hold several secrets during rotation.
    pub bank: Arc<RwLock<Vec<(String, String)>>>,
    pub client: Arc<RwLock<Vec<(String, String)>>>,
    pub enforced: bool,
}

fn parse_text(raw: &str) -> Vec<(String, String)> {
    raw.split([',', '\n'])
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

fn parse_list(env: &str) -> Vec<(String, String)> {
    // Canonical PAYMENT_ names first, bare legacy names as fallback.
    let bare = env.strip_prefix("PAYMENT_").unwrap_or(env);
    parse_text(
        &std::env::var(env).or_else(|_| std::env::var(bare)).unwrap_or_default(),
    )
}

fn read_key_file(env: &str) -> Option<(String, Vec<(String, String)>)> {
    let bare = env.strip_prefix("PAYMENT_").unwrap_or(env);
    let path =
        std::env::var(env).or_else(|_| std::env::var(format!("{bare}_FILE"))).ok()?;
    let raw = std::fs::read_to_string(&path).ok()?;
    Some((path, parse_text(&raw)))
}

impl KeySets {
    pub fn from_env() -> Self {
        let bank = parse_list("PAYMENT_BANK_KEYS");
        let client = parse_list("PAYMENT_CLIENT_KEYS");
        // File mode wins when present (rotation-capable); else env lists.
        let bank = read_key_file("PAYMENT_BANK_KEYS").map(|(_, m)| m).unwrap_or(bank);
        let client = read_key_file("PAYMENT_CLIENT_KEYS").map(|(_, m)| m).unwrap_or(client);
        let enforced = !bank.is_empty() || !client.is_empty();
        if !enforced {
            tracing::warn!("no API keys configured: running OPEN (sandbox only, do not expose)");
        } else {
            // Never log secrets — paths only. Production files must be ACL-restricted.
            for env in ["PAYMENT_BANK_KEYS_FILE", "BANK_KEYS_FILE", "PAYMENT_CLIENT_KEYS_FILE", "CLIENT_KEYS_FILE"] {
                if let Ok(p) = std::env::var(env) {
                    tracing::info!("key file in use: {env}={p}");
                }
            }
        }
        let ks = Self {
            bank: Arc::new(RwLock::new(bank)),
            client: Arc::new(RwLock::new(client)),
            enforced,
        };
        ks.spawn_reloader();
        ks
    }

    /// Poll key files for rotation (30s). Env-var mode requires restart (documented).
    fn spawn_reloader(&self) {
        let bank = self.bank.clone();
        let client = self.client.clone();
        let every = std::env::var("PAYMENT_KEY_RELOAD_SECS")
            .or_else(|_| std::env::var("KEY_RELOAD_SECS"))
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        tokio::spawn(async move {
            let mut last_bank = file_mtime("PAYMENT_BANK_KEYS");
            let mut last_client = file_mtime("PAYMENT_CLIENT_KEYS");
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(every)).await;
                if file_mtime("PAYMENT_BANK_KEYS") != last_bank {
                    if let Some((_, m)) = read_key_file("PAYMENT_BANK_KEYS") {
                        if let Ok(mut g) = bank.write() {
                            *g = m;
                            tracing::info!("bank keys reloaded");
                        }
                    }
                    last_bank = file_mtime("PAYMENT_BANK_KEYS");
                }
                if file_mtime("PAYMENT_CLIENT_KEYS") != last_client {
                    if let Some((_, m)) = read_key_file("PAYMENT_CLIENT_KEYS") {
                        if let Ok(mut g) = client.write() {
                            *g = m;
                            tracing::info!("client keys reloaded");
                        }
                    }
                    last_client = file_mtime("PAYMENT_CLIENT_KEYS");
                }
            }
        });
    }
}

fn file_mtime(env: &str) -> Option<SystemTime> {
    let bare = env.strip_prefix("PAYMENT_").unwrap_or(env);
    let path =
        std::env::var(env).or_else(|_| std::env::var(format!("{bare}_FILE"))).ok()?;
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
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

fn lookup(keys: &[(String, String)], presented: &str) -> Option<String> {
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
    match key_of(&req).and_then(|k| keys.client.read().ok().and_then(|g| lookup(&g, &k))) {
        Some(_) => next.run(req).await,
        None => uniform_401(),
    }
}

/// Bank plane: /ws/bank. Inserts owner into extensions; handler enforces bank_id == owner.
pub async fn bank_auth(State(keys): State<KeySets>, mut req: Request, next: Next) -> Response {
    if !keys.enforced {
        return next.run(req).await;
    }
    match key_of(&req).and_then(|k| keys.bank.read().ok().and_then(|g| lookup(&g, &k))) {
        Some(owner) => {
            req.extensions_mut().insert(BankOwner(owner));
            next.run(req).await
        }
        None => uniform_401(),
    }
}

#[derive(Clone)]
pub struct BankOwner(pub String);
