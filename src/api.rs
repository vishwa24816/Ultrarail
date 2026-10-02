//! HTTP API: POST /payments (201 ACCEPTED_DURABLE / 422 REJECTED), GET /health.

use std::time::Instant;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use tokio::sync::oneshot;
use validator::Validate;

use crate::app_state::AppState;
use crate::app_state::WriteCmd;
use crate::domain::{CreatePayment, Currency, Money};
use crate::metrics;
use crate::partition;

#[derive(Serialize)]
struct Accepted {
    tx_id: String,
    status: &'static str,
    lsn: u64,
    idempotency: &'static str,
}

#[derive(Serialize)]
struct Rejected {
    status: &'static str,
    reason: String,
}

pub fn router(state: AppState) -> Router {
    use tower_http::limit::RequestBodyLimitLayer;
    let mut r = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(read_ready))
        .route("/payments", axum::routing::post(create_payment))
        .route("/dlq", get(read_dlq))
        .route("/audit", get(read_audit))
        .route("/payments/:id", get(read_payment))
        .route("/ws/client", get(crate::ws::ws_client))
        .route("/ws/bank", get(crate::ws::ws_bank));
    // Sandbox-only shutdown hook: same code path as SIGTERM. Never enable in prod.
    if std::env::var("PAYMENT_TEST_HOOKS").as_deref() == Ok("true") {
        r = r.route("/test/shutdown", axum::routing::post(test_shutdown));
    }
    r.layer(RequestBodyLimitLayer::new(64 * 1024))
        // 503s from /ready polling are routine, not errors — keep logs clean.
        .layer(tower_http::trace::TraceLayer::new_for_http().on_failure(()))
        .with_state(state)
}

async fn test_shutdown(State(state): State<AppState>) -> impl IntoResponse {
    state.draining.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = state.shutdown_tx.send(true);
    (StatusCode::ACCEPTED, Json(serde_json::json!({"draining": true}))).into_response()
}

async fn read_ready(State(state): State<AppState>) -> impl IntoResponse {
    let ok = state.ready.lock().map(|r| !r.is_empty() && r.iter().all(|b| *b)).unwrap_or(false);
    if ok {
        (StatusCode::OK, Json(serde_json::json!({"ready": true}))).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"ready": false}))).into_response()
    }
}

async fn read_payment(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let stored = state.store.lock().ok().and_then(|s| s.get(&id).cloned());
    match stored {
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown tx"}))).into_response(),
        Some(st) => {
            let mut evs = vec![];
            if st.partition < state.events.len() {
                evs = crate::delivery::Delivery::snapshot(&state.events[st.partition], &id);
            }
            let last = evs.last().map(|e| format!("{:?}", e.kind)).unwrap_or_else(|| "ACCEPTED".into());
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "tx": st.tx, "lsn": st.lsn, "partition": st.partition,
                    "delivery_state": last, "events": evs,
                })),
            )
                .into_response()
        }
    }
}
async fn read_audit(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    // Operator view. Localhost only; Phase 5 auth covers it (03-04).
    match (q.get("tx_id"), state.audit.lock()) {
        (Some(id), Ok(a)) => {
            // ponytail: full scan per query; index when it matters.
            let entries = a.for_tx(id, q.get("limit").and_then(|s| s.parse().ok()).unwrap_or(50));
            (StatusCode::OK, Json(serde_json::json!(entries))).into_response()
        }
        _ => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "tx_id required"}))).into_response(),
    }
}

async fn read_dlq(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let limit: usize = q.get("limit").and_then(|s| s.parse().ok()).unwrap_or(20).min(200);
    match state.dlq.lock() {
        Ok(d) => match d.tail(limit) {
            Ok(entries) => (StatusCode::OK, Json(serde_json::json!(entries))).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
        },
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "dlq locked"}))).into_response(),
    }
}

async fn create_payment(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<CreatePayment>,
) -> impl IntoResponse {
    let t0 = Instant::now();
    let key = match headers.get("idempotency-key").and_then(|v| v.to_str().ok()) {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => {
            metrics::count_rejected();
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"status":"REJECTED","reason":"missing Idempotency-Key header"})),
            )
                .into_response();
        }
    };
    if let Err(e) = body.validate() {
        metrics::count_rejected();
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"status":"REJECTED","reason":e.to_string()})),
        )
            .into_response();
    }
    // Currency deserializes via serde; unknown variants already rejected with 422 by Json extractor.
    let money = Money {
        amount: body.amount,
        price: body.price.unwrap_or(body.amount),
        quantity: body.quantity.unwrap_or(1),
        currency: body.currency,
    };
    let _ = Currency::USD; // keep import live across refactors
    metrics::observe_validation(t0.elapsed().as_secs_f64() * 1000.0);

    let (tx_reply, rx) = oneshot::channel();
    let n = state.partitions();
    let bucket = partition::bucket();
    let p = partition::route(&body.idempotency_scope, &key, &body.debit_account, &body.credit_account, bucket, n);
    let tx_id = partition::tx_id(bucket, &body.debit_account, &body.credit_account);
    let cmd = WriteCmd {
        scope: body.idempotency_scope,
        key,
        money,
        debit_account: body.debit_account,
        credit_account: body.credit_account,
        bucket,
        tx_id,
        reply: tx_reply,
    };
    if state.draining.load(std::sync::atomic::Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "5")],
            Json(serde_json::json!({"status":"REJECTED","reason":"draining for shutdown"})),
        )
            .into_response();
    }
    if let Err(reason) = state.writers[p].send(cmd).await {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"status":"REJECTED","reason":reason})))
            .into_response();
    }
    match rx.await {
        Ok(Ok((tx_id, lsn, replayed))) => {
            metrics::observe_total(t0.elapsed().as_secs_f64() * 1000.0);
            let body = Accepted {
                tx_id,
                status: "ACCEPTED_DURABLE",
                lsn,
                idempotency: if replayed { "replayed" } else { "fresh" },
            };
            (StatusCode::CREATED, Json(serde_json::json!(body))).into_response()
        }
        Ok(Err(reason)) => {
            let rej = Rejected { status: "REJECTED", reason };
            (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!(rej))).into_response()
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"REJECTED","reason":"writer gone"})),
        )
            .into_response(),
    }
}
