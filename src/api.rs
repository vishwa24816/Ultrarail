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

#[derive(Serialize)]
struct Accepted {
    tx_id: String,
    status: &'static str,
    lsn: u64,
}

#[derive(Serialize)]
struct Rejected {
    status: &'static str,
    reason: String,
}

pub fn router(state: AppState) -> Router {
    use tower_http::limit::RequestBodyLimitLayer;
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/payments", axum::routing::post(create_payment))
        .layer(RequestBodyLimitLayer::new(64 * 1024))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state)
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
    let money = Money { amount: body.amount, currency: body.currency };
    let _ = Currency::USD; // keep import live across refactors
    metrics::observe_validation(t0.elapsed().as_secs_f64() * 1000.0);

    let (tx_reply, rx) = oneshot::channel();
    let cmd = WriteCmd {
        scope: body.idempotency_scope,
        key,
        money,
        debit_account: body.debit_account,
        credit_account: body.credit_account,
        reply: tx_reply,
    };
    if state.tx.send(cmd).await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"status":"REJECTED","reason":"writer overloaded"})))
            .into_response();
    }
    match rx.await {
        Ok(Ok((tx_id, lsn))) => {
            metrics::observe_total(t0.elapsed().as_secs_f64() * 1000.0);
            (StatusCode::CREATED, Json(serde_json::json!(Accepted { tx_id, status: "ACCEPTED_DURABLE", lsn })))
                .into_response()
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
