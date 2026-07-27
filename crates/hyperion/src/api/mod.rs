//! Hyperion-compatible HTTP API (v2, plus a v1 history compatibility layer).

mod health;
mod history;
mod state;
mod v1;

use crate::config::Config;
use crate::elastic::Elastic;
use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct ApiState {
    pub es: Elastic,
    pub config: Config,
    pub chain: crate::chain_client::ChainClient,
    lib_cache: Mutex<Option<(Instant, Value)>>,
}

pub type Shared = Arc<ApiState>;

impl ApiState {
    /// `get_info` from the chain API, cached briefly (head/lib move every
    /// 500 ms; every API response embeds them).
    pub async fn chain_info(&self) -> Value {
        {
            let cache = self.lib_cache.lock().await;
            if let Some((at, info)) = cache.as_ref() {
                if at.elapsed() < Duration::from_secs(2) {
                    return info.clone();
                }
            }
        }
        let info = match self.chain.get_info().await {
            Ok(info) => info,
            Err(e) => {
                tracing::debug!(error = %e, "get_info failed");
                Value::Null
            }
        };
        *self.lib_cache.lock().await = Some((Instant::now(), info.clone()));
        info
    }

    pub async fn lib(&self) -> u64 {
        self.chain_info().await["last_irreversible_block_num"]
            .as_u64()
            .unwrap_or(0)
    }
}

/// Uniform API error → JSON body with proper status code.
pub struct ApiError(pub StatusCode, pub String);

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        ApiError(StatusCode::BAD_REQUEST, msg.into())
    }

    pub fn internal(err: impl std::fmt::Display) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

pub type ApiResult = std::result::Result<Json<Value>, ApiError>;

pub async fn run(config: Config) -> Result<()> {
    let listen = config.api.listen.clone();
    let state: Shared = Arc::new(ApiState {
        es: Elastic::new(&config.elasticsearch),
        chain: config.chain.client(),
        config,
        lib_cache: Mutex::new(None),
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/v2/health", get(health::health))
        .route("/v2/history/get_actions", get(history::get_actions))
        .route("/v2/history/get_transaction", get(history::get_transaction))
        .route("/v2/history/get_deltas", get(history::get_deltas))
        .route(
            "/v2/history/get_abi_snapshot",
            get(history::get_abi_snapshot),
        )
        .route(
            "/v2/history/get_created_accounts",
            get(history::get_created_accounts),
        )
        .route("/v2/history/get_creator", get(history::get_creator))
        .route("/v2/state/get_key_accounts", get(state::get_key_accounts))
        .route("/v2/state/get_tokens", get(state::get_tokens))
        .route("/v2/state/get_account", get(state::get_account))
        .route("/v1/history/get_actions", post(v1::get_actions))
        .route("/v1/history/get_transaction", post(v1::get_transaction))
        .route("/v1/history/get_key_accounts", post(v1::get_key_accounts))
        .route(
            "/v1/history/get_controlled_accounts",
            post(v1::get_controlled_accounts),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    tracing::info!(listen, "API server started");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root(State(state): State<Shared>) -> Json<Value> {
    Json(json!({
        "name": "hyperion-rs",
        "version": env!("CARGO_PKG_VERSION"),
        "chain": state.config.chain.name,
        "endpoints": [
            "/v2/health",
            "/v2/history/get_actions",
            "/v2/history/get_transaction",
            "/v2/history/get_deltas",
            "/v2/history/get_abi_snapshot",
            "/v2/history/get_created_accounts",
            "/v2/history/get_creator",
            "/v2/state/get_key_accounts",
            "/v2/state/get_tokens",
            "/v2/state/get_account",
            "/v1/history/get_actions",
            "/v1/history/get_transaction",
            "/v1/history/get_key_accounts",
            "/v1/history/get_controlled_accounts",
        ],
    }))
}

// ---------------------------------------------------------------------------
// Shared query helpers
// ---------------------------------------------------------------------------

/// Clamp a user-supplied limit to the configured maximum.
pub fn clamp_limit(state: &ApiState, limit: Option<usize>) -> usize {
    limit.unwrap_or(100).min(state.config.api.max_limit)
}

/// Interpret an `after`/`before` boundary: integers are block numbers,
/// anything else is treated as an ISO timestamp.
pub fn range_filters(after: Option<&str>, before: Option<&str>) -> Vec<Value> {
    let mut filters = Vec::new();
    let mut block_range = serde_json::Map::new();
    let mut time_range = serde_json::Map::new();
    for (bound, value) in [("gte", after), ("lte", before)] {
        if let Some(value) = value {
            if let Ok(block) = value.parse::<u64>() {
                block_range.insert(bound.to_string(), json!(block));
            } else {
                time_range.insert(bound.to_string(), json!(value));
            }
        }
    }
    if !block_range.is_empty() {
        filters.push(json!({"range": {"block_num": block_range}}));
    }
    if !time_range.is_empty() {
        filters.push(json!({"range": {"@timestamp": time_range}}));
    }
    filters
}

/// Run a search and return `(hits_sources, total, took_ms)`.
pub async fn search_sources(
    state: &ApiState,
    index_kind: &str,
    body: Value,
) -> std::result::Result<(Vec<Value>, Value, u64), ApiError> {
    let index = state.config.index(index_kind);
    let started = Instant::now();
    let res = state
        .es
        .search(&index, body)
        .await
        .map_err(ApiError::internal)?;
    let took = started.elapsed().as_millis() as u64;
    let hits = res["hits"]["hits"]
        .as_array()
        .map(|hits| hits.iter().map(|h| h["_source"].clone()).collect())
        .unwrap_or_default();
    let total = res["hits"]["total"].clone();
    Ok((hits, total, took))
}
