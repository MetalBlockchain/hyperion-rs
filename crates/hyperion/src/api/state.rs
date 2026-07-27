use super::{search_sources, ApiResult, Shared};
use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub struct KeyParams {
    pub public_key: String,
}

pub async fn get_key_accounts(
    State(state): State<Shared>,
    Query(params): Query<KeyParams>,
) -> ApiResult {
    let key = antelope::keys::normalize_public_key(params.public_key.trim());
    let body = json!({
        "size": 1000,
        "query": {"bool": {"filter": [{"term": {"keys": key}}]}},
        "sort": [{"owner": {"order": "asc"}}],
    });
    let (hits, _, took) = search_sources(&state, "perm", body).await?;
    let mut account_names: Vec<&str> = hits.iter().filter_map(|p| p["owner"].as_str()).collect();
    account_names.dedup();
    Ok(Json(json!({
        "query_time_ms": took,
        "account_names": account_names,
        "permissions": hits,
    })))
}

#[derive(Debug, Deserialize)]
pub struct AccountParams {
    pub account: String,
}

async fn fetch_tokens(state: &Shared, account: &str) -> Result<Vec<Value>, super::ApiError> {
    let body = json!({
        "size": 1000,
        "query": {"bool": {"filter": [{"term": {"scope": account}}]}},
        "sort": [{"amount": {"order": "desc"}}],
    });
    let (hits, _, _) = search_sources(state, "token", body).await?;
    Ok(hits
        .iter()
        .map(|t| {
            json!({
                "symbol": t["symbol"],
                "precision": t["precision"],
                "amount": t["amount"],
                "contract": t["code"],
            })
        })
        .collect())
}

pub async fn get_tokens(
    State(state): State<Shared>,
    Query(params): Query<AccountParams>,
) -> ApiResult {
    let tokens = fetch_tokens(&state, &params.account).await?;
    Ok(Json(json!({
        "account": params.account,
        "tokens": tokens,
    })))
}

pub async fn get_account(
    State(state): State<Shared>,
    Query(params): Query<AccountParams>,
) -> ApiResult {
    let tokens = fetch_tokens(&state, &params.account).await?;

    let body = json!({
        "track_total_hits": true,
        "size": 20,
        "query": {"bool": {"filter": [{"term": {"notified": params.account}}]}},
        "sort": [{"global_sequence": {"order": "desc"}}],
    });
    let (mut actions, total, took) = search_sources(&state, "action", body).await?;
    for action in &mut actions {
        if let Some(ts) = action.get("@timestamp").cloned() {
            action["timestamp"] = ts;
        }
    }

    let perm_body = json!({
        "size": 100,
        "query": {"bool": {"filter": [{"term": {"owner": params.account}}]}},
    });
    let (permissions, _, _) = search_sources(&state, "perm", perm_body).await?;

    Ok(Json(json!({
        "query_time_ms": took,
        "account": params.account,
        "lib": state.lib().await,
        "tokens": tokens,
        "permissions": permissions,
        "total_actions": total["value"],
        "actions": actions,
    })))
}
