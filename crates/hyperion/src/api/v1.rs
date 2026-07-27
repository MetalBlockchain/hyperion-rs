//! Minimal nodeos history-plugin (v1) compatibility layer, translating onto
//! the same Elasticsearch indices as the v2 API.

use super::{search_sources, ApiError, ApiResult, Shared};
use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub struct V1GetActions {
    pub account_name: String,
    /// Absolute sequence to start from; -1 = tail of history.
    pub pos: Option<i64>,
    /// Count (negative with pos=-1 means "last N").
    pub offset: Option<i64>,
}

pub async fn get_actions(
    State(state): State<Shared>,
    Json(params): Json<V1GetActions>,
) -> ApiResult {
    let pos = params.pos.unwrap_or(-1);
    let offset = params.offset.unwrap_or(-20);
    let (from, size, order) = if pos < 0 {
        (0, offset.unsigned_abs().min(1000) as usize, "desc")
    } else {
        (pos as usize, offset.clamp(1, 1000) as usize, "asc")
    };
    let body = json!({
        "from": from,
        "size": size,
        "query": {"bool": {"filter": [{"term": {"notified": params.account_name}}]}},
        "sort": [{"global_sequence": {"order": order}}],
    });
    let (mut hits, _, _) = search_sources(&state, "action", body).await?;
    if order == "desc" {
        hits.reverse();
    }
    let actions: Vec<Value> = hits
        .iter()
        .enumerate()
        .map(|(i, a)| {
            json!({
                "global_action_seq": a["global_sequence"],
                "account_action_seq": from + i,
                "block_num": a["block_num"],
                "block_time": a["@timestamp"],
                "action_trace": {
                    "receipt": a["receipts"][0],
                    "act": a["act"],
                    "trx_id": a["trx_id"],
                    "block_num": a["block_num"],
                    "block_time": a["@timestamp"],
                    "producer_block_id": a["block_id"],
                },
            })
        })
        .collect();
    Ok(Json(json!({
        "actions": actions,
        "last_irreversible_block": state.lib().await,
    })))
}

#[derive(Debug, Deserialize)]
pub struct V1GetTransaction {
    pub id: String,
}

pub async fn get_transaction(
    State(state): State<Shared>,
    Json(params): Json<V1GetTransaction>,
) -> ApiResult {
    let body = json!({
        "size": 1000,
        "query": {"bool": {"filter": [{"term": {"trx_id": params.id.to_lowercase()}}]}},
        "sort": [{"global_sequence": {"order": "asc"}}],
    });
    let (hits, _, _) = search_sources(&state, "action", body).await?;
    if hits.is_empty() {
        return Err(ApiError(
            axum::http::StatusCode::NOT_FOUND,
            format!("transaction {} not found", params.id),
        ));
    }
    let first = &hits[0];
    let traces: Vec<Value> = hits
        .iter()
        .map(|a| {
            json!({
                "receipt": a["receipts"][0],
                "act": a["act"],
                "block_num": a["block_num"],
                "block_time": a["@timestamp"],
                "trx_id": a["trx_id"],
            })
        })
        .collect();
    Ok(Json(json!({
        "id": params.id,
        "block_num": first["block_num"],
        "block_time": first["@timestamp"],
        "last_irreversible_block": state.lib().await,
        "traces": traces,
        "trx": {"trx": {"actions": hits.iter().map(|a| a["act"].clone()).collect::<Vec<_>>()}},
    })))
}

#[derive(Debug, Deserialize)]
pub struct V1GetKeyAccounts {
    pub public_key: String,
}

pub async fn get_key_accounts(
    State(state): State<Shared>,
    Json(params): Json<V1GetKeyAccounts>,
) -> ApiResult {
    let key = antelope::keys::normalize_public_key(params.public_key.trim());
    let body = json!({
        "size": 1000,
        "query": {"bool": {"filter": [{"term": {"keys": key}}]}},
        "sort": [{"owner": {"order": "asc"}}],
    });
    let (hits, _, _) = search_sources(&state, "perm", body).await?;
    let mut account_names: Vec<&str> = hits.iter().filter_map(|p| p["owner"].as_str()).collect();
    account_names.dedup();
    Ok(Json(json!({"account_names": account_names})))
}

#[derive(Debug, Deserialize)]
pub struct V1GetControlledAccounts {
    pub controlling_account: String,
}

pub async fn get_controlled_accounts(
    State(state): State<Shared>,
    Json(params): Json<V1GetControlledAccounts>,
) -> ApiResult {
    // `accounts` entries are stored as `actor@permission`.
    let body = json!({
        "size": 1000,
        "query": {"bool": {"filter": [
            {"prefix": {"accounts": format!("{}@", params.controlling_account)}}
        ]}},
        "sort": [{"owner": {"order": "asc"}}],
    });
    let (hits, _, _) = search_sources(&state, "perm", body).await?;
    let mut account_names: Vec<&str> = hits.iter().filter_map(|p| p["owner"].as_str()).collect();
    account_names.dedup();
    Ok(Json(json!({"controlled_accounts": account_names})))
}
