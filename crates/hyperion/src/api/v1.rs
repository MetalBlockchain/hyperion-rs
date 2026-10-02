//! Minimal nodeos history-plugin (v1) compatibility layer, translating onto
//! the same ClickHouse tables as the v2 API.

use super::{query_rows, ApiError, ApiResult, Shared};
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
    let sql = crate::clickhouse::build_get_actions_query(
        Some(&params.account_name),
        None,
        from,
        size,
        order,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .map_err(ApiError::internal)?;
    let (rows, _) = query_rows(&state, &sql).await?;
    let mut hits: Vec<Value> = rows.iter().map(crate::clickhouse::action_doc).collect();
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
    let sql = crate::clickhouse::build_get_transaction_query(&params.id.to_lowercase())
        .map_err(ApiError::internal)?;
    let (rows, _) = query_rows(&state, &sql).await?;
    let hits: Vec<Value> = rows.iter().map(crate::clickhouse::action_doc).collect();
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
    let sql = crate::clickhouse::build_get_key_accounts_query(&key, 0, 1000)
        .map_err(ApiError::internal)?;
    let (hits, _) = query_rows(&state, &sql).await?;
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
    let sql = crate::clickhouse::build_get_controlled_accounts_query(&params.controlling_account)
        .map_err(ApiError::internal)?;
    let (hits, _) = query_rows(&state, &sql).await?;
    let mut account_names: Vec<&str> = hits.iter().filter_map(|p| p["owner"].as_str()).collect();
    account_names.dedup();
    Ok(Json(json!({"controlled_accounts": account_names})))
}
