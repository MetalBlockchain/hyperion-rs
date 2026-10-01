use super::{query_rows, ApiResult, Shared};
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
    let sql = crate::clickhouse::build_get_key_accounts_query(&key, 0, 1000)
        .map_err(super::ApiError::internal)?;
    let (hits, took) = query_rows(&state, &sql).await?;
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
    let sql = crate::clickhouse::build_get_tokens_query(Some(account), None, 0, 1000)
        .map_err(super::ApiError::internal)?;
    let (hits, _) = query_rows(state, &sql).await?;
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

    let actions_sql = crate::clickhouse::build_get_actions_query(
        Some(&params.account),
        None,
        0,
        20,
        "desc",
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .map_err(super::ApiError::internal)?;
    let count_sql = crate::clickhouse::build_get_actions_with_count_query(
        Some(&params.account),
        None,
        None,
        None,
        None,
        None,
    )
    .map_err(super::ApiError::internal)?;
    let (action_rows, took) = query_rows(&state, &actions_sql).await?;
    let (count_rows, _) = query_rows(&state, &count_sql).await?;
    let total = count_rows
        .first()
        .map(|r| r["total"].clone())
        .unwrap_or(json!(0));
    let mut actions: Vec<Value> = action_rows
        .iter()
        .map(crate::clickhouse::action_doc)
        .collect();
    for action in &mut actions {
        if let Some(ts) = action.get("@timestamp").cloned() {
            action["timestamp"] = ts;
        }
    }

    let perm_sql = crate::clickhouse::build_get_account_query(&params.account)
        .map_err(super::ApiError::internal)?;
    let (permissions, _) = query_rows(&state, &perm_sql).await?;

    Ok(Json(json!({
        "query_time_ms": took,
        "account": params.account,
        "lib": state.lib().await,
        "tokens": tokens,
        "permissions": permissions,
        "total_actions": total,
        "actions": actions,
    })))
}
