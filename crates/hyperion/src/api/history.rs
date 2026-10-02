use super::{clamp_limit, parse_block_bound, query_rows, ApiError, ApiResult, Shared};
use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub struct GetActionsParams {
    pub account: Option<String>,
    /// `code:action` pairs, comma-separated; `*` wildcards allowed on
    /// either side.
    pub filter: Option<String>,
    pub track: Option<String>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
    /// `asc` | `desc` (default `desc`).
    pub sort: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub simple: Option<bool>,
    /// Filters on the indexed `@transfer` extract (Hyperion-compatible).
    #[serde(rename = "transfer.from")]
    pub transfer_from: Option<String>,
    #[serde(rename = "transfer.to")]
    pub transfer_to: Option<String>,
    #[serde(rename = "transfer.symbol")]
    pub transfer_symbol: Option<String>,
    #[serde(rename = "transfer.memo")]
    pub transfer_memo: Option<String>,
}

pub async fn get_actions(
    State(state): State<Shared>,
    Query(params): Query<GetActionsParams>,
) -> ApiResult {
    let sort_dir = match params.sort.as_deref() {
        None | Some("desc") => "desc",
        Some("asc") => "asc",
        Some(other) => return Err(ApiError::bad_request(format!("bad sort: {other}"))),
    };
    let after = params.after.as_deref().map(parse_block_bound).transpose()?;
    let before = params
        .before
        .as_deref()
        .map(parse_block_bound)
        .transpose()?;
    let skip = params.skip.unwrap_or(0);
    let limit = clamp_limit(&state, params.limit);

    let sql = crate::clickhouse::build_get_actions_query(
        params.account.as_deref(),
        params.filter.as_deref(),
        skip,
        limit,
        sort_dir,
        after,
        before,
        params.transfer_from.as_deref(),
        params.transfer_to.as_deref(),
        params.transfer_symbol.as_deref(),
        params.transfer_memo.as_deref(),
    )
    .map_err(ApiError::internal)?;
    let (rows, took) = query_rows(&state, &sql).await?;
    let mut actions: Vec<Value> = rows.iter().map(crate::clickhouse::action_doc).collect();

    let total = if params.track.as_deref() == Some("true") {
        let count_sql = crate::clickhouse::build_get_actions_with_count_query(
            params.account.as_deref(),
            params.filter.as_deref(),
            params.transfer_from.as_deref(),
            params.transfer_to.as_deref(),
            params.transfer_symbol.as_deref(),
            params.transfer_memo.as_deref(),
        )
        .map_err(ApiError::internal)?;
        let (count_rows, _) = query_rows(&state, &count_sql).await?;
        count_rows
            .first()
            .map(|r| r["total"].clone())
            .unwrap_or(json!(0))
    } else {
        json!(actions.len())
    };

    for action in &mut actions {
        if let Some(ts) = action.get("@timestamp").cloned() {
            action["timestamp"] = ts;
        }
    }
    if params.simple.unwrap_or(false) {
        actions = actions
            .iter()
            .map(|a| {
                json!({
                    "block": a["block_num"],
                    "timestamp": a["timestamp"],
                    "transaction_id": a["trx_id"],
                    "contract": a["act"]["account"],
                    "action": a["act"]["name"],
                    "actors": a["act"]["authorization"],
                    "notified": a["notified"],
                    "data": a["act"]["data"],
                })
            })
            .collect();
    }

    Ok(Json(json!({
        "query_time_ms": took,
        "cached": false,
        "lib": state.lib().await,
        "total": total,
        (if params.simple.unwrap_or(false) { "simple_actions" } else { "actions" }): actions,
    })))
}

#[derive(Debug, Deserialize)]
pub struct GetTransactionParams {
    pub id: String,
}

pub async fn get_transaction(
    State(state): State<Shared>,
    Query(params): Query<GetTransactionParams>,
) -> ApiResult {
    let sql = crate::clickhouse::build_get_transaction_query(&params.id.to_lowercase())
        .map_err(ApiError::internal)?;
    let (rows, took) = query_rows(&state, &sql).await?;
    let mut actions: Vec<Value> = rows.iter().map(crate::clickhouse::action_doc).collect();
    for action in &mut actions {
        if let Some(ts) = action.get("@timestamp").cloned() {
            action["timestamp"] = ts;
        }
    }
    Ok(Json(json!({
        "query_time_ms": took,
        "executed": !actions.is_empty(),
        "trx_id": params.id,
        "lib": state.lib().await,
        "actions": actions,
    })))
}

#[derive(Debug, Deserialize)]
pub struct GetDeltasParams {
    pub code: Option<String>,
    pub scope: Option<String>,
    pub table: Option<String>,
    pub payer: Option<String>,
    pub present: Option<bool>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
    pub sort: Option<String>,
}

pub async fn get_deltas(
    State(state): State<Shared>,
    Query(params): Query<GetDeltasParams>,
) -> ApiResult {
    let sort_dir = if params.sort.as_deref() == Some("asc") {
        "asc"
    } else {
        "desc"
    };
    let after = params.after.as_deref().map(parse_block_bound).transpose()?;
    let before = params
        .before
        .as_deref()
        .map(parse_block_bound)
        .transpose()?;
    let skip = params.skip.unwrap_or(0);
    let limit = clamp_limit(&state, params.limit);

    let sql = crate::clickhouse::build_get_deltas_query(
        params.code.as_deref(),
        params.scope.as_deref(),
        params.table.as_deref(),
        params.payer.as_deref(),
        params.present,
        after,
        before,
        skip,
        limit,
        sort_dir,
    )
    .map_err(ApiError::internal)?;
    let count_sql = crate::clickhouse::build_get_deltas_count_query(
        params.code.as_deref(),
        params.scope.as_deref(),
        params.table.as_deref(),
        params.payer.as_deref(),
        params.present,
        after,
        before,
    )
    .map_err(ApiError::internal)?;
    let (deltas, took) = query_rows(&state, &sql).await?;
    let (count_rows, _) = query_rows(&state, &count_sql).await?;
    let total = count_rows
        .first()
        .map(|r| r["total"].clone())
        .unwrap_or(json!(0));
    Ok(Json(json!({
        "query_time_ms": took,
        "total": total,
        "deltas": deltas,
    })))
}

#[derive(Debug, Deserialize)]
pub struct AbiSnapshotParams {
    pub contract: String,
    pub block: Option<u64>,
}

pub async fn get_abi_snapshot(
    State(state): State<Shared>,
    Query(params): Query<AbiSnapshotParams>,
) -> ApiResult {
    let sql = crate::clickhouse::build_get_abi_snapshot_query(&params.contract, params.block)
        .map_err(ApiError::internal)?;
    let (hits, took) = query_rows(&state, &sql).await?;
    match hits.first() {
        Some(doc) => {
            let abi: Value = doc["abi"]
                .as_str()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(Value::Null);
            Ok(Json(json!({
                "query_time_ms": took,
                "present": true,
                "block_num": doc["block_num"],
                "abi": abi,
            })))
        }
        None => Ok(Json(json!({"query_time_ms": took, "present": false}))),
    }
}

#[derive(Debug, Deserialize)]
pub struct AccountParam {
    pub account: String,
}

pub async fn get_created_accounts(
    State(state): State<Shared>,
    Query(params): Query<AccountParam>,
) -> ApiResult {
    let sql = crate::clickhouse::build_get_created_accounts_query(&params.account)
        .map_err(ApiError::internal)?;
    let (hits, took) = query_rows(&state, &sql).await?;
    let accounts: Vec<Value> = hits
        .iter()
        .map(|a| {
            json!({
                "name": a["newaccount_newact"],
                "timestamp": a["timestamp"],
                "trx_id": a["trx_id"],
            })
        })
        .collect();
    Ok(Json(json!({"query_time_ms": took, "accounts": accounts})))
}

pub async fn get_creator(
    State(state): State<Shared>,
    Query(params): Query<AccountParam>,
) -> ApiResult {
    let sql =
        crate::clickhouse::build_get_creator_query(&params.account).map_err(ApiError::internal)?;
    let (hits, took) = query_rows(&state, &sql).await?;
    match hits.first() {
        Some(a) => Ok(Json(json!({
            "query_time_ms": took,
            "account": params.account,
            "creator": a["newaccount_creator"],
            "timestamp": a["timestamp"],
            "block_num": a["block_num"],
            "trx_id": a["trx_id"],
        }))),
        None => Err(ApiError(
            axum::http::StatusCode::NOT_FOUND,
            "account creation not found".into(),
        )),
    }
}
