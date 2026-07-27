use super::{clamp_limit, range_filters, search_sources, ApiError, ApiResult, Shared};
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
}

fn filter_clause(filter: &str) -> Result<Value, ApiError> {
    let mut should = Vec::new();
    for pair in filter.split(',') {
        let (code, action) = pair
            .split_once(':')
            .ok_or_else(|| ApiError::bad_request(format!("bad filter entry: {pair}")))?;
        let mut must = Vec::new();
        if code != "*" {
            must.push(json!({"term": {"act.account": code}}));
        }
        if action != "*" {
            must.push(json!({"term": {"act.name": action}}));
        }
        should.push(json!({"bool": {"must": must}}));
    }
    Ok(json!({"bool": {"should": should, "minimum_should_match": 1}}))
}

pub async fn get_actions(
    State(state): State<Shared>,
    Query(params): Query<GetActionsParams>,
) -> ApiResult {
    let mut filters = Vec::new();
    if let Some(account) = &params.account {
        filters.push(json!({"term": {"notified": account}}));
    }
    if let Some(filter) = &params.filter {
        filters.push(filter_clause(filter)?);
    }
    filters.extend(range_filters(
        params.after.as_deref(),
        params.before.as_deref(),
    ));

    let sort_dir = match params.sort.as_deref() {
        None | Some("desc") => "desc",
        Some("asc") => "asc",
        Some(other) => return Err(ApiError::bad_request(format!("bad sort: {other}"))),
    };
    // track=false counts up to ES's 10k cap (cheap); track=true is exact.
    let track: Value = if params.track.as_deref() == Some("true") {
        json!(true)
    } else {
        json!(10000)
    };
    let body = json!({
        "track_total_hits": track,
        "from": params.skip.unwrap_or(0),
        "size": clamp_limit(&state, params.limit),
        "query": {"bool": {"filter": filters}},
        "sort": [{"global_sequence": {"order": sort_dir}}],
    });
    let (mut actions, total, took) = search_sources(&state, "action", body).await?;

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
    let body = json!({
        "size": 1000,
        "query": {"bool": {"filter": [{"term": {"trx_id": params.id.to_lowercase()}}]}},
        "sort": [{"global_sequence": {"order": "asc"}}],
    });
    let (mut actions, _, took) = search_sources(&state, "action", body).await?;
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
    let mut filters = Vec::new();
    for (field, value) in [
        ("code", &params.code),
        ("scope", &params.scope),
        ("table", &params.table),
        ("payer", &params.payer),
    ] {
        if let Some(value) = value {
            filters.push(json!({"term": {field: value}}));
        }
    }
    if let Some(present) = params.present {
        filters.push(json!({"term": {"present": present}}));
    }
    filters.extend(range_filters(
        params.after.as_deref(),
        params.before.as_deref(),
    ));

    let sort_dir = if params.sort.as_deref() == Some("asc") {
        "asc"
    } else {
        "desc"
    };
    let body = json!({
        "from": params.skip.unwrap_or(0),
        "size": clamp_limit(&state, params.limit),
        "query": {"bool": {"filter": filters}},
        "sort": [{"block_num": {"order": sort_dir}}],
    });
    let (deltas, total, took) = search_sources(&state, "delta", body).await?;
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
    let mut filters = vec![json!({"term": {"account": params.contract}})];
    if let Some(block) = params.block {
        filters.push(json!({"range": {"block_num": {"lte": block}}}));
    }
    let body = json!({
        "size": 1,
        "query": {"bool": {"filter": filters}},
        "sort": [{"block_num": {"order": "desc"}}],
    });
    let (hits, _, took) = search_sources(&state, "abi", body).await?;
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
    let body = json!({
        "size": 100,
        "query": {"bool": {"filter": [
            {"term": {"act.name": "newaccount"}},
            {"term": {"act.data.creator.keyword": params.account}},
        ]}},
        "sort": [{"global_sequence": {"order": "desc"}}],
    });
    let (hits, _, took) = search_sources(&state, "action", body).await?;
    let accounts: Vec<Value> = hits
        .iter()
        .map(|a| {
            let name = a["act"]["data"]["newact"]
                .as_str()
                .or_else(|| a["act"]["data"]["name"].as_str());
            json!({
                "name": name,
                "timestamp": a["@timestamp"],
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
    let body = json!({
        "size": 1,
        "query": {"bool": {
            "filter": [{"term": {"act.name": "newaccount"}}],
            "should": [
                {"term": {"act.data.newact.keyword": params.account}},
                {"term": {"act.data.name.keyword": params.account}},
            ],
            "minimum_should_match": 1,
        }},
        "sort": [{"global_sequence": {"order": "asc"}}],
    });
    let (hits, _, took) = search_sources(&state, "action", body).await?;
    match hits.first() {
        Some(a) => Ok(Json(json!({
            "query_time_ms": took,
            "account": params.account,
            "creator": a["act"]["data"]["creator"],
            "timestamp": a["@timestamp"],
            "block_num": a["block_num"],
            "trx_id": a["trx_id"],
        }))),
        None => Err(ApiError(
            axum::http::StatusCode::NOT_FOUND,
            "account creation not found".into(),
        )),
    }
}
