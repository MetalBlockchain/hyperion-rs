use super::{ApiResult, Shared};
use axum::extract::State;
use axum::Json;
use serde_json::json;
use std::time::Instant;

pub async fn health(State(state): State<Shared>) -> ApiResult {
    let mut services = Vec::new();

    let started = Instant::now();
    let es_status = match state.es.ping().await {
        Ok(info) => json!({
            "service": "Elasticsearch",
            "status": "OK",
            "service_data": {"version": info["version"]["number"]},
            "time": started.elapsed().as_millis() as u64,
        }),
        Err(e) => json!({
            "service": "Elasticsearch",
            "status": "Error",
            "service_data": {"error": e.to_string()},
            "time": started.elapsed().as_millis() as u64,
        }),
    };
    services.push(es_status);

    let started = Instant::now();
    let service = state.chain.service_name();
    let info = state.chain_info().await;
    let nodeos_status = if info.is_null() {
        json!({"service": service, "status": "Error", "time": started.elapsed().as_millis() as u64})
    } else {
        json!({
            "service": service,
            "status": "OK",
            "service_data": {
                "head_block_num": info["head_block_num"],
                "last_irreversible_block": info["last_irreversible_block_num"],
                "chain_id": info["chain_id"],
            },
            "time": started.elapsed().as_millis() as u64,
        })
    };
    services.push(nodeos_status);

    let last_indexed = state
        .es
        .max_block_num(&state.config.index("block"))
        .await
        .ok()
        .flatten()
        .unwrap_or(0);
    let head = info["head_block_num"].as_u64().unwrap_or(0);
    services.push(json!({
        "service": "Indexer",
        "status": if head > 0 && (head - (last_indexed as u64).min(head)) < 20 { "OK" } else { "Warning" },
        "service_data": {"last_indexed_block": last_indexed, "head_block_num": head},
    }));

    Ok(Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "chain": state.config.chain.name,
        "health": services,
    })))
}
