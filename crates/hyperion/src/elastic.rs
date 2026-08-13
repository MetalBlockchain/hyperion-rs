//! Thin Elasticsearch REST client over reqwest. Works against
//! Elasticsearch 7/8 and OpenSearch (only core APIs are used: index
//! creation, _bulk, _search, _count).

use crate::config::ElasticConfig;
use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct Elastic {
    http: reqwest::Client,
    base: String,
    user: String,
    pass: String,
}

impl Elastic {
    pub fn new(config: &ElasticConfig) -> Self {
        Elastic {
            http: reqwest::Client::new(),
            base: config.url.trim_end_matches('/').to_string(),
            user: config.user.clone(),
            pass: config.pass.clone(),
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let req = self.http.request(method, format!("{}{path}", self.base));
        if self.user.is_empty() {
            req
        } else {
            req.basic_auth(&self.user, Some(&self.pass))
        }
    }

    async fn json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value> {
        let mut req = self.request(method.clone(), path);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let res = req
            .send()
            .await
            .with_context(|| format!("elasticsearch {method} {path}"))?;
        let status = res.status();
        let value: Value = res.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(anyhow!(
                "elasticsearch {method} {path} failed ({status}): {value}"
            ));
        }
        Ok(value)
    }

    pub async fn ping(&self) -> Result<Value> {
        self.json(reqwest::Method::GET, "/", None).await
    }

    pub async fn index_exists(&self, index: &str) -> Result<bool> {
        let res = self
            .request(reqwest::Method::HEAD, &format!("/{index}"))
            .send()
            .await?;
        Ok(res.status().is_success())
    }

    pub async fn create_index(&self, index: &str, body: Value) -> Result<()> {
        if !self.index_exists(index).await? {
            self.json(reqwest::Method::PUT, &format!("/{index}"), Some(body))
                .await?;
            tracing::info!(index, "created index");
        }
        Ok(())
    }

    /// Submit an NDJSON `_bulk` body. Returns the number of failed items.
    pub async fn bulk(&self, body: String) -> Result<usize> {
        let res = self
            .request(reqwest::Method::POST, "/_bulk")
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .context("elasticsearch _bulk")?;
        let status = res.status();
        let value: Value = res.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(anyhow!("_bulk failed ({status}): {value}"));
        }
        if value["errors"].as_bool() != Some(true) {
            return Ok(0);
        }
        let mut failed = 0;
        if let Some(items) = value["items"].as_array() {
            for item in items {
                let op = item.as_object().and_then(|o| o.values().next());
                if let Some(op) = op {
                    let code = op["status"].as_u64().unwrap_or(0);
                    if code >= 300 {
                        failed += 1;
                        tracing::warn!(error = %op["error"], "bulk item failed");
                    }
                }
            }
        }
        Ok(failed)
    }

    pub async fn search(&self, index: &str, body: Value) -> Result<Value> {
        self.json(
            reqwest::Method::POST,
            &format!("/{index}/_search"),
            Some(body),
        )
        .await
    }

    /// Highest indexed block number in the given index, if any.
    pub async fn max_block_num(&self, index: &str) -> Result<Option<u32>> {
        if !self.index_exists(index).await? {
            return Ok(None);
        }
        let res = self
            .search(
                index,
                json!({"size": 0, "aggs": {"max_block": {"max": {"field": "block_num"}}}}),
            )
            .await?;
        Ok(res["aggregations"]["max_block"]["value"]
            .as_f64()
            .map(|v| v as u32))
    }
}

/// Index mappings, kept close to Hyperion's (subset of fields).
pub fn index_definitions(shards: u32, replicas: u32) -> Vec<(&'static str, Value)> {
    let settings = json!({
        "number_of_shards": shards,
        "number_of_replicas": replicas,
        "refresh_interval": "1s",
    });
    vec![
        (
            "action",
            json!({
                "settings": settings,
                "mappings": {"properties": {
                    "@timestamp": {"type": "date"},
                    "global_sequence": {"type": "long"},
                    "block_num": {"type": "long"},
                    "block_id": {"type": "keyword"},
                    "trx_id": {"type": "keyword"},
                    "producer": {"type": "keyword"},
                    "act.account": {"type": "keyword"},
                    "act.name": {"type": "keyword"},
                    "act.authorization.actor": {"type": "keyword"},
                    "act.authorization.permission": {"type": "keyword"},
                    // act.data field types vary per action (e.g. `owner` is a
                    // name in buyram but an authority object in newaccount), so
                    // it must not be indexed; it stays in _source. Searchable
                    // extracts of known actions live in @transfer/@newaccount.
                    "act.data": {"type": "object", "enabled": false},
                    "@transfer.from": {"type": "keyword"},
                    "@transfer.to": {"type": "keyword"},
                    "@transfer.amount": {"type": "double"},
                    "@transfer.symbol": {"type": "keyword"},
                    "@transfer.memo": {"type": "text"},
                    "@newaccount.newact": {"type": "keyword"},
                    "@newaccount.creator": {"type": "keyword"},
                    "act.hex_data": {"type": "keyword", "index": false, "doc_values": false},
                    "notified": {"type": "keyword"},
                    "receipts.receiver": {"type": "keyword"},
                    "receipts.global_sequence": {"type": "long"},
                    "cpu_usage_us": {"type": "integer"},
                    "net_usage_words": {"type": "integer"},
                    "action_ordinal": {"type": "integer"},
                    "creator_action_ordinal": {"type": "integer"},
                    "signatures": {"type": "keyword", "index": false},
                }}
            }),
        ),
        (
            "block",
            json!({
                "settings": settings,
                "mappings": {"properties": {
                    "@timestamp": {"type": "date"},
                    "block_num": {"type": "long"},
                    "block_id": {"type": "keyword"},
                    "prev_id": {"type": "keyword"},
                    "producer": {"type": "keyword"},
                    "schedule_version": {"type": "integer"},
                    "trx_count": {"type": "integer"},
                    "cpu_usage_us": {"type": "long"},
                    "net_usage_words": {"type": "long"},
                }}
            }),
        ),
        (
            "delta",
            json!({
                "settings": settings,
                "mappings": {"properties": {
                    "@timestamp": {"type": "date"},
                    "block_num": {"type": "long"},
                    "block_id": {"type": "keyword"},
                    "code": {"type": "keyword"},
                    "scope": {"type": "keyword"},
                    "table": {"type": "keyword"},
                    "primary_key": {"type": "keyword"},
                    "payer": {"type": "keyword"},
                    "present": {"type": "boolean"},
                    // Same per-contract type-conflict hazard as act.data.
                    "data": {"type": "object", "enabled": false},
                    "value_hex": {"type": "keyword", "index": false, "doc_values": false},
                }}
            }),
        ),
        (
            "abi",
            json!({
                "settings": settings,
                "mappings": {"properties": {
                    "@timestamp": {"type": "date"},
                    "block_num": {"type": "long"},
                    "account": {"type": "keyword"},
                    "abi": {"type": "keyword", "index": false, "doc_values": false},
                    "actions": {"type": "keyword"},
                    "tables": {"type": "keyword"},
                }}
            }),
        ),
        (
            "perm",
            json!({
                "settings": settings,
                "mappings": {"properties": {
                    "block_num": {"type": "long"},
                    "owner": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "parent": {"type": "keyword"},
                    "last_updated": {"type": "date"},
                    "keys": {"type": "keyword"},
                    "accounts": {"type": "keyword"},
                    "threshold": {"type": "integer"},
                }}
            }),
        ),
        (
            "token",
            json!({
                "settings": settings,
                "mappings": {"properties": {
                    "block_num": {"type": "long"},
                    "code": {"type": "keyword"},
                    "scope": {"type": "keyword"},
                    "symbol": {"type": "keyword"},
                    "precision": {"type": "integer"},
                    "amount": {"type": "double"},
                }}
            }),
        ),
    ]
}
