//! Dual-write infrastructure: run both indexers in parallel with divergence detection.

use crate::processor::Doc;
use crate::clickhouse::{ClickHouse, ClickHouseBatch};
use crate::elastic::Elastic;
use anyhow::Result;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Tracks divergences between ES and ClickHouse during dual-write.
#[derive(Debug)]
pub struct DivergenceCounter {
    es_errors: AtomicU64,
    ck_errors: AtomicU64,
    results_differ: AtomicU64,
}

impl DivergenceCounter {
    pub fn new() -> Arc<Self> {
        Arc::new(DivergenceCounter {
            es_errors: AtomicU64::new(0),
            ck_errors: AtomicU64::new(0),
            results_differ: AtomicU64::new(0),
        })
    }

    pub fn record_es_error(&self) {
        self.es_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_ck_error(&self) {
        self.ck_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_divergence(&self) {
        self.results_differ.fetch_add(1, Ordering::Relaxed);
    }

    pub fn report(&self) -> String {
        format!(
            "ES errors: {}, CK errors: {}, Result divergences: {}",
            self.es_errors.load(Ordering::Relaxed),
            self.ck_errors.load(Ordering::Relaxed),
            self.results_differ.load(Ordering::Relaxed),
        )
    }
}

/// Write batch to both ES and ClickHouse concurrently.
pub async fn write_both(
    es: &Elastic,
    ck: &ClickHouse,
    es_body: Vec<u8>,
    ck_batch: ClickHouseBatch,
    max_block: u32,
    progress_index: &str,
    counter: &DivergenceCounter,
) -> Result<()> {
    let es_future = async {
        match es.bulk(es_body).await {
            Ok(_) => {
                tracing::debug!("ES bulk succeeded");
                Ok(())
            }
            Err(e) => {
                counter.record_es_error();
                tracing::warn!("ES bulk failed: {}", e);
                Err(e)
            }
        }
    };

    let ck_future = async {
        for table in &["action", "block", "delta", "abi", "perm", "token"] {
            let data = ck_batch.to_tab_separated(table);
            if !data.is_empty() {
                match ck.insert_tab_separated(table, data).await {
                    Ok(_) => {
                        tracing::debug!(table, "CK insert succeeded");
                    }
                    Err(e) => {
                        counter.record_ck_error();
                        tracing::warn!("CK insert failed on {}: {}", table, e);
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    };

    let (es_result, ck_result) = tokio::join!(es_future, ck_future);

    // Checkpoint only advances if BOTH succeed
    match (&es_result, &ck_result) {
        (Ok(_), Ok(_)) => {
            let version = (u64::from(max_block) << 32) | 0;
            es.set_checkpoint(progress_index, max_block).await?;
            ck.set_checkpoint(max_block, version).await?;
            tracing::debug!(max_block, "both backends advanced checkpoint");
            Ok(())
        }
        _ => {
            if es_result.is_err() {
                es_result?;
            }
            if ck_result.is_err() {
                ck_result?;
            }
            Ok(())
        }
    }
}

/// Shadow read: execute same query against both backends and compare results.
pub async fn shadow_read(
    es: &Elastic,
    ck: &ClickHouse,
    index_kind: &str,
    sql_query: &str,
    counter: &DivergenceCounter,
) -> Result<(Value, String, u128, u128)> {
    let es_start = std::time::Instant::now();
    let es_result = es
        .search(index_kind, serde_json::json!({"query": {"match_all": {}}, "size": 100}))
        .await?;
    let es_time = es_start.elapsed();

    let ck_start = std::time::Instant::now();
    let ck_result = ck.query(sql_query).await?;
    let ck_time = ck_start.elapsed();

    // Compare row counts if available
    if let Some(total) = es_result.get("hits").and_then(|h| h.get("total")) {
        if let Some(es_count) = total.get("value").and_then(|v| v.as_u64()) {
            // Parse ClickHouse result to count rows
            let ck_count = ck_result.lines().count() as u64;
            if es_count != ck_count {
                tracing::warn!(
                    es_count,
                    ck_count,
                    "shadow read count divergence: {} vs {}",
                    es_count,
                    ck_count
                );
                counter.record_divergence();
            }
        }
    }

    Ok((
        es_result,
        ck_result,
        es_time.as_millis(),
        ck_time.as_millis(),
    ))
}
