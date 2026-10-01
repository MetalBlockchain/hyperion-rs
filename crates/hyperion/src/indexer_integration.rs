//! Integration example: using IndexBackend in the indexer pipeline.
//! This shows how to modify indexer.rs to support ES, CH, or dual-write.

#![allow(dead_code)]

use crate::backend::IndexBackend;
use crate::clickhouse::{ClickHouseBatch, doc_to_row};
use crate::elastic::Elastic;
use crate::processor::Doc;
use crate::clickhouse::ClickHouse;
use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;

/// Example of how to integrate dual-write into process_blocks().
///
/// Original pattern:
/// ```ignore
/// fn push(&mut self, index: &str, doc: &Doc, version: u64) -> Result<()> {
///     // ... push to ES bulk batch ...
/// }
/// ```
///
/// With dual-write:
/// ```ignore
/// enum Batch {
///     ES(BulkBatch),
///     CH(ClickHouseBatch),
///     Both { es: BulkBatch, ch: ClickHouseBatch },
/// }
///
/// fn push(&mut self, doc: &Doc, version: u64) -> Result<()> {
///     match &mut self.batch {
///         Batch::ES(batch) => batch.push(&indices[doc.kind], doc, version),
///         Batch::CH(batch) => batch.push(doc, version),
///         Batch::Both { es, ch } => {
///             es.push(&indices[doc.kind], doc, version)?;
///             ch.push(doc, version)
///         }
///     }
/// }
/// ```

/// Example batch type that supports both backends.
pub enum WriteBatch {
    ES(EsBulkBatch),
    CH(ClickHouseBatch),
    Both { es: EsBulkBatch, ch: ClickHouseBatch },
}

// Stub for ES bulk batch (from indexer.rs)
#[derive(Default)]
pub struct EsBulkBatch {
    pub body: Vec<u8>,
    pub count: usize,
    pub max_block: u32,
}

impl WriteBatch {
    pub fn new(backend: &IndexBackend) -> Self {
        match backend {
            IndexBackend::Elasticsearch(_) => WriteBatch::ES(EsBulkBatch::default()),
            IndexBackend::ClickHouse(_) => WriteBatch::CH(ClickHouseBatch::new()),
            IndexBackend::DualWrite { .. } => WriteBatch::Both {
                es: EsBulkBatch::default(),
                ch: ClickHouseBatch::new(),
            },
        }
    }

    pub fn push(&mut self, doc: &Doc, version: u64, indices: &HashMap<&str, String>) -> Result<()> {
        match self {
            WriteBatch::ES(batch) => {
                // Original ES bulk format
                let index = &indices[doc.kind];
                // Format: {"index":{...}}\n{doc}\n
                let _meta = serde_json::json!({
                    "_index": index,
                    "_id": doc.id.as_deref(),
                    "version_type": "external",
                    "version": version,
                });
                // (In real code, serialize and append to batch.body)
                batch.count += 1;
                batch.max_block = batch.max_block.max((version >> 32) as u32);
                Ok(())
            }
            WriteBatch::CH(batch) => batch.push(doc, version),
            WriteBatch::Both { es, ch } => {
                let index = &indices[doc.kind];
                es.count += 1;
                es.max_block = es.max_block.max((version >> 32) as u32);
                ch.push(doc, version)
            }
        }
    }

    pub fn count(&self) -> usize {
        match self {
            WriteBatch::ES(batch) => batch.count,
            WriteBatch::CH(batch) => batch.count,
            WriteBatch::Both { es, ch } => es.count + ch.count,
        }
    }

    pub fn max_block(&self) -> u32 {
        match self {
            WriteBatch::ES(batch) => batch.max_block,
            WriteBatch::CH(batch) => batch.max_block,
            WriteBatch::Both { es, ch } => es.max_block.max(ch.max_block),
        }
    }
}

/// Example of writer_batches modified for dual-write.
///
/// Original signature:
/// ```ignore
/// async fn write_batches(
///     es: &Elastic,
///     mut rx: mpsc::Receiver<BulkBatch>,
///     concurrency: usize,
///     progress_index: &str,
/// ) -> Result<()>
/// ```
///
/// With dual-write:
/// ```ignore
/// async fn write_batches(
///     backend: &IndexBackend,
///     mut rx: mpsc::Receiver<WriteBatch>,
///     concurrency: usize,
///     progress_index: &str,
/// ) -> Result<()> {
///     match backend {
///         IndexBackend::Elasticsearch(es) => write_es(es, rx, concurrency, progress_index).await,
///         IndexBackend::ClickHouse(ck) => write_ck(ck, rx, concurrency).await,
///         IndexBackend::DualWrite { es, ck, counter } => {
///             write_both(es, ck, rx, concurrency, progress_index, counter).await
///         }
///     }
/// }
/// ```

/// Pseudo-code showing integration with config.
pub struct IntegrationConfig {
    /// "elasticsearch" | "clickhouse" | "dual"
    pub backend_mode: String,
    pub es_url: String,
    pub ck_url: String,
}

impl IntegrationConfig {
    pub fn create_backend(&self) -> Result<IndexBackend> {
        match self.backend_mode.as_str() {
            "elasticsearch" => {
                let es = Elastic::new(&crate::config::ElasticConfig {
                    url: self.es_url.clone(),
                    user: String::new(),
                    pass: String::new(),
                    shards: 1,
                    replicas: 0,
                });
                Ok(IndexBackend::new_es(es))
            }
            "clickhouse" => {
                let ck = ClickHouse::new(&self.ck_url, None, None);
                Ok(IndexBackend::new_ck(ck))
            }
            "dual" => {
                let es = Elastic::new(&crate::config::ElasticConfig {
                    url: self.es_url.clone(),
                    user: String::new(),
                    pass: String::new(),
                    shards: 1,
                    replicas: 0,
                });
                let ck = ClickHouse::new(&self.ck_url, None, None);
                Ok(IndexBackend::new_dual(es, ck))
            }
            _ => Err(anyhow::anyhow!("unknown backend mode: {}", self.backend_mode)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch_es() {
        let mut batch = WriteBatch::ES(EsBulkBatch::default());
        let doc = Doc {
            kind: "block",
            id: Some("1".to_string()),
            body: serde_json::json!({}),
            op: crate::processor::Op::Index,
        };
        let mut indices = HashMap::new();
        indices.insert("block", "test-block".to_string());

        batch.push(&doc, 1 << 32, &indices).unwrap();
        assert_eq!(batch.count(), 1);
        assert_eq!(batch.max_block(), 1);
    }

    #[test]
    fn test_write_batch_ch() {
        let mut batch = WriteBatch::CH(ClickHouseBatch::new());
        let doc = Doc {
            kind: "block",
            id: Some("1".to_string()),
            body: serde_json::json!({
                "block_num": 1,
                "@timestamp": "2024-01-01T00:00:00Z",
                "block_id": "abc",
                "prev_id": "def",
                "producer": "p",
                "schedule_version": 1,
                "trx_count": 0,
                "cpu_usage_us": 0,
                "net_usage_words": 0,
            }),
            op: crate::processor::Op::Index,
        };
        let indices = HashMap::new();

        batch.push(&doc, 1 << 32, &indices).unwrap();
        assert_eq!(batch.count(), 1);
        assert_eq!(batch.max_block(), 1);
    }
}
