//! Prometheus metrics for ClickHouse indexing and queries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Prometheus-compatible metrics collector.
pub struct Metrics {
    // Ingestion metrics
    pub ch_rows_inserted: Arc<AtomicU64>,
    pub es_rows_inserted: Arc<AtomicU64>,
    pub batch_inserts_total: Arc<AtomicU64>,
    pub batch_insert_errors: Arc<AtomicU64>,

    // Latency metrics (milliseconds)
    pub ch_insert_latency_ms: Arc<AtomicU64>,
    pub es_insert_latency_ms: Arc<AtomicU64>,

    // Query metrics
    pub queries_total: Arc<AtomicU64>,
    pub query_latency_ms: Arc<AtomicU64>,

    // Memory (bytes)
    pub memory_bytes: Arc<AtomicU64>,

    // Checkpoint
    pub checkpoint_block: Arc<AtomicU64>,

    // Divergence
    pub divergences_total: Arc<AtomicU64>,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            ch_rows_inserted: Arc::new(AtomicU64::new(0)),
            es_rows_inserted: Arc::new(AtomicU64::new(0)),
            batch_inserts_total: Arc::new(AtomicU64::new(0)),
            batch_insert_errors: Arc::new(AtomicU64::new(0)),
            ch_insert_latency_ms: Arc::new(AtomicU64::new(0)),
            es_insert_latency_ms: Arc::new(AtomicU64::new(0)),
            queries_total: Arc::new(AtomicU64::new(0)),
            query_latency_ms: Arc::new(AtomicU64::new(0)),
            memory_bytes: Arc::new(AtomicU64::new(0)),
            checkpoint_block: Arc::new(AtomicU64::new(0)),
            divergences_total: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_ch_insert(&self, rows: u64, latency_ms: u64) {
        self.ch_rows_inserted.fetch_add(rows, Ordering::Relaxed);
        self.ch_insert_latency_ms.store(latency_ms, Ordering::Relaxed);
        self.batch_inserts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_es_insert(&self, rows: u64, latency_ms: u64) {
        self.es_rows_inserted.fetch_add(rows, Ordering::Relaxed);
        self.es_insert_latency_ms.store(latency_ms, Ordering::Relaxed);
    }

    pub fn record_insert_error(&self) {
        self.batch_insert_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query(&self, latency_ms: u64) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.query_latency_ms.store(latency_ms, Ordering::Relaxed);
    }

    pub fn set_checkpoint(&self, block_num: u32) {
        self.checkpoint_block.store(block_num as u64, Ordering::Relaxed);
    }

    pub fn record_divergence(&self) {
        self.divergences_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_memory(&self, bytes: u64) {
        self.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Export metrics in Prometheus text format.
    pub fn export(&self) -> String {
        format!(
            "# HELP hyperion_ch_rows_inserted_total Total rows inserted to ClickHouse\n\
             # TYPE hyperion_ch_rows_inserted_total counter\n\
             hyperion_ch_rows_inserted_total {}\n\
             \n\
             # HELP hyperion_es_rows_inserted_total Total rows inserted to Elasticsearch\n\
             # TYPE hyperion_es_rows_inserted_total counter\n\
             hyperion_es_rows_inserted_total {}\n\
             \n\
             # HELP hyperion_batch_inserts_total Total batch insert operations\n\
             # TYPE hyperion_batch_inserts_total counter\n\
             hyperion_batch_inserts_total {}\n\
             \n\
             # HELP hyperion_batch_insert_errors_total Total insert errors\n\
             # TYPE hyperion_batch_insert_errors_total counter\n\
             hyperion_batch_insert_errors_total {}\n\
             \n\
             # HELP hyperion_ch_insert_latency_ms Last ClickHouse insert latency\n\
             # TYPE hyperion_ch_insert_latency_ms gauge\n\
             hyperion_ch_insert_latency_ms {}\n\
             \n\
             # HELP hyperion_es_insert_latency_ms Last Elasticsearch insert latency\n\
             # TYPE hyperion_es_insert_latency_ms gauge\n\
             hyperion_es_insert_latency_ms {}\n\
             \n\
             # HELP hyperion_queries_total Total API queries\n\
             # TYPE hyperion_queries_total counter\n\
             hyperion_queries_total {}\n\
             \n\
             # HELP hyperion_query_latency_ms Last query latency\n\
             # TYPE hyperion_query_latency_ms gauge\n\
             hyperion_query_latency_ms {}\n\
             \n\
             # HELP hyperion_checkpoint_block Current checkpoint block number\n\
             # TYPE hyperion_checkpoint_block gauge\n\
             hyperion_checkpoint_block {}\n\
             \n\
             # HELP hyperion_divergences_total Total ES vs CH divergences detected\n\
             # TYPE hyperion_divergences_total counter\n\
             hyperion_divergences_total {}\n\
             \n\
             # HELP hyperion_memory_bytes Process memory usage\n\
             # TYPE hyperion_memory_bytes gauge\n\
             hyperion_memory_bytes {}\n",
            self.ch_rows_inserted.load(Ordering::Relaxed),
            self.es_rows_inserted.load(Ordering::Relaxed),
            self.batch_inserts_total.load(Ordering::Relaxed),
            self.batch_insert_errors.load(Ordering::Relaxed),
            self.ch_insert_latency_ms.load(Ordering::Relaxed),
            self.es_insert_latency_ms.load(Ordering::Relaxed),
            self.queries_total.load(Ordering::Relaxed),
            self.query_latency_ms.load(Ordering::Relaxed),
            self.checkpoint_block.load(Ordering::Relaxed),
            self.divergences_total.load(Ordering::Relaxed),
            self.memory_bytes.load(Ordering::Relaxed),
        )
    }
}

/// Timer for recording latency metrics.
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Timer {
            start: Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_export() {
        let m = Metrics::new();
        m.record_ch_insert(100, 50);
        m.set_checkpoint(12345);
        let export = m.export();
        assert!(export.contains("hyperion_ch_rows_inserted_total 100"));
        assert!(export.contains("hyperion_checkpoint_block 12345"));
    }

    #[test]
    fn test_timer() {
        let t = Timer::start();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(t.elapsed_ms() >= 10);
    }
}
