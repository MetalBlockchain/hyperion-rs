use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub chain: ChainConfig,
    #[serde(default)]
    pub indexer: IndexerConfig,
    #[serde(default)]
    pub elasticsearch: ElasticConfig,
    #[serde(default)]
    pub api: ApiConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    /// Short chain name, used as the index prefix (e.g. `wax`, `eos`).
    pub name: String,
    /// Chain API endpoint (nodeos REST or PulseVM JSON-RPC, see `api`).
    pub http: String,
    /// State-history websocket endpoint.
    pub ship: String,
    /// Chain API dialect: `antelope` (nodeos `/v1/chain/*` REST, default)
    /// or `pulsevm` (JSON-RPC 2.0, `pulsevm.*` methods).
    #[serde(default)]
    pub api: crate::chain_client::ChainApiKind,
    /// Privileged system account (`eosio` on Antelope, `pulse` on PulseVM);
    /// its `setabi` action updates the ABI cache inline.
    #[serde(default = "default_system_account")]
    pub system_account: String,
}

fn default_system_account() -> String {
    "eosio".to_string()
}

impl ChainConfig {
    pub fn client(&self) -> crate::chain_client::ChainClient {
        crate::chain_client::ChainClient::new(&self.http, self.api)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IndexerConfig {
    /// First block to index; 0 = resume from last indexed (or genesis).
    pub start_block: u32,
    /// Last block to index; 0 = follow the chain head indefinitely.
    pub stop_block: u32,
    pub fetch_block: bool,
    pub fetch_traces: bool,
    pub fetch_deltas: bool,
    pub max_messages_in_flight: u32,
    /// Concurrent raw block decoders; 0 decodes inline in the processor.
    pub decode_workers: usize,
    /// Documents per bulk request.
    pub batch_size: usize,
    /// Target maximum serialized bulk size, checked after each complete block.
    pub batch_max_bytes: usize,
    /// Max time a partial batch may wait before being flushed.
    pub flush_interval_ms: u64,
    /// Actions to skip, as `contract::action` (e.g. `eosio::onblock`).
    pub skip_actions: Vec<String>,
    /// Bulk requests allowed in flight at once. Each document carries an
    /// external version derived from its block position, so Elasticsearch
    /// itself rejects a stale write that lands out of order; completions are
    /// still confirmed in submission order so the resume checkpoint never
    /// advances past a batch that hasn't landed yet.
    pub writer_concurrency: usize,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        IndexerConfig {
            start_block: 0,
            stop_block: 0,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
            max_messages_in_flight: 128,
            decode_workers: std::thread::available_parallelism()
                .map(|cpus| cpus.get().saturating_sub(1).min(2))
                .unwrap_or(0),
            batch_size: 2000,
            batch_max_bytes: 5 * 1024 * 1024,
            flush_interval_ms: 500,
            skip_actions: Vec::new(),
            writer_concurrency: 4,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ElasticConfig {
    pub url: String,
    pub user: String,
    pub pass: String,
    /// Number of shards for new indices.
    pub shards: u32,
    pub replicas: u32,
}

impl Default for ElasticConfig {
    fn default() -> Self {
        ElasticConfig {
            url: "http://127.0.0.1:9200".to_string(),
            user: String::new(),
            pass: String::new(),
            shards: 1,
            replicas: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    pub listen: String,
    /// Hard cap on `limit` request parameters.
    pub max_limit: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        ApiConfig {
            listen: "127.0.0.1:7000".to_string(),
            max_limit: 1000,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read config {}: {e}", path.display()))?;
        let config: Config = toml::from_str(&raw)?;
        Ok(config)
    }

    /// Index name for a document type, e.g. `wax-action`.
    pub fn index(&self, kind: &str) -> String {
        format!("{}-{kind}", self.chain.name)
    }
}
