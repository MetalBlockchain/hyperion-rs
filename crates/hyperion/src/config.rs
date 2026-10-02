use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub chain: ChainConfig,
    #[serde(default)]
    pub indexer: IndexerConfig,
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
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
    /// Rows per insert batch.
    pub batch_size: usize,
    /// Max time a partial batch may wait before being flushed.
    pub flush_interval_ms: u64,
    /// Actions to skip, as `contract::action` (e.g. `eosio::onblock`).
    pub skip_actions: Vec<String>,
    /// Insert requests allowed in flight at once. Completions are confirmed
    /// in submission order so the resume checkpoint never advances past a
    /// batch that hasn't landed yet, even though ClickHouse may finish a
    /// later batch's insert first.
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
            flush_interval_ms: 500,
            skip_actions: Vec::new(),
            writer_concurrency: 4,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClickHouseConfig {
    pub url: String,
    pub user: Option<String>,
    pub pass: Option<String>,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        ClickHouseConfig {
            url: "http://127.0.0.1:8123".to_string(),
            user: None,
            pass: None,
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
}
