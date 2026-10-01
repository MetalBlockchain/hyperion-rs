//! ClickHouse table schemas and serialization types for Hyperion.
//!
//! All tables use ReplacingMergeTree with version column for replay idempotence.
//! Version is computed as: (block_num << 32) | position_in_block

use serde::{Serialize, Deserialize};


/// Action document: one per action, inline notifications collapsed to notified array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    #[serde(rename = "block_num")]
    pub block_num: u32,
    #[serde(rename = "global_sequence")]
    pub global_sequence: u64,
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    pub block_id: String,
    pub trx_id: String,
    pub producer: String,
    #[serde(rename = "act.account")]
    pub act_account: String,
    #[serde(rename = "act.name")]
    pub act_name: String,
    #[serde(rename = "act.authorization")]
    pub act_authorization: Vec<Authorization>,
    #[serde(rename = "act.data")]
    pub act_data: serde_json::Value,
    #[serde(rename = "act.hex_data")]
    pub act_hex_data: Option<String>,

    // Extracted transfer fields
    #[serde(rename = "@transfer.from", skip_serializing_if = "Option::is_none")]
    pub transfer_from: Option<String>,
    #[serde(rename = "@transfer.to", skip_serializing_if = "Option::is_none")]
    pub transfer_to: Option<String>,
    #[serde(rename = "@transfer.amount", skip_serializing_if = "Option::is_none")]
    pub transfer_amount: Option<f64>,
    #[serde(rename = "@transfer.symbol", skip_serializing_if = "Option::is_none")]
    pub transfer_symbol: Option<String>,
    #[serde(rename = "@transfer.memo", skip_serializing_if = "Option::is_none")]
    pub transfer_memo: Option<String>,

    pub notified: Vec<String>,
    pub receipts: Vec<Receipt>,
    pub cpu_usage_us: u32,
    pub net_usage_words: u32,
    pub action_ordinal: u32,
    pub creator_action_ordinal: u32,
    pub signatures: Vec<String>,

    #[serde(skip)]
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Authorization {
    pub actor: String,
    pub permission: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub receiver: String,
    pub global_sequence: u64,
}

/// Block document: one per block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub block_num: u32,
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    pub block_id: String,
    pub prev_id: String,
    pub producer: String,
    pub schedule_version: u32,
    pub trx_count: u32,
    pub cpu_usage_us: u64,
    pub net_usage_words: u64,

    #[serde(skip)]
    pub version: u64,
}

/// Delta document: one per table delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delta {
    pub block_num: u32,
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    pub block_id: String,
    pub code: String,
    pub scope: String,
    pub table: String,
    pub primary_key: String,
    pub payer: String,
    pub present: bool,
    pub data: serde_json::Value,
    pub value_hex: Option<String>,

    #[serde(skip)]
    pub version: u64,
}

/// ABI document: one per contract ABI change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Abi {
    pub block_num: u32,
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    pub account: String,
    pub abi: String,
    pub actions: Vec<String>,
    pub tables: Vec<String>,

    #[serde(skip)]
    pub version: u64,
}

/// Permission document: snapshot table, one per unique (owner, name).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    pub block_num: u32,
    pub owner: String,
    pub name: String,
    pub parent: String,
    pub last_updated: String,
    pub keys: Vec<String>,
    pub accounts: Vec<String>,
    pub threshold: u32,

    #[serde(skip)]
    pub version: u64,
}

/// Token document: snapshot table, one per unique (code, scope, symbol).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub block_num: u32,
    pub code: String,
    pub scope: String,
    pub symbol: String,
    pub precision: u8,
    pub amount: f64,

    #[serde(skip)]
    pub version: u64,
}

/// Progress checkpoint document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Progress {
    pub block_num: u32,

    #[serde(skip)]
    pub version: u64,
}

/// SQL DDL for creating all ClickHouse tables.
pub fn create_tables_sql() -> &'static str {
    r#"
-- Actions table
CREATE TABLE IF NOT EXISTS action (
    block_num UInt32,
    global_sequence UInt64,
    timestamp DateTime,
    block_id String,
    trx_id String,
    producer String,
    act_account String,
    act_name String,
    act_authorization Array(Tuple(actor String, permission String)),
    act_data String,
    act_hex_data Nullable(String),

    transfer_from Nullable(String),
    transfer_to Nullable(String),
    transfer_amount Nullable(Float64),
    transfer_symbol Nullable(String),
    transfer_memo Nullable(String),

    notified Array(String),
    receipts Array(Tuple(receiver String, global_sequence UInt64)),
    cpu_usage_us UInt32,
    net_usage_words UInt32,
    action_ordinal UInt32,
    creator_action_ordinal UInt32,
    signatures Array(String),

    version UInt64,

    INDEX idx_notified notified TYPE bloom_filter(),
    INDEX idx_act_account act_account TYPE set(10000),
    INDEX idx_trx_id trx_id TYPE bloom_filter()
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 10000)
ORDER BY (notified, block_num DESC, global_sequence DESC)
SETTINGS index_granularity = 8192;

-- Blocks table
CREATE TABLE IF NOT EXISTS block (
    block_num UInt32,
    timestamp DateTime,
    block_id String,
    prev_id String,
    producer String,
    schedule_version UInt32,
    trx_count UInt32,
    cpu_usage_us UInt64,
    net_usage_words UInt64,
    version UInt64,

    INDEX idx_producer producer TYPE set(1000)
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 100000)
ORDER BY (block_num DESC)
SETTINGS index_granularity = 8192;

-- Deltas table
CREATE TABLE IF NOT EXISTS delta (
    block_num UInt32,
    timestamp DateTime,
    block_id String,
    code String,
    scope String,
    table String,
    primary_key String,
    payer String,
    present Boolean,
    data String,
    value_hex Nullable(String),
    version UInt64,

    INDEX idx_code_scope (code, scope) TYPE set(10000),
    INDEX idx_table table TYPE set(1000)
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 50000)
ORDER BY (code, scope, table, block_num DESC, primary_key)
SETTINGS index_granularity = 8192;

-- ABI table
CREATE TABLE IF NOT EXISTS abi (
    block_num UInt32,
    timestamp DateTime,
    account String,
    abi String,
    actions Array(String),
    tables Array(String),
    version UInt64,

    INDEX idx_account account TYPE set(1000)
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 100000)
ORDER BY (account, block_num DESC)
SETTINGS index_granularity = 4096;

-- Permission table (snapshot)
CREATE TABLE IF NOT EXISTS perm (
    block_num UInt32,
    owner String,
    name String,
    parent String,
    last_updated DateTime,
    keys Array(String),
    accounts Array(String),
    threshold UInt32,
    version UInt64,

    INDEX idx_owner owner TYPE set(10000),
    INDEX idx_keys keys TYPE arrayAll(bloom_filter())
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 100000)
ORDER BY (owner, name, block_num DESC)
SETTINGS index_granularity = 8192;

-- Token table (snapshot)
CREATE TABLE IF NOT EXISTS token (
    block_num UInt32,
    code String,
    scope String,
    symbol String,
    precision UInt8,
    amount Float64,
    version UInt64,

    INDEX idx_scope scope TYPE set(10000),
    INDEX idx_code_symbol (code, symbol) TYPE set(1000)
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 100000)
ORDER BY (scope, code, symbol, block_num DESC)
SETTINGS index_granularity = 8192;

-- Progress checkpoint table
CREATE TABLE IF NOT EXISTS progress (
    id String,
    block_num UInt32,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (id)
SETTINGS index_granularity = 1;
"#
}
