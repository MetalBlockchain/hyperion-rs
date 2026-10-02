//! ClickHouse table schemas for Hyperion.
//!
//! All tables use ReplacingMergeTree with a version column for replay
//! idempotence. For the two append-only history tables (`action`, `delta`,
//! `block`, `abi`) version is `(block_num << 32) | position_in_block` and the
//! sort key includes `block_num`, so every block's rows are kept. For the two
//! current-state snapshot tables (`perm`, `token`) the sort key deliberately
//! excludes `block_num` - see the comment on the `perm` table below.
//!
//! `writer::doc_to_row` builds rows in exactly the column order declared
//! here; there is no explicit column list on the `INSERT ... FORMAT
//! TabSeparated` calls, so the two must stay in lockstep.
//!
//! `timestamp`/`last_updated` are `DateTime64(3)`, not `DateTime`: the
//! processor's own timestamp strings are millisecond-precision ISO-8601
//! (`2000-01-01T00:08:20.000`, no trailing `Z`), and `DateTime` - confirmed
//! against a real server - fails to parse the fractional part at all
//! ("garbage after DateTime"), not just silently truncating it.
//!
//! None of the `ORDER BY` clauses below use `DESC`: a MergeTree-family
//! table's `ORDER BY` is a physical sort/sparse-index key, not a query
//! result order, and this ClickHouse version rejects a direction modifier
//! there outright (confirmed against a real server: `DESC` in a table's
//! `ORDER BY` is a `SYNTAX_ERROR`, not just a style choice). Query-time
//! ordering is independent and already handled by `clickhouse::queries`'
//! own `ORDER BY ... DESC` in each `SELECT`.

/// SQL DDL for creating all ClickHouse tables.
pub fn create_tables_sql() -> &'static str {
    r#"
-- Actions table
CREATE TABLE IF NOT EXISTS action (
    block_num UInt32,
    global_sequence UInt64,
    timestamp DateTime64(3),
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

    newaccount_creator Nullable(String),
    newaccount_newact Nullable(String),

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
ORDER BY (notified, block_num, global_sequence)
SETTINGS index_granularity = 8192;

-- Blocks table
CREATE TABLE IF NOT EXISTS block (
    block_num UInt32,
    timestamp DateTime64(3),
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
ORDER BY (block_num)
SETTINGS index_granularity = 8192;

-- Deltas table
CREATE TABLE IF NOT EXISTS delta (
    block_num UInt32,
    timestamp DateTime64(3),
    block_id String,
    code String,
    scope String,
    table String,
    primary_key String,
    payer String,
    present Bool,
    data String,
    value_hex Nullable(String),
    version UInt64,

    INDEX idx_code_scope (code, scope) TYPE set(10000),
    INDEX idx_table table TYPE set(1000)
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 50000)
ORDER BY (code, scope, table, block_num, primary_key)
SETTINGS index_granularity = 8192;

-- ABI table
CREATE TABLE IF NOT EXISTS abi (
    block_num UInt32,
    timestamp DateTime64(3),
    account String,
    abi String,
    actions Array(String),
    tables Array(String),
    version UInt64,

    INDEX idx_account account TYPE set(1000)
) ENGINE = ReplacingMergeTree(version)
PARTITION BY (block_num DIV 100000)
ORDER BY (account, block_num)
SETTINGS index_granularity = 4096;

-- Permission table (current-state snapshot, not a history log). ORDER BY is
-- deliberately just (owner, name) with no block_num: ReplacingMergeTree only
-- collapses rows that share the exact sort-key tuple, so including block_num
-- (as the other, log-style tables do) would keep every historical update as
-- a permanently-separate row instead of replacing it, defeating "FINAL gives
-- you current state". A delete (permission removed) is written as a
-- tombstone row with the same (owner, name) key and a higher version, with
-- is_deleted = 1 (see `writer::doc_to_row`). Not partitioned: write volume is
-- low enough that a single partition is simpler and keeps FINAL's dedup
-- scope to one partition's merges.
CREATE TABLE IF NOT EXISTS perm (
    block_num UInt32,
    owner String,
    name String,
    parent String,
    last_updated DateTime64(3),
    keys Array(String),
    accounts Array(String),
    threshold UInt32,
    version UInt64,
    is_deleted UInt8,

    INDEX idx_owner owner TYPE set(10000),
    -- `bloom_filter` applies directly to Array(String) columns - there is no
    -- `arrayAll(...)` index type (confirmed against a real server:
    -- "Only literals can be skip index arguments").
    INDEX idx_keys keys TYPE bloom_filter()
) ENGINE = ReplacingMergeTree(version)
ORDER BY (owner, name)
SETTINGS index_granularity = 8192;

-- Token table (current-state snapshot) - see the `perm` table comment above -
-- the same reasoning applies to its ORDER BY, is_deleted column, and lack of
-- partitioning.
CREATE TABLE IF NOT EXISTS token (
    block_num UInt32,
    code String,
    scope String,
    symbol String,
    precision UInt8,
    amount Float64,
    version UInt64,
    is_deleted UInt8,

    INDEX idx_scope scope TYPE set(10000),
    INDEX idx_code_symbol (code, symbol) TYPE set(1000)
) ENGINE = ReplacingMergeTree(version)
ORDER BY (scope, code, symbol)
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
