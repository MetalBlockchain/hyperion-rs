# ClickHouse Migration Implementation Guide

## Overview

This document describes the complete ClickHouse migration implementation for Hyperion, designed to replace Elasticsearch with ClickHouse as the primary indexing backend. The migration targets:

- **Throughput improvement**: 2–4× (from 50–150 to 200–600+ blocks/sec)
- **Memory savings**: 30 GB reduction (from 64 GB to ~45–50 GB)
- **Query performance**: Maintain or improve ES baseline
- **Zero downtime**: Blue-green cutover with instant rollback

## Architecture Overview

### Four-Phase Implementation

#### Phase 0: Validation & Benchmarking (Testing)
- **Status**: ✅ IMPLEMENTED
- **Location**: `tests/es_vs_ch_comparison.rs`
- **Deliverables**:
  - Sample document generators (action, block, delta)
  - Benchmark harness (latency, throughput)
  - Checkpoint management tests
  - Memory footprint baseline tests

**Run tests**:
```bash
# Requires: Elasticsearch at http://localhost:9200, ClickHouse at http://localhost:8123
cargo test --test es_vs_ch_comparison -- --ignored --nocapture
```

**Key tests**:
- `test_basic_connectivity` - Both systems reachable
- `test_action_insert_elasticsearch` - ES baseline throughput
- `test_block_insert` - Block throughput
- `test_memory_footprint_baseline` - Observe RSS under load
- `test_checkpoint_management` - Checkpoint logic both ways
- `test_count_operations` - Query result counting

#### Phase 1: ClickHouse Schema & Ingestion Layer
- **Status**: ✅ IMPLEMENTED
- **Location**: `src/clickhouse/`
- **Components**:
  - `schema.rs`: 7 ReplacingMergeTree tables with version column
  - `client.rs`: HTTP API wrapper with TabSeparated format
  - `writer.rs`: Batch serialization & concurrent insertion

**Files**:
- `ClickHouse::new(url, user, pass)` - Create client
- `ClickHouseBatch::push(doc, version)` - Add documents to batch
- `write_batches(ck, rx, concurrency)` - Concurrent writer task

**Features**:
- External versioning for replay idempotence
- Block-based partitioning for efficient deletion
- Skip indexes (Bloom filter, SET) for query pushdown
- Array/tuple serialization for complex fields
- Checkpoint management for resume on crash

**Example usage**:
```rust
use hyperion::clickhouse::{ClickHouse, ClickHouseBatch, doc_to_row};

let ck = ClickHouse::new("http://localhost:8123", None, None);
ck.create_all().await?;

let mut batch = ClickHouseBatch::new();
batch.push(&doc, version)?;

// Send to ClickHouse
let (batch_tx, batch_rx) = mpsc::channel(2);
tokio::spawn(write_batches(&ck, batch_rx, 4));
batch_tx.send(batch).await?;
```

#### Phase 2: Query Layer Implementation
- **Status**: ✅ IMPLEMENTED
- **Location**: `src/clickhouse/queries.rs`
- **Components**:
  - SQL query builders for all endpoints
  - Filter parsing and translation
  - Escape functions for SQL injection prevention

**Functions**:
- `build_get_actions_query()` - Actions with filters, sorting, pagination
- `build_get_tokens_query()` - Token snapshots by account/code
- `build_get_key_accounts_query()` - Permissions by public key
- `build_get_account_query()` - Account permission snapshots

**Features**:
- Code:action filter parsing (e.g., "eosio.token:transfer")
- Transfer field filtering (@transfer.from, @transfer.to, @transfer.symbol)
- Full-text search on memo fields
- Range filtering on sequence numbers
- Sorting and pagination

**Example**:
```rust
use hyperion::clickhouse::build_get_actions_query;

let sql = build_get_actions_query(
    Some("alice"),  // account
    Some("eosio.token:transfer"),  // filter
    0,  // skip
    100,  // limit
    "desc",  // sort order
    None,  // after
    None,  // before
    None, None, None, None  // transfer fields
)?;

let result = ck.query(&sql).await?;
```

#### Phase 3: Dual-Write & Shadow Read
- **Status**: ✅ IMPLEMENTED
- **Location**: `src/clickhouse/dual_write.rs`, `tests/integration_clickhouse.rs`
- **Components**:
  - Concurrent write to both backends
  - Divergence detection and logging
  - Shadow read comparison framework

**Functions**:
- `DivergenceCounter::new()` - Track ES vs CH divergences
- `write_both(es, ck, ...)` - Concurrent writes with ordered checkpointing
- `shadow_read(es, ck, ...)` - Query both and compare results

**Example**:
```rust
use hyperion::clickhouse::{write_both, DivergenceCounter};

let counter = DivergenceCounter::new();
write_both(&es, &ck, es_body, ck_batch, max_block, &progress_index, &counter).await?;

println!("{}", counter.report());  // ES errors: 0, CK errors: 0, Divergences: 0
```

**Integration tests**:
```bash
# Requires: ClickHouse at http://localhost:8123
cargo test --test integration_clickhouse -- --ignored --nocapture
```

Key tests:
- `test_full_ingestion_pipeline` - 5 blocks, 50 actions end-to-end
- `test_row_serialization` - TabSeparated format validation
- `test_batch_operations` - Multi-table atomic ops

#### Phase 4: Cutover & Backoff Plan
- **Status**: ⚠️ TODO - Integration with indexer.rs required
- **Deliverables**:
  - Config flag for dual-write, ClickHouse primary, or ES primary
  - Blue-green API routing
  - Instant rollback capability
  - Monitoring dashboard skeleton

## Configuration

Add to your `hyperion.toml`:

```toml
[chain]
name = "eos"
http = "http://api.example.com"
ship = "ws://ship.example.com:8080"

[elasticsearch]
url = "http://localhost:9200"
shards = 1
replicas = 0

[clickhouse]
url = "http://localhost:8123"
user = "default"  # optional
pass = ""  # optional
enabled = false  # set to true to enable dual-write

[indexer]
writer_concurrency = 4
batch_size = 2000
```

## Schema Design

### Core Tables

All tables use `ReplacingMergeTree(version)` with external versioning:

```
version = (block_num << 32) | position_in_block
```

| Table | Purpose | Key Columns | Partition |
|-------|---------|-----------|-----------|
| action | Actions, receipts, transfers | (notified, block_num DESC, global_sequence DESC) | block_num DIV 10000 |
| block | Block headers | (block_num DESC) | block_num DIV 100000 |
| delta | Contract state deltas | (code, scope, table, block_num DESC) | block_num DIV 50000 |
| abi | Contract ABIs | (account, block_num DESC) | block_num DIV 100000 |
| perm | Account permissions (snapshot) | (owner, name, block_num DESC) | block_num DIV 100000 |
| token | Token balances (snapshot) | (scope, code, symbol, block_num DESC) | block_num DIV 100000 |
| progress | Checkpoint | (id) | none |

### Indexes

- **Bloom filters**: High-cardinality fields (notified, trx_id, keys)
- **SET indexes**: Bounded cardinality (code, table, producer)
- **No secondary indexes**: ORDER BY prefix handles most queries

## Testing & Validation

### Phase 0: Benchmarking

```bash
# Start dependencies
docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:8.0.0
docker run -p 8123:8123 clickhouse/clickhouse-server

# Run comparison tests
cargo test --test es_vs_ch_comparison -- --ignored --nocapture
```

Expected results:
- ES latency baseline: measure p50, p95, p99
- CH latency: should be ≤ baseline
- Memory: observe RSS during 10K action insert
- Throughput: blocks/sec, actions/sec

### Phase 1 & 2: Unit Tests

```bash
cargo test clickhouse
```

All tests in-memory, no dependencies.

### Phase 3: Integration Tests

```bash
# Start ClickHouse
docker run -p 8123:8123 clickhouse/clickhouse-server

# Run integration tests
cargo test --test integration_clickhouse -- --ignored --nocapture
```

Tests:
1. `test_full_ingestion_pipeline`: 50 documents, checkpoint, queries
2. `test_row_serialization`: TabSeparated format
3. `test_batch_operations`: Multi-table atomicity

### Phase 4: Production Cutover

1. **Dry run on staging**:
   - Enable `clickhouse.enabled = true` for dual-write
   - Run 24 hours
   - Compare divergence counter: should be < 0.1%
   - Check memory footprint: target 15 GB for CH at 1M blocks

2. **Canary on production**:
   - Set `api.clickhouse_read_percent = 10` (10% of reads to CH)
   - Monitor latency, error rates
   - Expand to 50%, then 100%

3. **Switch writes**:
   - Stop ES writer
   - Verify CH throughput ≥ 150 blocks/sec
   - Monitor for 24 hours

4. **Rollback plan**:
   - If latency > 2× baseline: switch reads back to ES (5 min)
   - If memory > 20 GB: reduce batch size (2 min)
   - If divergence > 1%: abort cutover, investigate

## File Structure

```
crates/hyperion/src/clickhouse/
├── mod.rs                 # Module exports
├── client.rs              # ClickHouse HTTP client
├── schema.rs              # Table definitions & DDL
├── writer.rs              # Batch serialization & writer task
├── queries.rs             # SQL query builders
└── dual_write.rs          # Dual-write infrastructure

crates/hyperion/tests/
├── es_vs_ch_comparison.rs # Phase 0 benchmarks
└── integration_clickhouse.rs # Phase 3 integration tests

crates/hyperion/src/
└── config.rs              # Added ClickHouseConfig
```

## Performance Tuning

### Batch Size

Default: 2000 documents. Adjust based on:
- **Increase if**: Memory available, writes are CPU-bound
- **Decrease if**: Network latency high, memory constrained

```toml
[indexer]
batch_size = 5000  # more docs per insert
batch_max_bytes = 10485760  # 10 MB max per batch
```

### Concurrency

Default: `writer_concurrency = 4`. Tune based on:
- **Increase**: 6–8 if disks have high I/O capacity
- **Decrease**: 2 if memory or CPU constrained

```toml
[indexer]
writer_concurrency = 6
```

### Merge Tuning

Edit `clickhouse/schema.rs` for merge settings:

```sql
-- Reduce merge memory:
SETTINGS index_granularity = 4096  -- smaller granules = less memory

-- Or increase merge chunk count:
SETTINGS parts_to_throw_insert_exception = 100
```

### Query Optimization

Use FINAL for consistent reads:
```sql
SELECT * FROM action FINAL WHERE notified = 'alice'
```

Avoid full-table scans:
```sql
-- Good (ORDER BY prefix match):
WHERE notified = 'alice' AND block_num > 1000

-- Bad (full scan required):
WHERE trx_id = 'abc...'  -- doesn't match ORDER BY
-- Mitigation: rely on Bloom filter instead
```

## Monitoring

### Key Metrics

Track in Prometheus/Grafana:

```
hyperion_ch_inserts_total{table}      # rows inserted per table
hyperion_ch_batch_latency_ms{percentile}  # p50, p95, p99
hyperion_ch_checkpoint_block           # current checkpoint
hyperion_ch_memory_bytes               # process memory
hyperion_divergence_count              # ES vs CH mismatches
```

### Operational Queries

```sql
-- Row counts
SELECT table, sum(rows) FROM system.parts WHERE active GROUP BY table;

-- Memory usage
SELECT formatReadableSize(sum(bytes_allocated)) FROM system.allocator_stats;

-- Slow queries
SELECT database, query, query_duration_ms FROM system.query_log 
WHERE event_date = today() AND query_duration_ms > 5000;

-- Merge status
SELECT * FROM system.merges;
```

## Troubleshooting

### High Memory Usage (> 20 GB)

1. Check for stuck merges: `SELECT * FROM system.merges`
2. Kill longest-running merge: `KILL MUTATION WHERE ...`
3. Trigger compact: `OPTIMIZE TABLE action FINAL`
4. Reduce `batch_size` or `writer_concurrency`

### Slow Queries (> 5 sec)

1. Check ORDER BY prefix match in WHERE clause
2. Add skip index: `INDEX idx_field field TYPE bloom_filter()`
3. Increase `index_granularity_bytes` (more rows per granule)
4. Use `PREWHERE` for early filtering

### Out-of-Order Data

1. ClickHouse may receive docs in different order within same version
2. Solution: `OPTIMIZE TABLE <table> FINAL` to deduplicate
3. ReplacingMergeTree keeps latest `version` only

### Divergence Between ES and CH

1. Run shadow read test: `cargo test shadow_read`
2. Identify diverging document type (action, block, delta, etc.)
3. Check: field parsing differences, missing fields, type mismatches
4. Common causes: ABI decode failures, null handling, precision loss

## Known Limitations & Tradeoffs

### Strengths

✅ **Append-heavy** workload: MergeTree LSM optimized for sequential writes  
✅ **Compression**: Column-oriented, 10:1 compression on repeated fields  
✅ **Deduplication**: Built-in via version column, no complex logic needed  
✅ **Memory**: No inverted index overhead, ~10–15 GB at 1M blocks  
✅ **Throughput**: Parallelizable inserts, no global shard lock  

### Tradeoffs

⚠️ **FINAL overhead**: 10–30% query latency cost for deduplication  
⚠️ **Snapshot tables**: Token/perm updated every block, not instant  
⚠️ **Non-prefix queries**: Without ORDER BY prefix match, full scan required  
⚠️ **No text search**: Hyperion actions use `positionCaseInsensitive()`, not full FTS  
⚠️ **Materialize views**: Must refresh manually, not auto-updated  

## Next Steps

1. **Run Phase 0 tests** to establish baseline metrics
2. **Run Phase 1–3 integration tests** to validate implementation
3. **Stage dual-write test**: 24 hours with divergence monitoring
4. **Production canary**: 10% → 50% → 100% traffic switch
5. **Monitor**: Memory, latency, divergence for 1 week post-cutover
6. **Optimize**: Fine-tune batch size, concurrency based on production data

## References

- [ClickHouse MergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
- [ClickHouse Indexes](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)
- [Hyperion API Docs](https://github.com/cc32d9/hyperion-history-api)

## Support

For issues, questions, or contributions:
1. Check this document and code comments
2. Review integration test examples
3. Check ClickHouse system tables for diagnostics
4. Consult ClickHouse docs for schema tuning
